package statepush_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/turborg/turborg/internal/statepush"
)

// recorder collects every PUT body the test server saw, with mu
// guarding both the slice and the optional callback that fires after
// each request.
type recorder struct {
	mu      sync.Mutex
	bodies  [][]byte
	onWrite func()
}

func (r *recorder) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, _ := io.ReadAll(req.Body)
		r.mu.Lock()
		r.bodies = append(r.bodies, body)
		cb := r.onWrite
		r.mu.Unlock()
		if cb != nil {
			cb()
		}
		w.WriteHeader(http.StatusNoContent)
	})
}

func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.bodies)
}

func (r *recorder) bodyAt(i int) []byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	if i >= len(r.bodies) {
		return nil
	}
	return r.bodies[i]
}

func TestNewChannelSnapshot_KeyEncodedAsNullWhenEmpty(t *testing.T) {
	withKey := statepush.NewChannelSnapshot("#a", "hunter2")
	noKey := statepush.NewChannelSnapshot("#b", "")

	got, err := json.Marshal(withKey)
	require.NoError(t, err)
	assert.JSONEq(t, `{"name":"#a","key":"hunter2"}`, string(got))

	got, err = json.Marshal(noKey)
	require.NoError(t, err)
	assert.JSONEq(t, `{"name":"#b","key":null}`, string(got))
}

func TestStateEmitter_DisabledWhenClientNil(t *testing.T) {
	// Disabled emitters must not spawn a goroutine (goleak in TestMain
	// would catch a leak even between sub-tests).
	e := statepush.NewEmitter(nil, nil, 0, nil)
	e.NotifyChange()
	e.Stop()
}

func TestStateEmitter_DisabledWhenURLEmpty(t *testing.T) {
	c := statepush.NewClient("", "", nil)
	called := atomic.Int32{}
	e := statepush.NewEmitter(c, func() statepush.Snapshot {
		called.Add(1)
		return statepush.Snapshot{}
	}, 0, nil)
	e.NotifyChange()
	e.NotifyChange()
	e.Stop()
	assert.Equal(t, int32(0), called.Load(), "snapshot builder must not run when URL empty")
}

func TestStateEmitter_DisabledWhenBuilderNil(t *testing.T) {
	c := statepush.NewClient("http://obs/state", "", nil)
	e := statepush.NewEmitter(c, nil, 0, nil)
	// Without a builder the emitter is inert.
	e.NotifyChange()
	e.Stop()
}

func TestStateEmitter_NotifyChangeFiresPutWithZeroDebounce(t *testing.T) {
	rec := &recorder{}
	done := make(chan struct{}, 1)
	rec.onWrite = func() {
		select {
		case done <- struct{}{}:
		default:
		}
	}
	server := httptest.NewServer(rec.handler())
	t.Cleanup(server.Close)

	c := statepush.NewClient(server.URL+"/state", "", nil)
	e := statepush.NewEmitter(c, func() statepush.Snapshot {
		return statepush.Snapshot{
			Connectors: map[string]statepush.ConnectorSnapshot{
				"irc": {
					State:    "registered",
					Since:    time.Unix(0, 0).UTC(),
					Channels: []statepush.ChannelSnapshot{statepush.NewChannelSnapshot("#a", "")},
					Nick:     "stefan",
				},
			},
		}
	}, 0, nil)
	t.Cleanup(e.Stop)

	e.NotifyChange()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("expected a PUT within 2s")
	}
	require.Equal(t, 1, rec.count())

	var got statepush.Snapshot
	require.NoError(t, json.Unmarshal(rec.bodyAt(0), &got))
	require.Contains(t, got.Connectors, "irc")
	assert.Equal(t, "registered", got.Connectors["irc"].State)
	assert.Equal(t, "stefan", got.Connectors["irc"].Nick)
	require.Len(t, got.Connectors["irc"].Channels, 1)
	assert.Equal(t, "#a", got.Connectors["irc"].Channels[0].Name)
}

func TestStateEmitter_BurstCoalescesIntoOnePut(t *testing.T) {
	rec := &recorder{}
	done := make(chan struct{}, 1)
	rec.onWrite = func() {
		select {
		case done <- struct{}{}:
		default:
		}
	}
	server := httptest.NewServer(rec.handler())
	t.Cleanup(server.Close)

	c := statepush.NewClient(server.URL+"/state", "", nil)
	debounce := 50 * time.Millisecond
	e := statepush.NewEmitter(c, func() statepush.Snapshot {
		return statepush.Snapshot{}
	}, debounce, nil)
	t.Cleanup(e.Stop)

	// Fire five notifies in rapid succession — they should collapse
	// into a single PUT after the debounce window elapses.
	for i := 0; i < 5; i++ {
		e.NotifyChange()
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("expected at least one PUT")
	}

	// Wait a bit more than the debounce to make sure no extra PUT
	// trickles in.
	time.Sleep(debounce * 3)
	assert.Equal(t, 1, rec.count(), "burst must coalesce to one PUT")
}

func TestStateEmitter_NewNotifyAfterFireRearmsTimer(t *testing.T) {
	rec := &recorder{}
	server := httptest.NewServer(rec.handler())
	t.Cleanup(server.Close)

	c := statepush.NewClient(server.URL+"/state", "", nil)
	debounce := 20 * time.Millisecond
	e := statepush.NewEmitter(c, func() statepush.Snapshot {
		return statepush.Snapshot{}
	}, debounce, nil)
	t.Cleanup(e.Stop)

	// First burst → one PUT.
	e.NotifyChange()
	require.Eventually(t, func() bool { return rec.count() == 1 }, time.Second, 5*time.Millisecond)

	// Second notify after the first fire should arm a fresh timer
	// and produce a second PUT.
	e.NotifyChange()
	require.Eventually(t, func() bool { return rec.count() == 2 }, time.Second, 5*time.Millisecond)
}

func TestStateEmitter_StopIsIdempotent(t *testing.T) {
	rec := &recorder{}
	server := httptest.NewServer(rec.handler())
	t.Cleanup(server.Close)

	c := statepush.NewClient(server.URL+"/state", "", nil)
	e := statepush.NewEmitter(c, func() statepush.Snapshot { return statepush.Snapshot{} }, 5*time.Millisecond, nil)

	e.NotifyChange()
	e.Stop()
	// Second Stop on an already-stopped emitter must not panic or
	// block — the agent shutdown path may call Stop more than once
	// via overlapping cleanup paths.
	e.Stop()
}

func TestEmitter_NegativeDebounceResetsToDefault(t *testing.T) {
	c := statepush.NewClient("http://obs/state", "", nil)
	// Negative debounce gets clamped to the package default. We can't
	// observe the field directly; the behavior under test is "doesn't
	// panic and starts the goroutine clean", which Stop verifies.
	e := statepush.NewEmitter(c, func() statepush.Snapshot { return statepush.Snapshot{} }, -1, nil)
	e.Stop()
}

func TestEmitter_SendLogsPutFailure(t *testing.T) {
	// 5xx from the receiver → retry exhaustion → debug-level failure
	// log inside send(). Exercises the err != nil branch.
	//
	// Stop is called explicitly before the deferred restore runs, so
	// the goroutine has stopped reading retryBackoffs by the time the
	// override is rolled back.
	restoreBackoffs := statepush.OverrideBackoffsForTesting(t, []time.Duration{0, 0})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	c := statepush.NewClient(server.URL, "", nil)
	e := statepush.NewEmitter(c, func() statepush.Snapshot { return statepush.Snapshot{} }, 0, nil)

	e.NotifyChange()
	// Give the emitter goroutine time to attempt all three tries +
	// log the debug-level failure.
	time.Sleep(100 * time.Millisecond)
	e.Stop()
	restoreBackoffs()
}

func TestStateEmitter_StopDrainsPendingNotify(t *testing.T) {
	rec := &recorder{}
	server := httptest.NewServer(rec.handler())
	t.Cleanup(server.Close)

	c := statepush.NewClient(server.URL+"/state", "", nil)
	// Long debounce → notify is pending in the collect() loop when
	// Stop fires. Goroutine must exit cleanly without leaking.
	e := statepush.NewEmitter(c, func() statepush.Snapshot { return statepush.Snapshot{} }, 5*time.Second, nil)
	e.NotifyChange()
	// Small delay to let the goroutine enter collect().
	time.Sleep(10 * time.Millisecond)
	e.Stop()
}
