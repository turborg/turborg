package commandrefresh

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"

	"github.com/turborg/turborg/internal/commands"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

type spyApply struct {
	mu    sync.Mutex
	calls int
	last  []commands.Definition
}

func (s *spyApply) apply(defs []commands.Definition) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.calls++
	s.last = defs
}

func (s *spyApply) snapshot() (int, []commands.Definition) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls, s.last
}

func TestNewReturnsNilWhenUnconfigured(t *testing.T) {
	s := &spyApply{}
	assert.Nil(t, New("", "tok", 0, s.apply, nil), "no endpoint → nil")
	assert.Nil(t, New("http://x", "", 0, s.apply, nil), "no token → nil")
	assert.Nil(t, New("http://x", "tok", 0, nil, nil), "no apply → nil")
	assert.NotNil(t, New("http://x", "tok", 0, s.apply, nil))
}

func TestRefreshAppliesCommandsAndSendsAuth(t *testing.T) {
	var gotAuth atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth.Store(r.Header.Get("Authorization"))
		_, _ = w.Write([]byte(`[{"name":"weather","type":"llm","template":"{args}","access":"owner"}]`))
	}))
	defer srv.Close()

	s := &spyApply{}
	r := New(srv.URL, "secret-tok", 1, s.apply, nil)
	require.NotNil(t, r)

	r.refreshOnce(context.Background())

	calls, last := s.snapshot()
	assert.Equal(t, 1, calls)
	require.Len(t, last, 1)
	assert.Equal(t, "weather", last[0].Name)
	assert.Equal(t, commands.AccessOwner, last[0].Access)
	assert.Equal(t, "Bearer secret-tok", gotAuth.Load())
}

func TestRefreshSkipsApplyWhenUnchanged(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`[{"name":"ping","type":"static","template":"pong","access":"everyone"}]`))
	}))
	defer srv.Close()

	s := &spyApply{}
	r := New(srv.URL, "tok", 1, s.apply, nil)
	require.NotNil(t, r)

	r.refreshOnce(context.Background())
	r.refreshOnce(context.Background()) // identical response → no second apply

	calls, _ := s.snapshot()
	assert.Equal(t, 1, calls, "an unchanged command set must not trigger a second swap")
}

func TestRefreshKeepsLastSetOnError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	s := &spyApply{}
	r := New(srv.URL, "tok", 1, s.apply, nil)
	require.NotNil(t, r)

	r.refreshOnce(context.Background())

	calls, _ := s.snapshot()
	assert.Equal(t, 0, calls, "a non-200 must not apply a command set")
}

func TestRefreshKeepsLastSetOnMalformedBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`not json`))
	}))
	defer srv.Close()

	s := &spyApply{}
	r := New(srv.URL, "tok", 1, s.apply, nil)
	require.NotNil(t, r)

	r.refreshOnce(context.Background())

	calls, _ := s.snapshot()
	assert.Equal(t, 0, calls, "a body that fails to decode must not apply a command set")
}

func TestRunRefreshesImmediatelyAndStopsOnCancel(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		_, _ = w.Write([]byte(`[]`))
	}))
	defer srv.Close()

	s := &spyApply{}
	r := New(srv.URL, "tok", 0, s.apply, nil)
	require.NotNil(t, r)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	require.Eventually(t, func() bool { return hits.Load() >= 1 }, time.Second, 10*time.Millisecond,
		"Run must refresh once immediately")

	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

func TestNewClampsSubFloorInterval(t *testing.T) {
	r := New("http://x", "tok", 1, (&spyApply{}).apply, nil) // 1s < floor
	require.NotNil(t, r)
	assert.Equal(t, minInterval, r.interval)
}
