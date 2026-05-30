package budgetrefresh

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
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

type spyBudget struct {
	mu       sync.Mutex
	input    int
	output   int
	setCalls int
}

func (s *spyBudget) SetBaseline(input, output int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.input, s.output = input, output
	s.setCalls++
}

func (s *spyBudget) snapshot() (int, int, int) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.input, s.output, s.setCalls
}

func TestNewReturnsNilWhenUnconfigured(t *testing.T) {
	b := &spyBudget{}
	assert.Nil(t, New("", "tok", 0, time.Now(), b, nil), "no endpoint → nil")
	assert.Nil(t, New("http://x", "", 0, time.Now(), b, nil), "no token → nil")
	assert.Nil(t, New("http://x", "tok", 0, time.Now(), nil, nil), "no budget → nil")
	assert.NotNil(t, New("http://x", "tok", 0, time.Now(), b, nil))
}

func TestRefreshAppliesBaselineAndSendsSinceAndAuth(t *testing.T) {
	var gotSince atomic.Value
	var gotAuth atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotSince.Store(r.URL.Query().Get("since"))
		gotAuth.Store(r.Header.Get("Authorization"))
		_, _ = w.Write([]byte(`{"input_used": 1200, "output_used": 300}`))
	}))
	defer srv.Close()

	b := &spyBudget{}
	since := time.Unix(1_700_000_000, 0)
	r := New(srv.URL, "secret-tok", 1, since, b, nil)
	require.NotNil(t, r)

	r.refreshOnce(context.Background())

	in, out, calls := b.snapshot()
	assert.Equal(t, 1200, in)
	assert.Equal(t, 300, out)
	assert.Equal(t, 1, calls)
	assert.Equal(t, "1700000000", gotSince.Load())
	assert.Equal(t, "Bearer secret-tok", gotAuth.Load())
}

func TestRefreshKeepsLastBaselineOnError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	b := &spyBudget{}
	r := New(srv.URL, "tok", 1, time.Now(), b, nil)
	require.NotNil(t, r)

	r.refreshOnce(context.Background())

	_, _, calls := b.snapshot()
	assert.Equal(t, 0, calls, "a non-200 must not push a baseline (keeps the last value)")
}

func TestRunRefreshesImmediatelyAndStopsOnContextCancel(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		_, _ = w.Write([]byte(`{"input_used": 5, "output_used": 1}`))
	}))
	defer srv.Close()

	b := &spyBudget{}
	r := New(srv.URL, "tok", 0, time.Now(), b, nil) // default interval; immediate first refresh
	require.NotNil(t, r)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	require.Eventually(t, func() bool { return hits.Load() >= 1 }, time.Second, 10*time.Millisecond,
		"Run must refresh once immediately, not wait a full interval")

	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

func TestRefreshKeepsLastBaselineOnMalformedBody(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`not json`))
	}))
	defer srv.Close()

	b := &spyBudget{}
	r := New(srv.URL, "tok", 1, time.Now(), b, nil)
	require.NotNil(t, r)

	r.refreshOnce(context.Background())

	_, _, calls := b.snapshot()
	assert.Equal(t, 0, calls, "a body that fails to decode must not push a baseline")
}

func TestRunRefreshesOnTick(t *testing.T) {
	var hits atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		_, _ = w.Write([]byte(`{"input_used": 1, "output_used": 0}`))
	}))
	defer srv.Close()

	b := &spyBudget{}
	r := New(srv.URL, "tok", 2, time.Now(), b, nil) // clamped to the 2s floor
	require.NotNil(t, r)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	// One immediate refresh + at least one ticked refresh.
	require.Eventually(t, func() bool { return hits.Load() >= 2 }, 5*time.Second, 50*time.Millisecond,
		"Run must keep refreshing on each tick")

	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

func TestNewClampsSubFloorInterval(t *testing.T) {
	b := &spyBudget{}
	r := New("http://x", "tok", 1, time.Now(), b, nil) // 1s < floor
	require.NotNil(t, r)
	assert.Equal(t, minInterval, r.interval)
}
