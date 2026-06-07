package configrefresh_test

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/turborg/turborg/internal/configrefresh"
)

func TestNewReturnsNilWhenUnconfigured(t *testing.T) {
	apply := func(configrefresh.Config) {}
	require.Nil(t, configrefresh.New("", "tok", 0, apply, nil), "no endpoint → nil")
	require.Nil(t, configrefresh.New("http://x", "", 0, apply, nil), "no token → nil")
	require.Nil(t, configrefresh.New("http://x", "tok", 0, nil, nil), "no apply → nil")
}

func TestRefresherAppliesAndSkipsUnchanged(t *testing.T) {
	var hits int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&hits, 1)
		assert.Equal(t, "Bearer tok", r.Header.Get("Authorization"))
		_, _ = w.Write([]byte(`{"nick":"alice","channels":["#a","#b"]}`))
	}))
	t.Cleanup(srv.Close)

	var mu sync.Mutex
	var applied []configrefresh.Config
	apply := func(c configrefresh.Config) {
		mu.Lock()
		applied = append(applied, c)
		mu.Unlock()
	}

	r := configrefresh.New(srv.URL, "tok", 1, apply, nil)
	require.NotNil(t, r)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = r.Run(ctx); close(done) }()

	// First poll applies.
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(applied) == 1
	}, 2*time.Second, 10*time.Millisecond)

	// Later identical polls must NOT re-apply (skip-unchanged). The interval
	// floors at 2s, so wait past one tick to ensure a second poll happened
	// and was skipped.
	require.Never(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(applied) != 1
	}, 2500*time.Millisecond, 100*time.Millisecond)

	cancel()
	<-done

	mu.Lock()
	defer mu.Unlock()
	require.Equal(t, "alice", applied[0].Nick)
	require.Equal(t, []string{"#a", "#b"}, applied[0].Channels)
	require.GreaterOrEqual(t, atomic.LoadInt32(&hits), int32(2), "should have polled more than once")
}

func TestRefresherKeepsLastOnError(t *testing.T) {
	cases := []struct {
		name    string
		status  int
		payload string
	}{
		{"non-200", http.StatusInternalServerError, "boom"},
		{"invalid-json", http.StatusOK, "not json"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.status)
				_, _ = w.Write([]byte(tc.payload))
			}))
			defer srv.Close()

			var calls int32
			r := configrefresh.New(srv.URL, "tok", 0, func(configrefresh.Config) {
				atomic.AddInt32(&calls, 1)
			}, slog.Default())
			require.NotNil(t, r)

			ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
			defer cancel()
			_ = r.Run(ctx) // immediate poll errors, no apply; ctx ends the loop
			require.Zero(t, atomic.LoadInt32(&calls), "apply must not fire on a fetch error")
		})
	}
}
