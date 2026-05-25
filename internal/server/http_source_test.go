package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// fakeFeed is a controllable /tenants endpoint whose returned snapshot the
// test can swap between polls.
type fakeFeed struct {
	mu      sync.Mutex
	tenants []TenantSpec
	bearer  string
	gotAuth string
	gotHost string
}

func (f *fakeFeed) set(specs ...TenantSpec) {
	f.mu.Lock()
	f.tenants = specs
	f.mu.Unlock()
}

func (f *fakeFeed) handler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.gotAuth = r.Header.Get("Authorization")
		f.gotHost = r.URL.Query().Get("host_id")
		out := tenantsEnvelope{Tenants: f.tenants}
		f.mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(out)
	}
}

func newHTTPSourceTest(t *testing.T) (*HTTPSource, *fakeFeed) {
	feed := &fakeFeed{bearer: "secret"}
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/internal/tenants", feed.handler())
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)

	return &HTTPSource{
		BaseURL:  srv.URL + "/v1/internal",
		Bearer:   "secret",
		HostID:   "host-7",
		Interval: 15 * time.Millisecond,
		Client:   srv.Client(),
		Log:      testLogger(),
	}, feed
}

func TestHTTPSourceInitial(t *testing.T) {
	src, feed := newHTTPSourceTest(t)
	feed.set(spec("a", "irc"), spec("b"))

	specs, err := src.Initial(context.Background())
	require.NoError(t, err)
	require.Len(t, specs, 2)
	require.Equal(t, "Bearer secret", feed.gotAuth, "bearer must be sent")
	require.Equal(t, "host-7", feed.gotHost, "host_id must be sent")
}

func TestHTTPSourceInitialNon200(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/v1/internal/tenants", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	src := &HTTPSource{BaseURL: srv.URL + "/v1/internal", Client: srv.Client()}
	_, err := src.Initial(context.Background())
	require.Error(t, err)
}

func TestHTTPSourceWatchEmitsDeltas(t *testing.T) {
	src, feed := newHTTPSourceTest(t)
	feed.set(spec("a")) // baseline: poll seeds this silently

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	events, err := src.Watch(ctx)
	require.NoError(t, err)

	// Add "b": expect a single upsert for b (a is unchanged, not re-emitted).
	feed.set(spec("a"), spec("b", "irc"))
	ev := waitEvent(t, events)
	require.Equal(t, TenantUpserted, ev.Kind)
	require.Equal(t, "b", ev.Spec.TurborgID)

	// Drop "a": expect a remove for a.
	feed.set(spec("b", "irc"))
	ev = waitEvent(t, events)
	require.Equal(t, TenantRemoved, ev.Kind)
	require.Equal(t, "a", ev.TurborgID)
}

func waitEvent(t *testing.T, ch <-chan TenantEvent) TenantEvent {
	t.Helper()
	select {
	case ev := <-ch:
		return ev
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for tenant event")
		return TenantEvent{}
	}
}
