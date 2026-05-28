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
	"github.com/turborg/turborg/tests/fixtures/fakeirc"
)

// TestPooledTenantPostsConnectorState: with a control plane configured, a
// pooled tenant POSTs its connector-state snapshot (to
// /turborgs/<id>/state, bearer-authed) on upstream transitions — the path that
// drives appui's connector pill for pooled the way dedicated already does.
func TestPooledTenantPostsConnectorState(t *testing.T) {
	fs := fakeirc.New(t)
	defer fs.Close()

	var (
		mu                      sync.Mutex
		gotPath, gotAuth, gotNS string
		gotState                string
		received                = make(chan struct{}, 1)
	)
	cp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Connectors map[string]struct {
				State string `json:"state"`
				Nick  string `json:"nick"`
			} `json:"connectors"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		mu.Lock()
		gotPath, gotAuth, gotNS = r.URL.Path, r.Header.Get("Authorization"), r.Method
		if c, ok := body.Connectors["irc"]; ok && c.State != "" {
			gotState = c.State
		}
		mu.Unlock()
		select {
		case received <- struct{}{}:
		default:
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer cp.Close()

	src := &StaticSource{Tenants: []TenantSpec{ircTenantSpec("state-tenant", fs.Port(), "statenick", "#state")}}
	srv := New(src, testLogger())
	srv.SetControlPlane(cp.URL, "cp-token")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()

	require.True(t, fs.WaitFor(lineContaining("JOIN #state"), 3*time.Second),
		"tenant did not attach; received: %v", fs.Received())

	select {
	case <-received:
	case <-time.After(3 * time.Second):
		t.Fatal("control plane never received a connector-state POST")
	}

	mu.Lock()
	path, auth, method, state := gotPath, gotAuth, gotNS, gotState
	mu.Unlock()
	require.Equal(t, http.MethodPost, method, "pooled state-sync POSTs (the receiver is a POST route)")
	require.Equal(t, "/turborgs/state-tenant/state", path)
	require.Equal(t, "Bearer cp-token", auth)
	require.NotEmpty(t, state, "the snapshot must carry the connector's upstream state")

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("server did not drain within 3s of cancel")
	}
}

// TestPooledTenantNoControlPlaneNoEmitter: without a control plane, the tenant
// builds no emitter (state-sync off) — the OSS/file-source path stays inert.
func TestPooledTenantNoControlPlaneNoEmitter(t *testing.T) {
	require.Nil(t, buildTenantStateEmitter(nil, "id", "", "tok", testLogger()),
		"empty control-plane URL → no emitter")
	require.Nil(t, buildTenantStateEmitter(nil, "", "http://cp", "tok", testLogger()),
		"empty turborg id → no emitter")
}
