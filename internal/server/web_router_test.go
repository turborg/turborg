package server

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/agent"
	"github.com/turborg/turborg/internal/connector/irc"
	"github.com/turborg/turborg/tests/fixtures/fakeirc"
)

// liveGatewayTenant builds a tenant whose web gateway is live, around an
// unstarted IRC connector. No upstream is needed: the gateway upgrades and
// streams its initial state frame from the connector's in-memory state, which
// is exactly the auth + routing surface PR A adds.
func liveGatewayTenant(t *testing.T, id, token string) *Tenant {
	t.Helper()
	bus := agent.NewEventBus(testLogger())
	conn := irc.New(&irc.Settings{
		Hostname: "127.0.0.1",
		Port:     6667,
		UseTLS:   false,
		Nick:     "bot",
		Username: "bot",
		RealName: "pooled tenant",
	}, testLogger(), bus)
	gw, err := buildTenantGateway(conn, token, testLogger(), nil, nil, 0, nil, nil)
	require.NoError(t, err)
	gw.Subscribe(bus)
	t.Cleanup(gw.Stop)
	return &Tenant{ID: id, log: testLogger(), gateway: gw}
}

// startWebRouter runs the web gateway router on an ephemeral port and returns
// its address plus a stop func that cancels and joins it (so goleak sees a
// clean shutdown).
func startWebRouter(t *testing.T, s *Server) (addr string, stop func()) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- s.serveWebGatewayRouter(ctx, ln) }()
	return ln.Addr().String(), func() {
		cancel()
		select {
		case <-errc:
		case <-time.After(2 * time.Second):
			t.Error("web router did not stop on cancel")
		}
	}
}

// TestRouteWSLookup: RouteWS returns false for an unknown tenant (leaving the
// ResponseWriter untouched for the router to 404), and true for an attached one
// — whose ServeWS answers 404 when it has no live gateway between runs.
func TestRouteWSLookup(t *testing.T) {
	s := New(nil, testLogger())
	s.tenants["known"] = &Tenant{ID: "known", log: testLogger()} // no live gateway

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/c/missing?token=x", nil)
	assert.False(t, s.RouteWS("missing", rec, req), "missing tenant routes false")

	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodGet, "/c/known?token=x", nil)
	assert.True(t, s.RouteWS("known", rec2, req2), "attached tenant routes true")
	assert.Equal(t, http.StatusNotFound, rec2.Code, "a tenant with no live gateway answers 404")
}

// TestServeWebGatewayRouterBindsAndStops covers the bind+delegate wrapper and a
// clean shutdown on context cancel.
func TestServeWebGatewayRouterBindsAndStops(t *testing.T) {
	s := New(nil, testLogger())
	ctx, cancel := context.WithCancel(context.Background())

	errc := make(chan error, 1)
	go func() { errc <- s.ServeWebGatewayRouter(ctx, "127.0.0.1:0") }()

	time.Sleep(50 * time.Millisecond) // let the listener bind
	cancel()

	select {
	case err := <-errc:
		assert.ErrorIs(t, err, context.Canceled, "router returns ctx.Err() on cancel")
	case <-time.After(2 * time.Second):
		t.Fatal("router did not stop on context cancel")
	}
}

// TestWebGatewayRouterUnknownTenant drives the full accept path: a request for a
// tenant that isn't attached gets a 404 over the real listener.
func TestWebGatewayRouterUnknownTenant(t *testing.T) {
	s := New(nil, testLogger())
	addr, stop := startWebRouter(t, s)
	defer stop()

	_, resp, err := websocket.Dial(context.Background(), "ws://"+addr+"/c/ghost?token=x", nil)
	if resp != nil {
		defer resp.Body.Close()
	}
	require.Error(t, err, "no upgrade for an unknown tenant")
	require.Equal(t, http.StatusNotFound, resp.StatusCode)
}

// TestWebGatewayRouterUpgradeAndAuth is the PR A end-to-end: a tenant with a
// live gateway is reachable at /c/<id>?token=<good> and upgrades the WS; a bad
// token is 401; an unknown tenant on the same live router is 404.
func TestWebGatewayRouterUpgradeAndAuth(t *testing.T) {
	s := New(nil, testLogger())
	s.tenants["alice"] = liveGatewayTenant(t, "alice", "good-token")
	addr, stop := startWebRouter(t, s)
	defer stop()

	conn, _, err := websocket.Dial(context.Background(), "ws://"+addr+"/c/alice?token=good-token", nil)
	require.NoError(t, err, "valid token upgrades the WS")
	_ = conn.Close(websocket.StatusNormalClosure, "")

	_, resp, err := websocket.Dial(context.Background(), "ws://"+addr+"/c/alice?token=nope", nil)
	if resp != nil {
		defer resp.Body.Close()
	}
	require.Error(t, err, "bad token must not upgrade")
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)

	_, resp2, err := websocket.Dial(context.Background(), "ws://"+addr+"/c/ghost?token=good-token", nil)
	if resp2 != nil {
		defer resp2.Body.Close()
	}
	require.Error(t, err, "unknown tenant on the live router is 404")
	require.Equal(t, http.StatusNotFound, resp2.StatusCode)
}

// TestBuildTenantGatewayEmptyToken: an empty token can't authorize a web shell,
// so the builder errors rather than constructing a gateway that accepts the
// empty string. buildConnectors only calls it when the token is non-empty, but
// the guard is the StaticPasswordVerifier's, so prove it here.
func TestBuildTenantGatewayEmptyToken(t *testing.T) {
	conn := irc.New(&irc.Settings{Hostname: "127.0.0.1", Port: 6667, Nick: "bot"}, testLogger(), nil)
	_, err := buildTenantGateway(conn, "", testLogger(), nil, nil, 0, nil, nil)
	require.Error(t, err)
}

// TestPooledTenantWebShellEndToEnd runs a real pooled tenant (IRC connector
// against a fake upstream + a GatewayToken) through the Server: the gateway is
// built in buildConnectors when the tenant attaches, the web router upgrades a
// valid-token client and 401s a bad one, and a clean cancel drains the tenant
// (which stops the gateway). Exercises the full PR A wiring end to end.
func TestPooledTenantWebShellEndToEnd(t *testing.T) {
	fs := fakeirc.New(t)
	defer fs.Close()

	sp := ircTenantSpec("web-tenant", fs.Port(), "webnick", "#web")
	sp.GatewayToken = "shell-token"

	srv := New(&StaticSource{Tenants: []TenantSpec{sp}}, testLogger())
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()

	require.True(t, fs.WaitFor(lineContaining("JOIN #web"), 3*time.Second),
		"tenant did not attach; received: %v", fs.Received())

	// The gateway is built during attach — wait until it's live on the tenant.
	require.Eventually(t, func() bool {
		srv.mu.Lock()
		tn := srv.tenants["web-tenant"]
		srv.mu.Unlock()
		if tn == nil {
			return false
		}
		tn.mu.Lock()
		defer tn.mu.Unlock()
		return tn.gateway != nil
	}, 3*time.Second, 20*time.Millisecond, "tenant gateway never came live")

	addr, stop := startWebRouter(t, srv)
	defer stop()

	conn, _, err := websocket.Dial(context.Background(), "ws://"+addr+"/c/web-tenant?token=shell-token", nil)
	require.NoError(t, err, "valid token upgrades the live tenant's web shell")
	_ = conn.Close(websocket.StatusNormalClosure, "")

	_, resp, err := websocket.Dial(context.Background(), "ws://"+addr+"/c/web-tenant?token=bad", nil)
	if resp != nil {
		defer resp.Body.Close()
	}
	require.Error(t, err)
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("server did not drain within 3s of cancel")
	}
}
