package server

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/agent"
	"github.com/turborg/turborg/internal/connector/irc"
)

// liveHookTenant builds a tenant whose web gateway is live and whose inbound
// webhook dispatches to the supplied fire func — the auth + routing surface the
// hook ingress adds, without needing an upstream IRC connection.
func liveHookTenant(t *testing.T, id, token string, fire func(string, map[string]string) bool) *Tenant {
	t.Helper()
	bus := agent.NewEventBus(testLogger())
	conn := irc.New(&irc.Settings{
		Hostname: "127.0.0.1", Port: 6667, Nick: "bot", Username: "bot", RealName: "pooled tenant",
	}, testLogger(), bus)
	gw, err := buildTenantGateway(conn, token, testLogger(), nil, nil, 0, nil, fire)
	require.NoError(t, err)
	gw.Subscribe(bus)
	t.Cleanup(gw.Stop)
	return &Tenant{ID: id, log: testLogger(), gateway: gw}
}

// TestRouteHookLookup: RouteHook returns false for an unknown tenant (router
// 404s), and true for an attached one whose ServeHook answers 404 when it has no
// live gateway between runs.
func TestRouteHookLookup(t *testing.T) {
	s := New(nil, testLogger())
	s.tenants["known"] = &Tenant{ID: "known", log: testLogger()} // no live gateway

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/c/missing/hook/x?token=x", strings.NewReader("{}"))
	assert.False(t, s.RouteHook("missing", rec, req), "missing tenant routes false")

	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodPost, "/c/known/hook/x?token=x", strings.NewReader("{}"))
	assert.True(t, s.RouteHook("known", rec2, req2), "attached tenant routes true")
	assert.Equal(t, http.StatusNotFound, rec2.Code, "a tenant with no live gateway answers 404")
}

// TestWebRouterHookEndToEnd drives the pooled route POST /c/<id>/hook/<name>
// through the real listener: a valid token fires the tenant's dispatcher, a bad
// token is 401, and an unknown tenant is 404.
func TestWebRouterHookEndToEnd(t *testing.T) {
	var mu sync.Mutex
	var gotName string
	var gotBag map[string]string
	fire := func(name string, bag map[string]string) bool {
		mu.Lock()
		defer mu.Unlock()
		gotName, gotBag = name, bag
		return true
	}

	s := New(nil, testLogger())
	s.tenants["alice"] = liveHookTenant(t, "alice", "good-token", fire)
	addr, stop := startWebRouter(t, s)
	defer stop()

	// Valid token → 200, dispatcher saw the name + flattened body.
	resp, err := http.Post("http://"+addr+"/c/alice/hook/deploy?token=good-token",
		"application/json", strings.NewReader(`{"text":"ship","from":"ci"}`))
	require.NoError(t, err)
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	mu.Lock()
	assert.Equal(t, "deploy", gotName)
	assert.Equal(t, "ship", gotBag["text"])
	assert.Equal(t, "ci", gotBag["user"], "{user} seeded from from")
	mu.Unlock()

	// Bad token → 401.
	resp2, err := http.Post("http://"+addr+"/c/alice/hook/deploy?token=nope",
		"application/json", strings.NewReader(`{}`))
	require.NoError(t, err)
	resp2.Body.Close()
	assert.Equal(t, http.StatusUnauthorized, resp2.StatusCode)

	// Unknown tenant → 404.
	resp3, err := http.Post("http://"+addr+"/c/ghost/hook/deploy?token=good-token",
		"application/json", strings.NewReader(`{}`))
	require.NoError(t, err)
	resp3.Body.Close()
	assert.Equal(t, http.StatusNotFound, resp3.StatusCode)
}
