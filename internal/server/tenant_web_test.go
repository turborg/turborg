package server

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/coder/websocket"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/agent"
	webconn "github.com/turborg/turborg/internal/connector/web"
	"github.com/turborg/turborg/internal/skill"
)

// newWebTenant builds a Tenant whose spec carries one web connector with the
// given extra config keys merged in. The GatewayToken (the tenant's
// container_token) doubles as the web-chat token signing key.
func newWebTenant(configOverrides map[string]any, cmds ...skill.Skill) *Tenant {
	cfg := map[string]any{"bot_nick": "helper", "room": "console"}
	for k, v := range configOverrides {
		cfg[k] = v
	}
	return &Tenant{
		ID:  "t1",
		log: testLogger(),
		spec: TenantSpec{
			TurborgID:        "t1",
			CommandPrefix:    "!",
			GatewayToken:     "signing-key",
			Connectors:       []ConnectorSpec{{Type: "web", Config: cfg, Secrets: map[string]any{}}},
			PlanCapabilities: &PlanCapabilities{CustomCommandsMax: -1},
			Commands:         cmds,
		},
	}
}

// mintWebToken signs a web-chat token in the canonical format the connector
// verifies against.
func mintWebToken(t *testing.T, key, tid, room, role string) string {
	t.Helper()
	payload, err := json.Marshal(map[string]any{
		"tid": tid, "room": room, "role": role, "vid": "u1", "iat": 1000, "exp": 4102444800,
	})
	require.NoError(t, err)
	seg := base64.RawURLEncoding.EncodeToString(payload)
	mac := hmac.New(sha256.New, []byte(key))
	mac.Write([]byte(seg))
	return seg + "." + base64.RawURLEncoding.EncodeToString(mac.Sum(nil))
}

// liveWebTenant builds a tenant whose web connector is live (no agent running),
// enough to exercise the router's auth + upgrade surface.
func liveWebTenant(t *testing.T, id, key string) *Tenant {
	t.Helper()
	v, err := webconn.NewSignedTokenVerifier(key)
	require.NoError(t, err)
	conn := webconn.New(webconn.Settings{BotNick: "helper", Room: "console"}, testLogger(), agent.NewEventBus(testLogger()), v)
	t.Cleanup(func() { _ = conn.Stop(context.Background()) })
	return &Tenant{ID: id, log: testLogger(), webConn: conn}
}

func TestWebSettingsFromConnectorSpec(t *testing.T) {
	cs := ConnectorSpec{Config: map[string]any{"bot_nick": "helper", "room": "lobby", "public": true}}
	s, v, err := webSettingsFromConnectorSpec(cs, "key")
	require.NoError(t, err)
	require.NotNil(t, v)
	assert.Equal(t, "helper", s.BotNick)
	assert.Equal(t, "lobby", s.Room)
	assert.True(t, s.Public)
}

func TestWebSettingsFromConnectorSpecDefaults(t *testing.T) {
	s, _, err := webSettingsFromConnectorSpec(ConnectorSpec{Config: map[string]any{}}, "key")
	require.NoError(t, err)
	assert.Equal(t, "turborg", s.BotNick)
	assert.Equal(t, "console", s.Room)
	assert.False(t, s.Public)
}

func TestWebSettingsFromConnectorSpecEmptyKey(t *testing.T) {
	_, _, err := webSettingsFromConnectorSpec(ConnectorSpec{Config: map[string]any{}}, "")
	assert.Error(t, err)
}

// TestBuildConnectorsWiresWebConnector proves the web arm installs the tenant's
// data-driven commands through the shared WireCore and captures the live
// connector for the chat router.
func TestBuildConnectorsWiresWebConnector(t *testing.T) {
	tn := newWebTenant(nil,
		skill.Command("rules", skill.TypeStatic, "be nice", skill.AccessEveryone))
	a := agent.NewWithPrefix(tn.log, "!")
	tn.buildConnectors(a)
	t.Cleanup(func() {
		tn.mu.Lock()
		conn := tn.webConn
		tn.mu.Unlock()
		if conn != nil {
			_ = conn.Stop(context.Background())
		}
	})

	require.Contains(t, a.Commands.Names(), "rules")
	tn.mu.Lock()
	conn := tn.webConn
	tn.mu.Unlock()
	require.NotNil(t, conn, "buildConnectors must capture the live web connector for the chat router")
}

// TestBuildWebConnectorEmptyTokenSkipped: with no GatewayToken there's no
// signing key, so the web arm skips the connector rather than build one that
// can't authenticate anyone.
func TestBuildWebConnectorEmptyTokenSkipped(t *testing.T) {
	tn := newWebTenant(nil)
	tn.spec.GatewayToken = ""
	a := agent.NewWithPrefix(tn.log, "!")
	tn.buildConnectors(a)

	tn.mu.Lock()
	conn := tn.webConn
	tn.mu.Unlock()
	assert.Nil(t, conn, "no signing key → no web connector")
}

// TestServeChatNoConnector: a tenant with no live web connector answers 404.
func TestServeChatNoConnector(t *testing.T) {
	tn := &Tenant{ID: "t1", log: testLogger()}
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/chat/t1?token=x", nil)
	tn.ServeChat(rec, req)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestRouteChatLookup: RouteChat returns false for an unknown tenant and true
// for an attached one (whose ServeChat 404s with no live connector).
func TestRouteChatLookup(t *testing.T) {
	s := New(nil, testLogger())
	s.tenants["known"] = &Tenant{ID: "known", log: testLogger()}

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/chat/missing?token=x", nil)
	assert.False(t, s.RouteChat("missing", rec, req))

	rec2 := httptest.NewRecorder()
	req2 := httptest.NewRequest(http.MethodGet, "/chat/known?token=x", nil)
	assert.True(t, s.RouteChat("known", rec2, req2))
	assert.Equal(t, http.StatusNotFound, rec2.Code)
}

// TestWebChatRouterUpgradeAndAuth is the end-to-end: a live web tenant is
// reachable at /chat/<id>?token=<good> and upgrades the WS; a bad token 401s; a
// token minted for another tenant 401s (cross-tenant replay fails closed); an
// unknown tenant 404s.
func TestWebChatRouterUpgradeAndAuth(t *testing.T) {
	s := New(nil, testLogger())
	s.tenants["alice"] = liveWebTenant(t, "alice", "key-a")
	s.tenants["bob"] = liveWebTenant(t, "bob", "key-b")
	addr, stop := startWebRouter(t, s)
	defer stop()

	good := mintWebToken(t, "key-a", "alice", "console", "owner")
	conn, _, err := websocket.Dial(context.Background(), "ws://"+addr+"/chat/alice?token="+good, nil)
	require.NoError(t, err, "valid token upgrades the WS")
	_ = conn.Close(websocket.StatusNormalClosure, "")

	_, resp, err := websocket.Dial(context.Background(), "ws://"+addr+"/chat/alice?token=nope", nil)
	if resp != nil {
		defer resp.Body.Close()
	}
	require.Error(t, err, "bad token must not upgrade")
	require.Equal(t, http.StatusUnauthorized, resp.StatusCode)

	// Cross-tenant replay: alice's token presented to bob's path must fail
	// closed (bob's key wouldn't validate the HMAC, and the tid check is the
	// belt-and-braces).
	_, resp2, err := websocket.Dial(context.Background(), "ws://"+addr+"/chat/bob?token="+good, nil)
	if resp2 != nil {
		defer resp2.Body.Close()
	}
	require.Error(t, err, "cross-tenant token must not upgrade")
	require.Equal(t, http.StatusUnauthorized, resp2.StatusCode)

	_, resp3, err := websocket.Dial(context.Background(), "ws://"+addr+"/chat/ghost?token="+good, nil)
	if resp3 != nil {
		defer resp3.Body.Close()
	}
	require.Error(t, err, "unknown tenant is 404")
	require.Equal(t, http.StatusNotFound, resp3.StatusCode)
}
