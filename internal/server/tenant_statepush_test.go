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
	"github.com/turborg/turborg/internal/agent"
	telegramconn "github.com/turborg/turborg/internal/connector/telegram"
	"github.com/turborg/turborg/tests/fixtures/fakeirc"
)

// TestPooledTenantPostsConnectorState: with a control plane configured, a
// pooled tenant POSTs its connector-state snapshot (to
// /turborgs/<id>/state, bearer-authed) on upstream transitions — the path that
// drives a downstream UI's connector status for pooled tenants the way the
// single-instance path already does.
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
	require.Nil(t, buildTenantStateEmitter([]agent.Connector{}, "id", "", "tok", testLogger()),
		"empty control-plane URL → no emitter")
	require.Nil(t, buildTenantStateEmitter(nil, "", "http://cp", "tok", testLogger()),
		"empty turborg id → no emitter")
	require.Nil(t, buildTenantStateEmitter(nil, "id", "http://cp", "tok", testLogger()),
		"no connectors → no emitter")
}

// TestMapConnectorStateToWire pins the locked connector-state → wire mapping
// (the control plane validates against these exact strings) plus the default.
func TestMapConnectorStateToWire(t *testing.T) {
	cases := map[string]string{
		"connected":    "registered",
		"connecting":   "connecting",
		"suspended":    "disconnected_by_user",
		"error":        "disconnected_auth_failed",
		"disconnected": "disconnected_transient",
		"":             "disconnected_transient",
		"anything":     "disconnected_transient",
	}
	for in, want := range cases {
		require.Equal(t, want, mapConnectorStateToWire(in), "state %q", in)
	}
}

// TestPooledTelegramTenantPostsMappedState: a tenant running a non-IRC
// connector (telegram) that reports its live state POSTs a snapshot whose
// connector entry carries the wire-mapped state — exercising the StateReporter
// branch of buildTenantSnapshot and the StateSubscriber branch of
// wireTenantStateEmitter.
func TestPooledTelegramTenantPostsMappedState(t *testing.T) {
	conn := telegramconn.New(&telegramconn.Settings{Token: "t"}, testLogger(), agent.NewEventBus(nil))
	t.Cleanup(func() { _ = conn.Stop(context.Background()) })

	var (
		mu       sync.Mutex
		gotState string
		gotNick  string
		received = make(chan struct{}, 1)
	)
	cp := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Connectors map[string]struct {
				State    string `json:"state"`
				Nick     string `json:"nick"`
				Channels []struct {
					Name string `json:"name"`
				} `json:"channels"`
			} `json:"connectors"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		mu.Lock()
		if c, ok := body.Connectors["telegram"]; ok {
			gotState, gotNick = c.State, c.Nick
		}
		mu.Unlock()
		select {
		case received <- struct{}{}:
		default:
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer cp.Close()

	em := buildTenantStateEmitter([]agent.Connector{conn}, "tg-tenant", cp.URL, "cp-token", testLogger())
	require.NotNil(t, em, "a telegram connector with a control plane builds an emitter")
	defer em.Stop()

	// A state transition on the connector must drive a POST via the wired hook.
	conn.Suspend() // → connector state "suspended" → wire "disconnected_by_user"

	select {
	case <-received:
	case <-time.After(3 * time.Second):
		t.Fatal("control plane never received a telegram connector-state POST")
	}

	mu.Lock()
	state, nick := gotState, gotNick
	mu.Unlock()
	require.Equal(t, "disconnected_by_user", state, "telegram suspended maps to the wire value")
	require.Equal(t, "turborg", nick, "the connector's BotName fills the nick field")
}
