package server

import (
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/agent"
)

// newChatTenant builds a Tenant carrying a single dial-out chat connector. No
// control plane / gateway token, so buildConnectors wires only the connector +
// the shared chat wiring (runtime.WireCore) — no network is opened.
func newChatTenant(cs ConnectorSpec) *Tenant {
	return &Tenant{
		ID:  "t1",
		log: slog.Default(),
		spec: TenantSpec{
			TurborgID:        "t1",
			CommandPrefix:    "!",
			Connectors:       []ConnectorSpec{cs},
			PlanCapabilities: &PlanCapabilities{CustomCommandsMax: -1},
		},
	}
}

func TestBuildConnectorsDiscord(t *testing.T) {
	tn := newChatTenant(ConnectorSpec{
		Type:    "discord",
		Config:  map[string]any{"guild_id": "g1"},
		Secrets: map[string]any{"bot_token": "tok"},
	})
	a := agent.NewWithPrefix(tn.log, "!")
	tn.buildConnectors(a)

	tn.mu.Lock()
	conn, wiring := tn.discordConn, tn.wiring
	tn.mu.Unlock()
	require.NotNil(t, conn, "the discord connector is captured on the tenant")
	require.NotNil(t, wiring)
}

func TestBuildConnectorsTelegram(t *testing.T) {
	tn := newChatTenant(ConnectorSpec{
		Type:    "telegram",
		Config:  map[string]any{},
		Secrets: map[string]any{"bot_token": "tok"},
	})
	a := agent.NewWithPrefix(tn.log, "!")
	tn.buildConnectors(a)

	tn.mu.Lock()
	conn, wiring := tn.telegramConn, tn.wiring
	tn.mu.Unlock()
	require.NotNil(t, conn)
	require.NotNil(t, wiring)
}

func TestBuildConnectorsSlack(t *testing.T) {
	tn := newChatTenant(ConnectorSpec{
		Type:    "slack",
		Config:  map[string]any{},
		Secrets: map[string]any{"bot_token": "xoxb-1", "app_token": "xapp-1"},
	})
	a := agent.NewWithPrefix(tn.log, "!")
	tn.buildConnectors(a)

	tn.mu.Lock()
	conn, wiring := tn.slackConn, tn.wiring
	tn.mu.Unlock()
	require.NotNil(t, conn)
	require.NotNil(t, wiring)
}

// TestBuildConnectorsSkipsInvalidChatSpec proves an invalid chat spec (missing
// required secret) is logged and skipped rather than captured — the same
// fail-soft contract the IRC/web arms follow.
func TestBuildConnectorsSkipsInvalidChatSpec(t *testing.T) {
	tn := newChatTenant(ConnectorSpec{
		Type:    "discord",
		Config:  map[string]any{}, // no guild_id
		Secrets: map[string]any{}, // no bot_token
	})
	a := agent.NewWithPrefix(tn.log, "!")
	tn.buildConnectors(a)

	tn.mu.Lock()
	conn := tn.discordConn
	tn.mu.Unlock()
	require.Nil(t, conn, "an invalid spec is skipped, not captured")
}

// TestBuildConnectorsUnsupportedType exercises the default arm: an unknown
// connector type is warned and skipped without capturing anything.
func TestBuildConnectorsUnsupportedType(t *testing.T) {
	tn := newChatTenant(ConnectorSpec{Type: "carrier-pigeon", Config: map[string]any{}, Secrets: map[string]any{}})
	a := agent.NewWithPrefix(tn.log, "!")
	tn.buildConnectors(a) // must not panic

	tn.mu.Lock()
	defer tn.mu.Unlock()
	require.Nil(t, tn.wiring, "an unsupported type wires nothing")
}
