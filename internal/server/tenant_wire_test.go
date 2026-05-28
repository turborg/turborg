package server

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/agent"
)

// newIRCTenant builds a Tenant whose spec carries one IRC connector with the
// given extra config keys merged in. No control plane / gateway token, so
// buildConnectors wires only the connector + shared agent wiring.
func newIRCTenant(configOverrides map[string]any, caps *PlanCapabilities) *Tenant {
	cfg := map[string]any{"network": "irc.example:6697", "nick": "bot"}
	for k, v := range configOverrides {
		cfg[k] = v
	}
	return &Tenant{
		ID:  "t1",
		log: slog.Default(),
		spec: TenantSpec{
			TurborgID:        "t1",
			CommandPrefix:    "!",
			Connectors:       []ConnectorSpec{{Type: "irc", Config: cfg, Secrets: map[string]any{}}},
			PlanCapabilities: caps,
		},
	}
}

// TestBuildConnectorsWiresBuiltins proves the pooled path now gets the builtins
// the dedicated runtime has — the core of the unify. Before WireCommon, pooled
// agents only had agent.RegisterBuiltins (ping+help) and no version/ask.
func TestBuildConnectorsWiresBuiltins(t *testing.T) {
	tn := newIRCTenant(map[string]any{"owner_mode": "self"}, &PlanCapabilities{MaxChannels: 3})
	a := agent.NewWithPrefix(tn.log, "!")
	tn.buildConnectors(a)

	names := a.Commands.Names()
	require.Contains(t, names, "ping")
	require.Contains(t, names, "version")

	tn.mu.Lock()
	conn := tn.ircConn
	tn.mu.Unlock()
	require.NotNil(t, conn, "buildConnectors must capture the live connector for the bouncer router")
}

// TestBuildConnectorsOwnerGuardFromConfig proves the owner-trust config that
// travels in the connector config (owner_mode) is applied to the command guard
// for pooled tenants exactly as it is for dedicated.
func TestBuildConnectorsOwnerGuardFromConfig(t *testing.T) {
	tn := newIRCTenant(map[string]any{"owner_mode": "self"}, nil)
	a := agent.NewWithPrefix(tn.log, "!")
	tn.buildConnectors(a)

	// owner_mode=self → the bot's own nick is trusted; strangers are denied.
	allowed, err := a.Commands.Dispatch(context.Background(), &agent.InboundEnvelope{Text: "!ping", Sender: "bot"})
	require.NoError(t, err)
	require.NotNil(t, allowed)
	require.Equal(t, "pong", allowed.Text)

	denied, err := a.Commands.Dispatch(context.Background(), &agent.InboundEnvelope{Text: "!ping", Sender: "stranger"})
	require.NoError(t, err)
	require.Nil(t, denied)
}

// TestBuildConnectorsOwnerGuardDefaultDenies proves a tenant with no owner
// config falls back to "none" — every !command denied, matching dedicated.
func TestBuildConnectorsOwnerGuardDefaultDenies(t *testing.T) {
	tn := newIRCTenant(nil, nil)
	a := agent.NewWithPrefix(tn.log, "!")
	tn.buildConnectors(a)

	out, err := a.Commands.Dispatch(context.Background(), &agent.InboundEnvelope{Text: "!ping", Sender: "bot"})
	require.NoError(t, err)
	require.Nil(t, out, "no owner_mode → none → !commands denied")
}

// TestCommonParamsMapsCapsAndOwner locks the spec→CommonParams mapping: owner
// fields come from the connector config, identity/throttle limits from caps.
func TestCommonParamsMapsCapsAndOwner(t *testing.T) {
	tn := newIRCTenant(nil, nil)
	cs := ConnectorSpec{Config: map[string]any{
		"owner_mode":     "external",
		"owner_nick":     "alice",
		"owner_account":  "aliceacct",
		"owner_hostmask": "*!*@trusted.host",
	}}
	caps := &PlanCapabilities{
		NickLocked:            true,
		RealnameLocked:        true,
		MaxChannels:           5,
		OutboundMsgsPerWindow: 7,
		OutboundWindowSeconds: 30,
	}

	p := tn.commonParams(cs, caps, "bot")

	require.Equal(t, "external", p.Owner.OwnerMode)
	require.Equal(t, "alice", p.Owner.OwnerNick)
	require.Equal(t, "aliceacct", p.Owner.OwnerAccount)
	require.Equal(t, "*!*@trusted.host", p.Owner.OwnerHostmask)
	require.Equal(t, "bot", p.Owner.BotNick)
	require.Equal(t, "alice", p.OwnerNick)

	require.True(t, p.Limits.NickLocked)
	require.True(t, p.Limits.RealnameLocked)
	require.Equal(t, 5, p.Limits.MaxChannels)
	require.Equal(t, 7, p.OutboundMaxPerWindow)
	require.Equal(t, 30, p.OutboundWindowSeconds)

	require.NotNil(t, p.Store, "pooled tenants get an in-process store for scrollback (Phase 1)")
}
