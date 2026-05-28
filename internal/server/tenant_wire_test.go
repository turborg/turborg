package server

import (
	"context"
	"log/slog"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/agent"
	"github.com/turborg/turborg/internal/connector/irc"
	"github.com/turborg/turborg/internal/messages"
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
// fields + ignored nicks from the spec, identity/throttle/nudge limits from
// caps, and the store threaded in.
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
		OwnerDMNudgeEvery:     100,
		CommandMaxPerWindow:   3,
		CommandWindowSeconds:  30,
	}
	store := messages.NewMemoryStore(0)

	p := tn.commonParams(cs, caps, "bot", store, []string{"spammer", "troll"})

	require.Equal(t, "external", p.Owner.OwnerMode)
	require.Equal(t, "alice", p.Owner.OwnerNick)
	require.Equal(t, "aliceacct", p.Owner.OwnerAccount)
	require.Equal(t, "*!*@trusted.host", p.Owner.OwnerHostmask)
	require.Equal(t, "bot", p.Owner.BotNick)
	require.Equal(t, []string{"spammer", "troll"}, p.Owner.IgnoredNicks)
	require.Equal(t, 3, p.Owner.CommandMaxPerWindow)
	require.Equal(t, 30, p.Owner.CommandWindowSeconds)
	require.Equal(t, "alice", p.OwnerNick)
	require.Equal(t, 100, p.OwnerDMNudgeEvery)

	require.True(t, p.Limits.NickLocked)
	require.True(t, p.Limits.RealnameLocked)
	require.Equal(t, 5, p.Limits.MaxChannels)
	require.Equal(t, 7, p.OutboundMaxPerWindow)
	require.Equal(t, 30, p.OutboundWindowSeconds)

	require.Equal(t, store, p.Store, "the store is threaded straight through")
	require.Nil(t, p.ActivityHook, "no aggregator on a directly-built tenant → no activity hook")
}

// TestApplyHostAndTierSettings: the host QUIT brand + per-tier CTCP / bouncer-
// failed caps override the connector's ApplyDefaults — closing the last drifts
// where pooled fell back to defaults dedicated overrode from the sidecar env.
func TestApplyHostAndTierSettings(t *testing.T) {
	tn := &Tenant{quitMessage: "xshellz.com — free, turborg.com"}
	s := &irc.Settings{Hostname: "irc.example", Nick: "bot"}
	s.ApplyDefaults() // quit "bye from turborg", CTCP 3/30, bouncer-failed 5

	tn.applyHostAndTierSettings(s, &PlanCapabilities{
		CTCPMaxPerWindow:         2,
		CTCPWindowSeconds:        30,
		BouncerMaxFailedAttempts: 3,
	})

	require.Equal(t, "xshellz.com — free, turborg.com", s.QuitMessage)
	require.Equal(t, 2, s.CTCPMaxPerWindow)
	require.Equal(t, 30, s.CTCPWindowSeconds)
	require.Equal(t, 3, s.BouncerMaxFailedAttempts)
}

// TestApplyHostAndTierSettingsKeepsDefaults: with no host brand + nil caps
// (OSS/file-source), the connector keeps its ApplyDefaults values untouched.
func TestApplyHostAndTierSettingsKeepsDefaults(t *testing.T) {
	tn := &Tenant{} // no quitMessage
	s := &irc.Settings{Hostname: "irc.example", Nick: "bot"}
	s.ApplyDefaults()

	tn.applyHostAndTierSettings(s, nil)

	require.Equal(t, "bye from turborg", s.QuitMessage)
	require.Equal(t, 3, s.CTCPMaxPerWindow)
	require.Equal(t, 5, s.BouncerMaxFailedAttempts)
}

// TestBuildMessageStoreMemoryWithoutControlPlane: the OSS/file-source path with
// no control plane gets an in-process store and no sink (nothing to close).
func TestBuildMessageStoreMemoryWithoutControlPlane(t *testing.T) {
	tn := &Tenant{ID: "t1", log: slog.Default()}
	store, sink := tn.buildMessageStore()
	require.NotNil(t, store)
	require.Nil(t, sink)
}

// TestBuildMessageStoreHTTPWithControlPlane: with a control plane the store is
// the write-through HTTP store pointed at the per-tenant endpoint, and a sink
// (owning a flush goroutine) is returned for the caller to Close.
func TestBuildMessageStoreHTTPWithControlPlane(t *testing.T) {
	tn := &Tenant{
		ID:                "t1",
		log:               slog.Default(),
		controlPlaneURL:   "https://cp.example/v1/internal",
		controlPlaneToken: "host-token",
	}
	store, sink := tn.buildMessageStore()
	require.NotNil(t, store)
	require.NotNil(t, sink, "control plane configured → durable sink with a flush goroutine")

	// Close the sink so its goroutine doesn't leak past the test.
	sink.Close(context.Background())
}
