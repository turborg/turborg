package server

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/agent"
	"github.com/turborg/turborg/internal/commands"
	"github.com/turborg/turborg/internal/connector/irc"
)

func staticCmd(name, tmpl string) commands.Definition {
	return commands.Definition{Name: name, Type: commands.TypeStatic, Template: tmpl, Access: commands.AccessEveryone}
}

func specWithChannel(id, channel string) TenantSpec {
	return TenantSpec{
		TurborgID:   id,
		RuntimeMode: "pooled",
		Connectors: []ConnectorSpec{{
			Type:    "irc",
			Config:  map[string]any{"channels": []any{channel}},
			Secrets: map[string]any{},
		}},
	}
}

// TestUpdateRestartsTenantWithNewSpec is the M7 deliverable: changing a
// running tenant's spec restarts its work with the new config, without losing
// the tenant or disturbing the process.
func TestUpdateRestartsTenantWithNewSpec(t *testing.T) {
	var runs atomic.Int32
	seen := make(chan string, 8)

	events := make(chan TenantEvent)
	src := &StaticSource{Tenants: []TenantSpec{specWithChannel("x", "#one")}, Events: events}
	srv := New(src, testLogger())
	srv.workFactory = func(tn *Tenant) func(context.Context) error {
		return func(ctx context.Context) error {
			runs.Add(1)
			tn.mu.Lock()
			ch := fmt.Sprint(tn.spec.Connectors[0].Config["channels"])
			tn.mu.Unlock()
			seen <- ch
			<-ctx.Done()
			return ctx.Err()
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()

	require.Equal(t, "[#one]", waitStr(t, seen), "first run should see the initial channel")

	// Change the channel — the tenant must restart and observe the new spec.
	events <- TenantEvent{Kind: TenantUpserted, Spec: specWithChannel("x", "#two")}
	require.Equal(t, "[#two]", waitStr(t, seen), "restart should see the updated channel")
	require.GreaterOrEqual(t, int(runs.Load()), 2)
	require.Equal(t, 1, srv.Count(), "update must not duplicate the tenant")

	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestIdenticalUpsertDoesNotRestart(t *testing.T) {
	var runs atomic.Int32

	events := make(chan TenantEvent)
	src := &StaticSource{Tenants: []TenantSpec{specWithChannel("x", "#one")}, Events: events}
	srv := New(src, testLogger())
	srv.workFactory = func(_ *Tenant) func(context.Context) error {
		return func(ctx context.Context) error {
			runs.Add(1)
			<-ctx.Done()
			return ctx.Err()
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()

	require.Eventually(t, func() bool { return runs.Load() == 1 }, time.Second, 5*time.Millisecond)

	// Re-upsert the identical spec: must be a no-op (no restart).
	events <- TenantEvent{Kind: TenantUpserted, Spec: specWithChannel("x", "#one")}
	require.Never(t, func() bool { return runs.Load() != 1 }, 150*time.Millisecond, 15*time.Millisecond,
		"identical spec must not restart the tenant")

	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestCommandsOnlyChange(t *testing.T) {
	base := specWithChannel("x", "#one")
	withCmds := specWithChannel("x", "#one")
	withCmds.Commands = []commands.Definition{staticCmd("a", "x")}
	require.True(t, commandsOnlyChange(base, withCmds),
		"specs differing only in their command set is a commands-only change")

	chAndCmds := specWithChannel("x", "#two")
	chAndCmds.Commands = withCmds.Commands
	require.False(t, commandsOnlyChange(base, chAndCmds),
		"a channel change alongside a command change is not commands-only")
}

func TestLiveUpdatableOnlyChangeIgnoresTokenUsageBaseline(t *testing.T) {
	base := specWithChannel("x", "#one")
	base.PlanCapabilities = &PlanCapabilities{LLMInputTokensPerDay: 4000, LLMInputTokensUsed: 100}

	// Only the usage baseline moved (the feed refreshes it every poll).
	movedBaseline := specWithChannel("x", "#one")
	movedBaseline.PlanCapabilities = &PlanCapabilities{LLMInputTokensPerDay: 4000, LLMInputTokensUsed: 3900}
	require.True(t, liveUpdatableOnlyChange(base, movedBaseline),
		"a change confined to the usage baseline must be applied without a reconnect")
	require.True(t, commandSetsEqual(base.Commands, movedBaseline.Commands),
		"command sets are unchanged here")

	// A real cap change (not just usage) must still force a restart.
	capChange := specWithChannel("x", "#one")
	capChange.PlanCapabilities = &PlanCapabilities{LLMInputTokensPerDay: 40000, LLMInputTokensUsed: 3900}
	require.False(t, liveUpdatableOnlyChange(base, capChange),
		"a plan-cap change is not a live-updatable-only change")

	// A channel change alongside a baseline move must still force a restart.
	chChange := specWithChannel("x", "#two")
	chChange.PlanCapabilities = movedBaseline.PlanCapabilities
	require.False(t, liveUpdatableOnlyChange(base, chChange),
		"a connector change is not a live-updatable-only change")
}

func TestReloadCommandsSwapsInPlace(t *testing.T) {
	a := agent.NewWithPrefix(testLogger(), "!")
	a.Commands.SetMaxDynamic(-1)
	cfg := &irc.Settings{Hostname: "irc.example", Nick: "bot"}
	cfg.ApplyDefaults()
	conn := irc.New(cfg, testLogger(), a.Events)
	tn := &Tenant{
		ID:  "t1",
		log: testLogger(),
		spec: TenantSpec{
			Connectors: []ConnectorSpec{{Type: "irc", Config: map[string]any{"owner_mode": "self"}}},
		},
		agentRef: a,
		ircConn:  conn,
	}

	require.True(t, tn.reloadCommands([]commands.Definition{staticCmd("rules", "be nice")}))
	require.Contains(t, a.Commands.Names(), "rules")

	// A second reload fully replaces the prior set — no reconnect involved.
	require.True(t, tn.reloadCommands([]commands.Definition{staticCmd("hi", "yo")}))
	require.NotContains(t, a.Commands.Names(), "rules")
	require.Contains(t, a.Commands.Names(), "hi")
}

func TestReloadCommandsFallsBackWhenNotRunning(t *testing.T) {
	tn := &Tenant{ID: "t1", log: testLogger()}
	require.False(t, tn.reloadCommands([]commands.Definition{staticCmd("x", "y")}),
		"no live agent/connector → reload defers to a restart")
}

// TestUpdateCommandsOnlyReloadsWithoutRestart is the no-reconnect deliverable:
// a change to ONLY the command set swaps the live registry in place; the work
// loop is never re-run (no reconnect), and the live agent gets the new command.
func TestUpdateCommandsOnlyReloadsWithoutRestart(t *testing.T) {
	var runs atomic.Int32
	base := TenantSpec{
		TurborgID:   "x",
		RuntimeMode: "pooled",
		Connectors:  []ConnectorSpec{{Type: "irc", Config: map[string]any{"owner_mode": "self"}, Secrets: map[string]any{}}},
	}

	events := make(chan TenantEvent)
	src := &StaticSource{Tenants: []TenantSpec{base}, Events: events}
	srv := New(src, testLogger())
	srv.workFactory = func(tn *Tenant) func(context.Context) error {
		return func(ctx context.Context) error {
			runs.Add(1)
			// Stand in for defaultWork: publish a live agent + connector so an
			// in-place command reload can find them.
			a := agent.NewWithPrefix(tn.log, "!")
			a.Commands.SetMaxDynamic(-1)
			cfg := &irc.Settings{Hostname: "irc.example", Nick: "bot"}
			cfg.ApplyDefaults()
			conn := irc.New(cfg, tn.log, a.Events)
			tn.mu.Lock()
			tn.agentRef = a
			tn.ircConn = conn
			tn.mu.Unlock()
			<-ctx.Done()
			tn.mu.Lock()
			tn.agentRef = nil
			tn.ircConn = nil
			tn.mu.Unlock()
			return ctx.Err()
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()

	require.Eventually(t, func() bool { return runs.Load() == 1 }, 2*time.Second, 5*time.Millisecond)

	// Command-only upsert: must reload in place, not restart.
	cmdSpec := base
	cmdSpec.Commands = []commands.Definition{staticCmd("rules", "be nice")}
	events <- TenantEvent{Kind: TenantUpserted, Spec: cmdSpec}

	require.Eventually(t, func() bool {
		srv.mu.Lock()
		tn := srv.tenants["x"]
		srv.mu.Unlock()
		if tn == nil {
			return false
		}
		tn.mu.Lock()
		a := tn.agentRef
		tn.mu.Unlock()
		if a == nil {
			return false
		}
		for _, n := range a.Commands.Names() {
			if n == "rules" {
				return true
			}
		}
		return false
	}, 2*time.Second, 10*time.Millisecond, "the live agent must gain the command in place")

	require.Equal(t, int32(1), runs.Load(), "a command-only change must not restart the work loop")

	// A connection-affecting change (new channel) still restarts.
	chSpec := cmdSpec
	chSpec.Connectors = []ConnectorSpec{{Type: "irc", Config: map[string]any{"owner_mode": "self", "channels": []any{"#two"}}, Secrets: map[string]any{}}}
	events <- TenantEvent{Kind: TenantUpserted, Spec: chSpec}
	require.Eventually(t, func() bool { return runs.Load() == 2 }, 2*time.Second, 10*time.Millisecond,
		"a connector change must restart the work loop")

	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

// TestUpdateTokenUsageBaselineDoesNotRestart guards the regression the
// account-wide budget change could introduce: the feed refreshes
// llm_*_tokens_used on every poll as tokens accrue, and that must NOT drop the
// IRC session. A spec that differs only in the usage baseline is a no-op for
// the work loop.
func TestUpdateTokenUsageBaselineDoesNotRestart(t *testing.T) {
	var runs atomic.Int32
	base := TenantSpec{
		TurborgID:        "x",
		RuntimeMode:      "pooled",
		Connectors:       []ConnectorSpec{{Type: "irc", Config: map[string]any{"owner_mode": "self"}, Secrets: map[string]any{}}},
		PlanCapabilities: &PlanCapabilities{LLMInputTokensPerDay: 4000, LLMInputTokensUsed: 0},
	}

	events := make(chan TenantEvent)
	src := &StaticSource{Tenants: []TenantSpec{base}, Events: events}
	srv := New(src, testLogger())
	srv.workFactory = func(tn *Tenant) func(context.Context) error {
		return func(ctx context.Context) error {
			runs.Add(1)
			<-ctx.Done()
			return ctx.Err()
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()

	require.Eventually(t, func() bool { return runs.Load() == 1 }, 2*time.Second, 5*time.Millisecond)

	// Three successive feed polls, each reporting more usage. None may restart.
	for _, used := range []int{1000, 2500, 3900} {
		s := base
		s.PlanCapabilities = &PlanCapabilities{LLMInputTokensPerDay: 4000, LLMInputTokensUsed: used}
		events <- TenantEvent{Kind: TenantUpserted, Spec: s}
	}
	require.Never(t, func() bool { return runs.Load() != 1 }, 200*time.Millisecond, 20*time.Millisecond,
		"a usage-baseline-only change must never restart the work loop")

	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func waitStr(t *testing.T, ch <-chan string) string {
	t.Helper()
	select {
	case s := <-ch:
		return s
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for run observation")
		return ""
	}
}
