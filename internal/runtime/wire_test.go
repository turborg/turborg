package runtime_test

import (
	"context"
	"iter"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/agent"
	"github.com/turborg/turborg/internal/connector/irc"
	"github.com/turborg/turborg/internal/llm"
	"github.com/turborg/turborg/internal/runtime"
	"github.com/turborg/turborg/internal/skill"
)

// stubProvider is a minimal llm.Provider — enough for WireCommon to build an
// LLM-type command; its methods are never invoked by these tests.
type stubProvider struct{}

func (stubProvider) Model() string { return "stub" }
func (stubProvider) Ask(context.Context, string, ...llm.CallOption) (string, llm.Usage, error) {
	return "", llm.Usage{}, nil
}
func (stubProvider) Stream(context.Context, string, ...llm.CallOption) iter.Seq2[string, error] {
	return func(func(string, error) bool) {}
}

func newWiredAgent(t *testing.T, p runtime.CommonParams) *agent.Agent {
	t.Helper()
	if p.CustomCommandsMax == 0 {
		p.CustomCommandsMax = -1 // unrestricted unless a test pins it
	}
	a := agent.NewWithPrefix(nil, "!")
	cfg := &irc.Settings{Hostname: "irc.example", Nick: "bot"}
	cfg.ApplyDefaults()
	conn := irc.New(cfg, nil, a.Events)
	_, err := runtime.WireCommon(a, conn, p, nil)
	require.NoError(t, err)
	return a
}

func TestWireCommonInstallsConfiguredCommands(t *testing.T) {
	a := newWiredAgent(t, runtime.CommonParams{
		Commands: []skill.Skill{
			skill.Command("rules", skill.TypeStatic, "be nice", skill.AccessEveryone),
		},
	})
	require.Contains(t, a.Commands.Names(), "rules")
}

func TestWireCommonSkipsLLMCommandsWithoutProvider(t *testing.T) {
	defs := []skill.Skill{
		skill.Command("ask", skill.TypeLLM, "{args}", skill.AccessOwner),
	}

	withoutLLM := newWiredAgent(t, runtime.CommonParams{Commands: defs})
	require.NotContains(t, withoutLLM.Commands.Names(), "ask",
		"no provider → LLM-type command must be skipped")

	withLLM := newWiredAgent(t, runtime.CommonParams{Commands: defs, LLM: stubProvider{}})
	require.Contains(t, withLLM.Commands.Names(), "ask",
		"provider present → LLM-type command registers")
}

func TestWireCommonEveryoneAccessDispatchesForAnyone(t *testing.T) {
	a := newWiredAgent(t, runtime.CommonParams{
		Commands: []skill.Skill{
			skill.Command("rules", skill.TypeStatic, "be nice", skill.AccessEveryone),
		},
	})
	out, err := a.Commands.Dispatch(context.Background(), &agent.InboundEnvelope{Text: "!rules", Sender: "anyone"})
	require.NoError(t, err)
	require.NotNil(t, out, "everyone access trusts any sender")
	require.Equal(t, "be nice", out.Text)
}

func TestWireCommonOwnerAccessDeniesNoneMode(t *testing.T) {
	// Default owner mode "none" denies an owner-access command for everyone.
	a := newWiredAgent(t, runtime.CommonParams{
		Commands: []skill.Skill{
			skill.Command("secret", skill.TypeStatic, "shh", skill.AccessOwner),
		},
	})
	out, err := a.Commands.Dispatch(context.Background(), &agent.InboundEnvelope{Text: "!secret", Sender: "anyone"})
	require.NoError(t, err)
	require.Nil(t, out, "owner_mode=none must deny an owner-access command")
}

func TestWireCommonOwnerAccessSelfMode(t *testing.T) {
	a := newWiredAgent(t, runtime.CommonParams{
		Owner: runtime.GuardParams{OwnerMode: "self", BotNick: "bot"},
		Commands: []skill.Skill{
			skill.Command("secret", skill.TypeStatic, "shh", skill.AccessOwner),
		},
	})

	allowed, err := a.Commands.Dispatch(context.Background(), &agent.InboundEnvelope{Text: "!secret", Sender: "bot"})
	require.NoError(t, err)
	require.NotNil(t, allowed, "self mode trusts the bot's own nick")
	require.Equal(t, "shh", allowed.Text)

	denied, err := a.Commands.Dispatch(context.Background(), &agent.InboundEnvelope{Text: "!secret", Sender: "stranger"})
	require.NoError(t, err)
	require.Nil(t, denied, "self mode denies anyone but the bot")
}

func TestWireCommonAllowlistAccess(t *testing.T) {
	a := newWiredAgent(t, runtime.CommonParams{
		Owner: runtime.GuardParams{OwnerMode: "self", BotNick: "bot"},
		Commands: []skill.Skill{
			skill.Command("team", skill.TypeStatic, "ok", skill.AccessAllowlist, "alice"),
		},
	})

	owner, err := a.Commands.Dispatch(context.Background(), &agent.InboundEnvelope{Text: "!team", Sender: "bot"})
	require.NoError(t, err)
	require.NotNil(t, owner, "owner is always allowed")

	listed, err := a.Commands.Dispatch(context.Background(), &agent.InboundEnvelope{Text: "!team", Sender: "alice"})
	require.NoError(t, err)
	require.NotNil(t, listed, "an allowlisted nick is allowed")

	other, err := a.Commands.Dispatch(context.Background(), &agent.InboundEnvelope{Text: "!team", Sender: "mallory"})
	require.NoError(t, err)
	require.Nil(t, other, "a non-owner non-allowlisted sender is denied")
}

func TestWireCommonWrapsLLMWithBudget(t *testing.T) {
	a := newWiredAgent(t, runtime.CommonParams{
		LLM:          stubProvider{},
		LLMInputCap:  1000,
		LLMOutputCap: 500,
		Commands: []skill.Skill{
			skill.Command("ask", skill.TypeLLM, "hi", skill.AccessEveryone),
		},
	})
	require.Contains(t, a.Commands.Names(), "ask")
}

func TestWireCommonNoBudgetWhenCapsZero(t *testing.T) {
	a := newWiredAgent(t, runtime.CommonParams{
		LLM:          stubProvider{},
		LLMInputCap:  0,
		LLMOutputCap: 0,
		Commands: []skill.Skill{
			skill.Command("ask", skill.TypeLLM, "hi", skill.AccessEveryone),
		},
	})
	require.Contains(t, a.Commands.Names(), "ask")
}

func TestBuildBudgetedProviderNilReturnsNil(t *testing.T) {
	a := agent.NewWithPrefix(nil, "!")
	require.Nil(t, runtime.BuildBudgetedProvider(a, nil, 100, 100, 0, 0, nil))
}

func TestBuildBudgetedProviderNoCapsReturnsRaw(t *testing.T) {
	a := agent.NewWithPrefix(nil, "!")
	raw := stubProvider{}
	got := runtime.BuildBudgetedProvider(a, raw, 0, 0, 0, 0, nil)
	require.Equal(t, raw, got)
}

func TestBuildBudgetedProviderWrapsWhenCapsSet(t *testing.T) {
	a := agent.NewWithPrefix(nil, "!")
	got := runtime.BuildBudgetedProvider(a, stubProvider{}, 100, 50, 0, 0, nil)
	_, ok := got.(*llm.BudgetedProvider)
	require.True(t, ok, "expected a *llm.BudgetedProvider wrapper")
}

func TestBuildBudgetedProviderIdempotent(t *testing.T) {
	a := agent.NewWithPrefix(nil, "!")
	wrapped := runtime.BuildBudgetedProvider(a, stubProvider{}, 100, 50, 0, 0, nil)
	again := runtime.BuildBudgetedProvider(a, wrapped, 100, 50, 0, 0, nil)
	require.Same(t, wrapped, again, "re-wrapping must return the same instance")
}

func TestBuildBudgetedProviderPublishesUsageEvent(t *testing.T) {
	a := agent.NewWithPrefix(nil, "!")
	got := make(chan *agent.Event, 1)
	a.Events.Subscribe(agent.EventLLMUsage, func(_ context.Context, ev *agent.Event) {
		got <- ev
	})
	wrapped := runtime.BuildBudgetedProvider(a, &usageProvider{in: 30, out: 12, cached: 8}, 1000, 500, 0, 0, nil)
	_, _, err := wrapped.Ask(context.Background(), "hi")
	require.NoError(t, err)
	select {
	case ev := <-got:
		require.Equal(t, 30, ev.Fields["input_tokens"])
		require.Equal(t, 12, ev.Fields["output_tokens"])
		require.Equal(t, 8, ev.Fields["cached_tokens"])
		require.Equal(t, "usage", ev.Fields["model"])
	default:
		t.Fatal("expected an EventLLMUsage to be published")
	}
}

// usageProvider reports fixed token usage so the budgeted wrapper records
// and publishes a usage event.
type usageProvider struct {
	in     int
	out    int
	cached int
}

func (usageProvider) Model() string { return "usage" }
func (p *usageProvider) Ask(context.Context, string, ...llm.CallOption) (string, llm.Usage, error) {
	return "ok", llm.Usage{InputTokens: p.in, OutputTokens: p.out, CachedTokens: p.cached}, nil
}
func (usageProvider) Stream(context.Context, string, ...llm.CallOption) iter.Seq2[string, error] {
	return func(func(string, error) bool) {}
}
