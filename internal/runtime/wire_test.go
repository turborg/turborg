package runtime_test

import (
	"context"
	"iter"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/agent"
	"github.com/turborg/turborg/internal/commands"
	"github.com/turborg/turborg/internal/connector/irc"
	"github.com/turborg/turborg/internal/llm"
	"github.com/turborg/turborg/internal/runtime"
)

// stubProvider is a minimal llm.Provider — enough for WireCommon to build an
// LLM-type command; its methods are never invoked by these tests.
type stubProvider struct{}

func (stubProvider) Model() string { return "stub" }
func (stubProvider) Ask(context.Context, string, ...llm.CallOption) (string, error) {
	return "", nil
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
	require.NoError(t, runtime.WireCommon(a, conn, p, nil))
	return a
}

func TestWireCommonInstallsConfiguredCommands(t *testing.T) {
	a := newWiredAgent(t, runtime.CommonParams{
		Commands: []commands.Definition{
			{Name: "rules", Type: commands.TypeStatic, Template: "be nice", Access: commands.AccessEveryone},
		},
	})
	require.Contains(t, a.Commands.Names(), "rules")
}

func TestWireCommonSkipsLLMCommandsWithoutProvider(t *testing.T) {
	defs := []commands.Definition{
		{Name: "ask", Type: commands.TypeLLM, Template: "{args}", Access: commands.AccessOwner},
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
		Commands: []commands.Definition{
			{Name: "rules", Type: commands.TypeStatic, Template: "be nice", Access: commands.AccessEveryone},
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
		Commands: []commands.Definition{
			{Name: "secret", Type: commands.TypeStatic, Template: "shh", Access: commands.AccessOwner},
		},
	})
	out, err := a.Commands.Dispatch(context.Background(), &agent.InboundEnvelope{Text: "!secret", Sender: "anyone"})
	require.NoError(t, err)
	require.Nil(t, out, "owner_mode=none must deny an owner-access command")
}

func TestWireCommonOwnerAccessSelfMode(t *testing.T) {
	a := newWiredAgent(t, runtime.CommonParams{
		Owner: runtime.GuardParams{OwnerMode: "self", BotNick: "bot"},
		Commands: []commands.Definition{
			{Name: "secret", Type: commands.TypeStatic, Template: "shh", Access: commands.AccessOwner},
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
		Commands: []commands.Definition{
			{Name: "team", Type: commands.TypeStatic, Template: "ok", Access: commands.AccessAllowlist, Allowlist: []string{"alice"}},
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
