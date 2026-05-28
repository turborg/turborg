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
)

// stubProvider is a minimal llm.Provider — enough for WireCommon to register
// the !ask builtin; its methods are never invoked by these tests.
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
	a := agent.NewWithPrefix(nil, "!")
	cfg := &irc.Settings{Hostname: "irc.example", Nick: "bot"}
	cfg.ApplyDefaults()
	conn := irc.New(cfg, nil, a.Events)
	require.NoError(t, runtime.WireCommon(a, conn, p, nil))
	return a
}

func TestWireCommonRegistersBuiltinsWithoutLLM(t *testing.T) {
	a := newWiredAgent(t, runtime.CommonParams{})
	names := a.Commands.Names()
	require.Contains(t, names, "ping")
	require.Contains(t, names, "version")
	require.NotContains(t, names, "ask", "no provider → !ask must not register")
}

func TestWireCommonRegistersAskWithLLM(t *testing.T) {
	a := newWiredAgent(t, runtime.CommonParams{LLM: stubProvider{}})
	require.Contains(t, a.Commands.Names(), "ask", "provider present → !ask registers")
}

func TestWireCommonOwnerGuardDeniesNoneMode(t *testing.T) {
	// Default owner mode "none" denies every !command — the same lock-down the
	// dedicated runtime gets, closing the gap where pooled ran builtins ungated.
	a := newWiredAgent(t, runtime.CommonParams{})
	out, err := a.Commands.Dispatch(context.Background(), &agent.InboundEnvelope{Text: "!ping", Sender: "anyone"})
	require.NoError(t, err)
	require.Nil(t, out, "owner_mode=none must deny !ping")
}

func TestWireCommonOwnerGuardSelfMode(t *testing.T) {
	a := newWiredAgent(t, runtime.CommonParams{
		Owner: runtime.GuardParams{OwnerMode: "self", BotNick: "bot"},
	})

	allowed, err := a.Commands.Dispatch(context.Background(), &agent.InboundEnvelope{Text: "!ping", Sender: "bot"})
	require.NoError(t, err)
	require.NotNil(t, allowed, "self mode trusts the bot's own nick")
	require.Equal(t, "pong", allowed.Text)

	denied, err := a.Commands.Dispatch(context.Background(), &agent.InboundEnvelope{Text: "!ping", Sender: "stranger"})
	require.NoError(t, err)
	require.Nil(t, denied, "self mode denies anyone but the bot")
}
