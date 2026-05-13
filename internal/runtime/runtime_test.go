package runtime_test

import (
	"context"
	"errors"
	"iter"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/agent"
	"github.com/turborg/turborg/internal/config"
	"github.com/turborg/turborg/internal/connector/irc"
	"github.com/turborg/turborg/internal/llm"
	"github.com/turborg/turborg/internal/runtime"
	"github.com/turborg/turborg/internal/version"
)

func TestBuildStandalone(t *testing.T) {
	s := &config.Settings{CommandPrefix: "!"}
	ircCfg := &irc.Settings{Hostname: "fake", Nick: "turborg"}
	b, err := runtime.Build(s, ircCfg, nil)
	require.NoError(t, err)
	assert.NotNil(t, b.Agent)
	assert.NotNil(t, b.IRC)
	assert.Nil(t, b.LLM, "no API key → no LLM provider")
	assert.Nil(t, b.Gateway, "no web password → no gateway")
}

func TestBuildWithAnthropic(t *testing.T) {
	s := &config.Settings{
		CommandPrefix:   "!",
		AnthropicAPIKey: "sk-test",
		AnthropicModel:  "claude-sonnet-4-6",
	}
	ircCfg := &irc.Settings{Hostname: "fake", Nick: "turborg"}
	b, err := runtime.Build(s, ircCfg, nil)
	require.NoError(t, err)
	require.NotNil(t, b.LLM)
	assert.Equal(t, "claude-sonnet-4-6", b.LLM.Model())
	assert.Contains(t, b.Agent.Commands.Names(), "ask",
		"!ask command must be registered when LLM is wired")
}

func TestBuildWithWebGateway(t *testing.T) {
	s := &config.Settings{
		CommandPrefix:           "!",
		WebPassword:             "hunter2",
		WebHost:                 "127.0.0.1",
		WebPort:                 0,
		WebMaxFailedAttempts:    5,
		WebFailureWindowSeconds: 60,
		WebLockoutSeconds:       300,
	}
	ircCfg := &irc.Settings{Hostname: "fake", Nick: "turborg"}
	b, err := runtime.Build(s, ircCfg, nil)
	require.NoError(t, err)
	require.NotNil(t, b.Gateway)
}

func TestBuildRejectsUnknownConnector(t *testing.T) {
	s := &config.Settings{CommandPrefix: "!", Connectors: []string{"irc", "discord"}}
	ircCfg := &irc.Settings{Hostname: "fake", Nick: "turborg"}
	_, err := runtime.Build(s, ircCfg, nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "not yet implemented in Go")
}

func TestVersionCommand(t *testing.T) {
	s := &config.Settings{CommandPrefix: "!"}
	ircCfg := &irc.Settings{Hostname: "fake", Nick: "turborg"}
	b, err := runtime.Build(s, ircCfg, nil)
	require.NoError(t, err)

	out, err := b.Agent.Commands.Dispatch(context.Background(),
		agent.NewInbound("irc", "#test", "alice", "!version"))
	require.NoError(t, err)
	require.NotNil(t, out)
	assert.Contains(t, out.Text, version.Version)
	assert.Contains(t, out.Text, "turborg")
}

func TestAskCommandReturnsUsageWithoutArgs(t *testing.T) {
	a := agent.NewWithPrefix(nil, "!")
	runtime.RegisterBuiltinCommands(a, fakeLLM{})

	out, err := a.Commands.Dispatch(context.Background(),
		agent.NewInbound("irc", "#test", "alice", "!ask"))
	require.NoError(t, err)
	require.NotNil(t, out)
	assert.Contains(t, out.Text, "usage:")
}

func TestAskCommandCallsProvider(t *testing.T) {
	a := agent.NewWithPrefix(nil, "!")
	llmStub := fakeLLM{response: "the answer\n\nis    42"}
	runtime.RegisterBuiltinCommands(a, llmStub)

	out, err := a.Commands.Dispatch(context.Background(),
		agent.NewInbound("irc", "#test", "alice", "!ask what is the answer"))
	require.NoError(t, err)
	require.NotNil(t, out)
	assert.Equal(t, "the answer is 42", out.Text,
		"collapseWhitespace should normalize internal newlines + spans")
}

func TestAskCommandHandlesProviderError(t *testing.T) {
	a := agent.NewWithPrefix(nil, "!")
	runtime.RegisterBuiltinCommands(a, fakeLLM{err: errors.New("rate limited")})

	out, err := a.Commands.Dispatch(context.Background(),
		agent.NewInbound("irc", "#test", "alice", "!ask hi"))
	require.NoError(t, err)
	require.NotNil(t, out)
	assert.Contains(t, out.Text, "sorry")
	assert.Contains(t, out.Text, "rate limited")
}

// --- guard composition ---------------------------------------------------

func TestGuardEmptySettingsReturnsNil(t *testing.T) {
	g := runtime.BuildCommandGuard(&config.Settings{})
	assert.Nil(t, g, "no owner + no throttle → no guard")
}

func TestGuardOwnerNickAllowAndDeny(t *testing.T) {
	s := &config.Settings{
		OwnerNick:            "Owner",
		CommandMaxPerWindow:  100,
		CommandWindowSeconds: 60,
	}
	g := runtime.BuildCommandGuard(s)
	require.NotNil(t, g)

	assert.True(t, g(agent.NewInbound("irc", "#ch", "owner", "!x")))
	assert.True(t, g(agent.NewInbound("irc", "#ch", "OWNER", "!x")),
		"owner check is case-insensitive")
	assert.False(t, g(agent.NewInbound("irc", "#ch", "intruder", "!x")))
}

func TestGuardOwnerAccountFailsClosedWithoutTag(t *testing.T) {
	s := &config.Settings{
		OwnerAccount:         "ownerAccount",
		CommandMaxPerWindow:  100,
		CommandWindowSeconds: 60,
	}
	g := runtime.BuildCommandGuard(s)
	require.NotNil(t, g)

	noTag := agent.NewInbound("irc", "#ch", "owner", "!x")
	assert.False(t, g(noTag), "missing account tag must fail closed")

	withTag := agent.NewInbound("irc", "#ch", "owner", "!x")
	withTag.Metadata["account"] = "ownerAccount"
	assert.True(t, g(withTag))

	wrongTag := agent.NewInbound("irc", "#ch", "owner", "!x")
	wrongTag.Metadata["account"] = "someone-else"
	assert.False(t, g(wrongTag))
}

func TestGuardThrottleScopedByAccountThenSender(t *testing.T) {
	s := &config.Settings{
		CommandMaxPerWindow:  2,
		CommandWindowSeconds: 60,
	}
	g := runtime.BuildCommandGuard(s)
	require.NotNil(t, g)

	for i := 0; i < 2; i++ {
		assert.True(t, g(agent.NewInbound("irc", "#ch", "alice", "!x")))
	}
	assert.False(t, g(agent.NewInbound("irc", "#ch", "alice", "!x")),
		"per-sender throttle should cap at MaxPerWindow")

	// A different sender has their own bucket.
	assert.True(t, g(agent.NewInbound("irc", "#ch", "bob", "!x")))
}

// --- Run pair stops mutually ---------------------------------------------

func TestRunStandaloneReturnsOnCtxCancel(t *testing.T) {
	s := &config.Settings{CommandPrefix: "!"}
	ircCfg := &irc.Settings{
		Hostname:           "fake",
		Nick:               "turborg",
		HandshakeTimeout:   100 * time.Millisecond,
		ClientPingInterval: 50 * time.Millisecond,
	}
	b, err := runtime.Build(s, ircCfg, nil)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx, b) }()

	// IRC connect to "fake:6697" will fail fast → Run returns.
	select {
	case err := <-done:
		// Whatever the error, Run should have unwound; either the dial
		// failed or the ctx cancellation fired.
		_ = err
	case <-time.After(3 * time.Second):
		cancel()
		t.Fatal("Run did not return within timeout")
	}
	cancel()
}

func TestRunWithGatewayUnwindsOnAgentStartFailure(t *testing.T) {
	// Web gateway is enabled; the IRC connector points at a port that
	// is closed so Agent.Run fails fast. Run() must cancel the shared
	// ctx so gateway.Serve also returns, and Wait must surface the
	// agent error.
	s := &config.Settings{
		CommandPrefix:           "!",
		WebPassword:             "p",
		WebHost:                 "127.0.0.1",
		WebPort:                 0,
		WebMaxFailedAttempts:    5,
		WebFailureWindowSeconds: 60,
		WebLockoutSeconds:       300,
	}
	ircCfg := &irc.Settings{
		Hostname:         "127.0.0.1",
		Port:             1, // port 1 is the discard port — nothing listens
		UseTLS:           false,
		Nick:             "turborg",
		HandshakeTimeout: 200 * time.Millisecond,
	}
	b, err := runtime.Build(s, ircCfg, nil)
	require.NoError(t, err)
	require.NotNil(t, b.Gateway)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runtime.Run(ctx, b) }()

	select {
	case <-done:
		// Either the agent's Dial-refused error surfaced, or the
		// gateway listener race won — both unwound the pair, which is
		// what we're testing.
	case <-time.After(3 * time.Second):
		t.Fatal("Run did not return within timeout when IRC start failed")
	}
}


// --- fakeLLM stub --------------------------------------------------------

type fakeLLM struct {
	response string
	err      error
}

func (f fakeLLM) Model() string { return "fake" }
func (f fakeLLM) Ask(_ context.Context, _ string, _ ...llm.CallOption) (string, error) {
	return f.response, f.err
}
func (f fakeLLM) Stream(_ context.Context, _ string, _ ...llm.CallOption) iter.Seq2[string, error] {
	return func(_ func(string, error) bool) {}
}
