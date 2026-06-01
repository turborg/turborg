package commands_test

import (
	"context"
	"errors"
	"iter"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/agent"
	"github.com/turborg/turborg/internal/commands"
	"github.com/turborg/turborg/internal/llm"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// recordingProvider captures the prompt + resolved options of the last Ask.
type recordingProvider struct {
	prompt string
	model  string
	system string
	maxTok int
	resp   string
	err    error
}

func (p *recordingProvider) Model() string { return "rec" }
func (p *recordingProvider) Ask(_ context.Context, prompt string, opts ...llm.CallOption) (string, llm.Usage, error) {
	co := llm.ApplyOptions(opts)
	p.prompt = prompt
	p.model = co.Model
	p.system = co.System
	p.maxTok = co.MaxTokens
	return p.resp, llm.Usage{InputTokens: 10, OutputTokens: 5}, p.err
}
func (p *recordingProvider) Stream(context.Context, string, ...llm.CallOption) iter.Seq2[string, error] {
	return func(func(string, error) bool) {}
}

func dispatch(t *testing.T, dc agent.DynamicCommand, env *agent.InboundEnvelope, args []string) *agent.OutboundEnvelope {
	t.Helper()
	env.Command = dc.Name
	env.Args = args
	out, err := dc.Handler(context.Background(), env, args)
	require.NoError(t, err)
	return out
}

func TestBuildStaticRendersPlaceholders(t *testing.T) {
	built := commands.Build([]commands.Definition{
		{Name: "greet", Type: commands.TypeStatic, Template: "hi {nick} in {channel}: {args}", Access: commands.AccessEveryone},
	}, nil, nil, nil)
	require.Len(t, built, 1)

	env := agent.NewInbound("irc", "#room", "alice", "!greet how are you")
	out := dispatch(t, built[0], env, []string{"how", "are", "you"})
	require.NotNil(t, out)
	assert.Equal(t, "hi alice in #room: how are you", out.Text)
}

func TestBuildLLMPassesModelAndSystem(t *testing.T) {
	prov := &recordingProvider{resp: "the answer\n\nis   42"}
	built := commands.Build([]commands.Definition{
		{Name: "ask", Type: commands.TypeLLM, Template: "Q: {args}", Model: "openai/gpt-4o-mini", Access: commands.AccessOwner},
	}, prov, nil, nil)
	require.Len(t, built, 1)

	env := agent.NewInbound("irc", "#room", "bob", "!ask why")
	out := dispatch(t, built[0], env, []string{"why"})
	require.NotNil(t, out)

	assert.Equal(t, "Q: why", prov.prompt)
	assert.Equal(t, "openai/gpt-4o-mini", prov.model, "the command's model must be passed via WithModel")
	assert.NotEmpty(t, prov.system, "the LLM system prompt must be set")
	assert.Equal(t, 512, prov.maxTok)
	assert.Equal(t, "the answer is 42", out.Text, "reply whitespace is collapsed for IRC")
}

func TestBuildLLMUsesInstructionsAsSystemPrompt(t *testing.T) {
	prov := &recordingProvider{resp: "ok"}
	built := commands.Build([]commands.Definition{
		{Name: "pirate", Type: commands.TypeLLM, Template: "{args}", Instructions: "Reply like a pirate.", Access: commands.AccessOwner},
	}, prov, nil, nil)
	require.Len(t, built, 1)

	dispatch(t, built[0], agent.NewInbound("irc", "#room", "bob", "!pirate hi"), []string{"hi"})
	assert.Equal(t, "Reply like a pirate.", prov.system, "skill instructions become the system prompt")
}

func TestBuildLLMFallsBackToDefaultSystemPromptWhenNoInstructions(t *testing.T) {
	prov := &recordingProvider{resp: "ok"}
	built := commands.Build([]commands.Definition{
		{Name: "ask", Type: commands.TypeLLM, Template: "{args}", Access: commands.AccessOwner},
	}, prov, nil, nil)
	require.Len(t, built, 1)

	dispatch(t, built[0], agent.NewInbound("irc", "#room", "bob", "!ask hi"), []string{"hi"})
	assert.NotEmpty(t, prov.system, "a default system prompt applies when no instructions are set")
}

func TestBuildLLMReportsProviderError(t *testing.T) {
	prov := &recordingProvider{err: errors.New("rate limited")}
	built := commands.Build([]commands.Definition{
		{Name: "ask", Type: commands.TypeLLM, Template: "{args}", Access: commands.AccessOwner},
	}, prov, nil, nil)
	require.Len(t, built, 1)

	out := dispatch(t, built[0], agent.NewInbound("irc", "#room", "bob", "!ask hi"), []string{"hi"})
	require.NotNil(t, out)
	assert.Contains(t, out.Text, "sorry")
	assert.Contains(t, out.Text, "rate limited")
}

func TestBuildLLMReportsBudgetExhausted(t *testing.T) {
	prov := &recordingProvider{err: llm.ErrBudgetExhausted}
	built := commands.Build([]commands.Definition{
		{Name: "ask", Type: commands.TypeLLM, Template: "{args}", Access: commands.AccessOwner},
	}, prov, nil, nil)
	require.Len(t, built, 1)

	out := dispatch(t, built[0], agent.NewInbound("irc", "#room", "bob", "!ask hi"), []string{"hi"})
	require.NotNil(t, out)
	assert.Contains(t, out.Text, "budget")
	assert.NotContains(t, out.Text, "sorry", "budget exhaustion gets its own message, not the generic error reply")
}

func TestBuildSkipsLLMCommandWithoutProvider(t *testing.T) {
	built := commands.Build([]commands.Definition{
		{Name: "ask", Type: commands.TypeLLM, Template: "{args}", Access: commands.AccessOwner},
		{Name: "rules", Type: commands.TypeStatic, Template: "be nice", Access: commands.AccessEveryone},
	}, nil, nil, nil)
	require.Len(t, built, 1, "the llm command is skipped with no provider")
	assert.Equal(t, "rules", built[0].Name)
}

func TestBuildSkipsUnknownType(t *testing.T) {
	built := commands.Build([]commands.Definition{
		{Name: "weird", Type: "mystery", Template: "x", Access: commands.AccessEveryone},
	}, nil, nil, nil)
	assert.Empty(t, built)
}

func TestBuildAppliesGuardFactory(t *testing.T) {
	var seen []commands.Definition
	guardFor := func(d commands.Definition) agent.CommandGuard {
		seen = append(seen, d)
		return func(env *agent.InboundEnvelope) bool { return env.Sender == "owner" }
	}
	built := commands.Build([]commands.Definition{
		{Name: "x", Type: commands.TypeStatic, Template: "ok", Access: commands.AccessOwner},
	}, nil, guardFor, nil)
	require.Len(t, built, 1)
	require.Len(t, seen, 1)
	require.NotNil(t, built[0].Guard)
	assert.True(t, built[0].Guard(&agent.InboundEnvelope{Sender: "owner"}))
	assert.False(t, built[0].Guard(&agent.InboundEnvelope{Sender: "stranger"}))
}
