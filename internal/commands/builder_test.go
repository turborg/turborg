package commands_test

import (
	"context"
	"errors"
	"iter"
	"sort"
	"strconv"
	"strings"
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

// TestBuildStaticRendersIRCAliases pins the backward-compatible IRC placeholder
// names ({nick}/{channel}) so existing skills authored before the
// connector-agnostic rename keep rendering identically.
func TestBuildStaticRendersIRCAliases(t *testing.T) {
	built := commands.Build([]commands.Definition{
		{Name: "greet", Type: commands.TypeStatic, Template: "hi {nick} in {channel}: {args}", Access: commands.AccessEveryone},
	}, nil, nil, "", "", nil)
	require.Len(t, built, 1)

	env := agent.NewInbound("irc", "#room", "alice", "!greet how are you")
	out := dispatch(t, built[0], env, []string{"how", "are", "you"})
	require.NotNil(t, out)
	assert.Equal(t, "hi alice in #room: how are you", out.Text)
}

// TestBuildStaticRendersGenericPlaceholders covers the connector-agnostic
// primary tokens plus the {network} alias mapping to {platform}.
func TestBuildStaticRendersGenericPlaceholders(t *testing.T) {
	built := commands.Build([]commands.Definition{
		{Name: "who", Type: commands.TypeStatic, Template: "{user}@{room} on {platform} (={network}) owner={owner}: {args}", Access: commands.AccessEveryone},
	}, nil, nil, "irc.libera.chat", "stefan", nil)
	require.Len(t, built, 1)

	env := agent.NewInbound("irc", "#chan", "alice", "!who x y")
	out := dispatch(t, built[0], env, []string{"x", "y"})
	require.NotNil(t, out)
	assert.Equal(t, "alice@#chan on irc.libera.chat (=irc.libera.chat) owner=stefan: x y", out.Text)
}

// TestBuildStaticRendersClockTokens checks the UTC date/time tokens render in
// the documented layouts. The clock value itself is not pinned — only its shape.
func TestBuildStaticRendersClockTokens(t *testing.T) {
	built := commands.Build([]commands.Definition{
		{Name: "now", Type: commands.TypeStatic, Template: "{date} | {time} | {datetime}", Access: commands.AccessEveryone},
	}, nil, nil, "", "", nil)
	require.Len(t, built, 1)

	out := dispatch(t, built[0], agent.NewInbound("irc", "#c", "bob", "!now"), nil)
	require.NotNil(t, out)
	parts := strings.Split(out.Text, " | ")
	require.Len(t, parts, 3)
	assert.Regexp(t, `^\d{4}-\d{2}-\d{2}$`, parts[0])
	assert.Regexp(t, `^\d{2}:\d{2}:\d{2}$`, parts[1])
	assert.Regexp(t, `^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2} UTC$`, parts[2])
}

// TestBuildStaticDynamicHelpers exercises {choice}, {random}, and {shuffle},
// including the malformed-bound and empty-list edge cases.
func TestBuildStaticDynamicHelpers(t *testing.T) {
	build := func(tmpl string) agent.DynamicCommand {
		b := commands.Build([]commands.Definition{
			{Name: "h", Type: commands.TypeStatic, Template: tmpl, Access: commands.AccessEveryone},
		}, nil, nil, "", "", nil)
		require.Len(t, b, 1)
		return b[0]
	}
	env := func() *agent.InboundEnvelope { return agent.NewInbound("irc", "#c", "u", "!h") }

	t.Run("choice picks an option", func(t *testing.T) {
		out := dispatch(t, build("{choice:red,green,blue}"), env(), nil)
		assert.Contains(t, []string{"red", "green", "blue"}, out.Text)
	})

	t.Run("choice empty list yields empty", func(t *testing.T) {
		out := dispatch(t, build("[{choice:}]"), env(), nil)
		assert.Equal(t, "[]", out.Text)
	})

	t.Run("random within bounds", func(t *testing.T) {
		for range 50 {
			out := dispatch(t, build("{random:6}"), env(), nil)
			n, err := strconv.Atoi(out.Text)
			require.NoError(t, err)
			assert.GreaterOrEqual(t, n, 1)
			assert.LessOrEqual(t, n, 6)
		}
	})

	t.Run("random with bad bound left literal", func(t *testing.T) {
		assert.Equal(t, "{random:0}", dispatch(t, build("{random:0}"), env(), nil).Text)
		assert.Equal(t, "{random:x}", dispatch(t, build("{random:x}"), env(), nil).Text)
	})

	t.Run("shuffle is a permutation", func(t *testing.T) {
		out := dispatch(t, build("{shuffle:a,b,c,d}"), env(), nil)
		got := strings.Split(out.Text, ",")
		sort.Strings(got)
		assert.Equal(t, []string{"a", "b", "c", "d"}, got)
	})
}

// TestBuildStaticArgsCannotInjectHelper guards the substitution order: a helper
// token arriving via user-supplied {args} must NOT be evaluated, only the
// author's template is. Otherwise a user could trigger random/choice expansion.
func TestBuildStaticArgsCannotInjectHelper(t *testing.T) {
	built := commands.Build([]commands.Definition{
		{Name: "echo", Type: commands.TypeStatic, Template: "you said: {args}", Access: commands.AccessEveryone},
	}, nil, nil, "", "", nil)
	require.Len(t, built, 1)

	out := dispatch(t, built[0], agent.NewInbound("irc", "#c", "u", "!echo {random:9}"), []string{"{random:9}"})
	require.NotNil(t, out)
	assert.Equal(t, "you said: {random:9}", out.Text, "a helper inside args stays literal")
}

func TestBuildLLMPassesModelAndSystem(t *testing.T) {
	prov := &recordingProvider{resp: "the answer\n\nis   42"}
	built := commands.Build([]commands.Definition{
		{Name: "ask", Type: commands.TypeLLM, Template: "Q: {args}", Model: "openai/gpt-4o-mini", Access: commands.AccessOwner},
	}, prov, nil, "", "", nil)
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
	}, prov, nil, "", "", nil)
	require.Len(t, built, 1)

	dispatch(t, built[0], agent.NewInbound("irc", "#room", "bob", "!pirate hi"), []string{"hi"})
	assert.Equal(t, "Reply like a pirate.", prov.system, "skill instructions become the system prompt")
}

func TestBuildLLMFallsBackToDefaultSystemPromptWhenNoInstructions(t *testing.T) {
	prov := &recordingProvider{resp: "ok"}
	built := commands.Build([]commands.Definition{
		{Name: "ask", Type: commands.TypeLLM, Template: "{args}", Access: commands.AccessOwner},
	}, prov, nil, "", "", nil)
	require.Len(t, built, 1)

	dispatch(t, built[0], agent.NewInbound("irc", "#room", "bob", "!ask hi"), []string{"hi"})
	assert.NotEmpty(t, prov.system, "a default system prompt applies when no instructions are set")
}

func TestBuildLLMReportsProviderError(t *testing.T) {
	prov := &recordingProvider{err: errors.New("rate limited")}
	built := commands.Build([]commands.Definition{
		{Name: "ask", Type: commands.TypeLLM, Template: "{args}", Access: commands.AccessOwner},
	}, prov, nil, "", "", nil)
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
	}, prov, nil, "", "", nil)
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
	}, nil, nil, "", "", nil)
	require.Len(t, built, 1, "the llm command is skipped with no provider")
	assert.Equal(t, "rules", built[0].Name)
}

func TestBuildSkipsUnknownType(t *testing.T) {
	built := commands.Build([]commands.Definition{
		{Name: "weird", Type: "mystery", Template: "x", Access: commands.AccessEveryone},
	}, nil, nil, "", "", nil)
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
	}, nil, guardFor, "", "", nil)
	require.Len(t, built, 1)
	require.Len(t, seen, 1)
	require.NotNil(t, built[0].Guard)
	assert.True(t, built[0].Guard(&agent.InboundEnvelope{Sender: "owner"}))
	assert.False(t, built[0].Guard(&agent.InboundEnvelope{Sender: "stranger"}))
}
