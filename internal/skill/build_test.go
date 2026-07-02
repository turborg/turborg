package skill

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/agent"
)

func TestBuildOnlyCommandSkills(t *testing.T) {
	skills := []Skill{
		Command("rules", TypeStatic, "be nice {nick}", AccessEveryone),
		{Name: "greet", Trigger: Trigger{Kind: KindEvent, Event: EventUserJoin}, Action: Action{Type: TypeStatic, Template: "hi"}},
		{Name: "flood", Trigger: Trigger{Kind: KindMatch, Match: ".*"}, Action: Action{Type: TypeEffect}},
	}
	built := Build(skills, nil, nil, "", "", nil)
	require.Len(t, built, 1, "only the command-kind skill becomes a command handler")
	assert.Equal(t, "rules", built[0].Name)

	env := agent.NewInbound("irc", "#c", "alice", "!rules")
	out, err := built[0].Handler(context.Background(), env, nil)
	require.NoError(t, err)
	require.NotNil(t, out)
	assert.Equal(t, "be nice alice", out.Text)
}

func TestBuildAppliesGuardFactory(t *testing.T) {
	var seen []string
	guardFor := func(s Skill) agent.CommandGuard {
		seen = append(seen, s.Name)
		return func(env *agent.InboundEnvelope) bool { return env.Sender == "owner" }
	}
	built := Build([]Skill{Command("x", TypeStatic, "ok", AccessOwner)}, nil, guardFor, "", "", nil)
	require.Len(t, built, 1)
	require.Equal(t, []string{"x"}, seen)
	require.NotNil(t, built[0].Guard)
	assert.True(t, built[0].Guard(&agent.InboundEnvelope{Sender: "owner"}))
	assert.False(t, built[0].Guard(&agent.InboundEnvelope{Sender: "nobody"}))
}

func TestBuildSkipsLLMWithoutProvider(t *testing.T) {
	built := Build([]Skill{Command("ask", TypeLLM, "{args}", AccessOwner)}, nil, nil, "", "", nil)
	assert.Empty(t, built, "an llm command with no provider is skipped")
}

func TestBuildLLMWithProvider(t *testing.T) {
	prov := &fakeProvider{resp: "the answer"}
	built := Build([]Skill{Command("ask", TypeLLM, "Q: {args}", AccessOwner)}, prov, nil, "", "", nil)
	require.Len(t, built, 1)
	out, err := built[0].Handler(context.Background(), agent.NewInbound("irc", "#c", "u", "!ask why"), []string{"why"})
	require.NoError(t, err)
	assert.Equal(t, "the answer", out.Text)
	assert.Equal(t, "Q: why", prov.lastPrompt())
}
