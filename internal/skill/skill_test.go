package skill

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/commands"
)

// TestLegacyCommandRoundTrip proves a legacy command definition decodes to a
// command/reply skill identical to today and marshals back byte-identically —
// the backward-compatibility guarantee.
func TestLegacyCommandRoundTrip(t *testing.T) {
	legacy := `{"name":"rules","type":"static","template":"be nice","access":"everyone"}`

	var s Skill
	require.NoError(t, json.Unmarshal([]byte(legacy), &s))

	assert.Equal(t, "rules", s.Name)
	assert.True(t, s.IsCommand(), "absent trigger_kind defaults to command")
	assert.Equal(t, KindCommand, s.Trigger.Kind)
	assert.Equal(t, TypeStatic, s.Action.Type)
	assert.Equal(t, "be nice", s.Action.Template)
	assert.Equal(t, AccessEveryone, s.Access)

	// The decoded skill projects onto the legacy command definition unchanged.
	def := s.ToDefinition()
	assert.Equal(t, commands.Definition{
		Name: "rules", Type: commands.TypeStatic, Template: "be nice", Access: commands.AccessEveryone,
	}, def)

	// Re-marshal must match the legacy bytes exactly (trigger_kind omitted).
	out, err := json.Marshal(s)
	require.NoError(t, err)
	assert.JSONEq(t, legacy, string(out))
	assert.NotContains(t, string(out), "trigger_kind", "default command kind must be omitted")
}

// TestLegacyArrayMatchesCommandsDefinition pins that an array of legacy command
// objects marshals identically whether modeled as a Skill or the historical
// commands.Definition.
func TestLegacyArrayMatchesCommandsDefinition(t *testing.T) {
	def := commands.Definition{Name: "ask", Type: commands.TypeLLM, Template: "{args}", Instructions: "be terse", Access: commands.AccessOwner, Allowlist: []string{"a"}}
	defBytes, err := json.Marshal(def)
	require.NoError(t, err)

	var s Skill
	require.NoError(t, json.Unmarshal(defBytes, &s))
	skillBytes, err := json.Marshal(s)
	require.NoError(t, err)

	assert.JSONEq(t, string(defBytes), string(skillBytes))
}

func TestUnmarshalFullWire(t *testing.T) {
	raw := `{
	  "name":"flood",
	  "type":"effect",
	  "trigger_kind":"match",
	  "match":".*",
	  "channels":["#ops"],
	  "effect":{"action":"kick","reason":"slow down","thresholds":{"warn":3,"kick":5}}
	}`
	var s Skill
	require.NoError(t, json.Unmarshal([]byte(raw), &s))

	assert.Equal(t, KindMatch, s.Trigger.Kind)
	assert.Equal(t, ".*", s.Trigger.Match)
	assert.Equal(t, []string{"#ops"}, s.Trigger.Channels)
	assert.False(t, s.IsCommand())
	require.NotNil(t, s.Action.Effect)
	assert.Equal(t, EffectKick, s.Action.Effect.Action)
	require.NotNil(t, s.Action.Effect.Thresholds)
	assert.Equal(t, 5, s.Action.Effect.Thresholds.Kick)
}

func TestMarshalEmitsTriggerKindWhenNonDefault(t *testing.T) {
	s := Skill{
		Name:    "greet",
		Trigger: Trigger{Kind: KindEvent, Event: EventUserJoin},
		Action:  Action{Type: TypeStatic, Template: "welcome {nick}"},
	}
	out, err := json.Marshal(s)
	require.NoError(t, err)
	assert.Contains(t, string(out), `"trigger_kind":"event"`)
	assert.Contains(t, string(out), `"event":"USER_JOIN"`)
}

func TestUnmarshalRejectsBadJSON(t *testing.T) {
	var s Skill
	require.Error(t, s.UnmarshalJSON([]byte(`{bad`)))
}

func TestCommandConstructor(t *testing.T) {
	s := Command("team", TypeStatic, "ok", AccessAllowlist, "alice", "bob")
	assert.True(t, s.IsCommand())
	assert.Equal(t, []string{"alice", "bob"}, s.Allowlist)
	assert.Equal(t, "ok", s.Action.Template)
}

func TestSplitPartitionsByKind(t *testing.T) {
	skills := []Skill{
		Command("rules", TypeStatic, "be nice", AccessEveryone),
		{Name: "greet", Trigger: Trigger{Kind: KindEvent, Event: EventUserJoin}, Action: Action{Type: TypeStatic, Template: "hi"}},
		{Name: "flood", Trigger: Trigger{Kind: KindMatch, Match: ".*"}, Action: Action{Type: TypeEffect}},
	}
	cmds, engine := Split(skills)
	require.Len(t, cmds, 1)
	assert.Equal(t, "rules", cmds[0].Name)
	require.Len(t, engine, 2)
	assert.Equal(t, "greet", engine[0].Name)
}
