package flow

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/skill"
)

func TestFlowJSONRoundTrip(t *testing.T) {
	raw := `{
	  "name":"welcome-flow",
	  "trigger":{"kind":"event","event":"USER_JOIN","channels":["#ops"]},
	  "nodes":[
	    {"id":"a","type":"if","config":{"left":"{user}","op":"ne","right":"bot"}},
	    {"id":"b","type":"say","config":{"text":"hi {user}"}}
	  ],
	  "edges":[{"from":"start","to":"a"},{"from":"a","to":"b","port":"true"}]
	}`
	var f Flow
	require.NoError(t, json.Unmarshal([]byte(raw), &f))
	assert.Equal(t, "welcome-flow", f.Name)
	assert.Equal(t, skill.KindEvent, f.Trigger.Kind)
	assert.Equal(t, "USER_JOIN", f.Trigger.Event)
	require.Len(t, f.Nodes, 2)
	require.Len(t, f.Edges, 2)

	out, err := json.Marshal(f)
	require.NoError(t, err)
	var again Flow
	require.NoError(t, json.Unmarshal(out, &again))
	assert.Equal(t, f, again)
}

func TestTriggerKindDefault(t *testing.T) {
	assert.Equal(t, skill.KindCommand, Flow{}.triggerKind())
	assert.Equal(t, skill.KindMatch, Flow{Trigger: skill.Trigger{Kind: skill.KindMatch}}.triggerKind())
}

func TestEntryNodes(t *testing.T) {
	f := Flow{
		Nodes: []Node{{ID: "a"}, {ID: "b"}, {ID: "c"}},
		Edges: []Edge{{From: "a", To: "b"}, {From: "b", To: "c"}},
	}
	// a has no incoming → entry.
	assert.Equal(t, []string{"a"}, f.entryNodes())

	// Explicit start edge seeds an entry even if the node has incoming edges.
	f2 := Flow{
		Nodes: []Node{{ID: "a"}, {ID: "b"}},
		Edges: []Edge{{From: "start", To: "a"}, {From: "b", To: "a"}, {From: "a", To: "b"}},
	}
	assert.Equal(t, []string{"a"}, f2.entryNodes())
}

func TestSuccessors(t *testing.T) {
	f := Flow{Edges: []Edge{
		{From: "a", To: "b"},
		{From: "a", To: "c", Port: "true"},
		{From: "a", To: "d", Port: "true"},
	}}
	assert.Equal(t, []string{"b"}, f.successors("a", ""))
	assert.Equal(t, []string{"c", "d"}, f.successors("a", "true"))
	assert.Nil(t, f.successors("a", "false"))
}

func TestIsEntryFrom(t *testing.T) {
	assert.True(t, isEntryFrom(""))
	assert.True(t, isEntryFrom("start"))
	assert.True(t, isEntryFrom("trigger"))
	assert.False(t, isEntryFrom("a"))
}
