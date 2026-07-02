package runtime

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/turborg/turborg/internal/agent"
	"github.com/turborg/turborg/internal/connector/irc"
	"github.com/turborg/turborg/internal/flow"
	"github.com/turborg/turborg/internal/skill"
)

// TestWiringFireWebhook proves the combined dispatcher reaches both engines: a
// webhook-trigger flow and a webhook-trigger skill installed via WireCommon are
// each reachable by name, and an unknown name (or a nil wiring) returns false.
func TestWiringFireWebhook(t *testing.T) {
	a := agent.NewWithPrefix(nil, "!")
	cfg := &irc.Settings{Hostname: "irc.example", Nick: "bot"}
	cfg.ApplyDefaults()
	conn := irc.New(cfg, nil, a.Events)

	cmds := []skill.Skill{{
		Name:    "skillhook",
		Trigger: skill.Trigger{Kind: skill.KindWebhook, Channels: []string{"#c"}},
		Action:  skill.Action{Type: skill.TypeStatic, Template: "{text}"},
	}}
	flows := []flow.Flow{{
		Name:    "flowhook",
		Trigger: skill.Trigger{Kind: skill.KindWebhook, Channels: []string{"#c"}},
		Nodes:   []flow.Node{{ID: "s", Type: "say", Config: map[string]any{"channel": "#c", "text": "{text}"}}},
	}}

	wiring, err := WireCommon(a, conn, CommonParams{CustomCommandsMax: -1, Commands: cmds, Flows: flows}, nil)
	if err != nil {
		t.Fatalf("WireCommon: %v", err)
	}

	// The actor errors (not connected), but a found trigger still fires and
	// FireWebhook reports true. Both engines are reachable through one call.
	assert.True(t, wiring.FireWebhook("flowhook", map[string]string{"text": "hi"}), "flow reachable")
	assert.True(t, wiring.FireWebhook("skillhook", map[string]string{"text": "hi"}), "skill reachable")
	assert.False(t, wiring.FireWebhook("ghost", map[string]string{"text": "hi"}), "unknown name is false")

	var nilWiring *Wiring
	assert.False(t, nilWiring.FireWebhook("x", nil), "nil wiring is false, not a panic")
}
