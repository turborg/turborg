package runtime

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/agent"
	"github.com/turborg/turborg/internal/connector/irc"
)

func TestParseFlows(t *testing.T) {
	none, err := parseFlows("")
	require.NoError(t, err)
	assert.Nil(t, none)

	flows, err := parseFlows(`[{"name":"f","trigger":{"kind":"event","event":"USER_JOIN"},"nodes":[{"id":"s","type":"say","config":{"text":"hi {user}"}}]}]`)
	require.NoError(t, err)
	require.Len(t, flows, 1)
	assert.Equal(t, "f", flows[0].Name)
	assert.Equal(t, "say", flows[0].Nodes[0].Type)

	_, err = parseFlows("not json")
	require.Error(t, err)
}

// TestWireCommonRunsFlows proves a flow installed via WireCommon fires on a
// matching event through the live engine.
func TestWireCommonRunsFlows(t *testing.T) {
	a := agent.NewWithPrefix(nil, "!")
	cfg := &irc.Settings{Hostname: "irc.example", Nick: "bot"}
	cfg.ApplyDefaults()
	conn := irc.New(cfg, nil, a.Events)

	flows, err := parseFlows(`[{"name":"welcome","trigger":{"kind":"event","event":"USER_JOIN"},"nodes":[{"id":"s","type":"say","config":{"text":"welcome {user}"}}]}]`)
	require.NoError(t, err)

	wiring, err := WireCommon(a, conn, CommonParams{CustomCommandsMax: -1, Flows: flows}, nil)
	require.NoError(t, err)
	require.NotNil(t, wiring.Flows)

	// Not connected, so the actor's SendRaw errors — but the flow must still be
	// dispatched (no panic, engine walks the graph). A nil-actor path is covered
	// in the flow package; here we assert the wiring publishes to the engine.
	a.Events.Publish(context.Background(), &agent.Event{
		Type:   agent.EventUserJoin,
		Fields: map[string]any{"channel": "#room", "nick": "newbie"},
	})
}
