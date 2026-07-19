package slack

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/agent"
)

func TestConnectorStateInitial(t *testing.T) {
	c := New(&Settings{}, nil, agent.NewEventBus(nil))
	assert.Equal(t, "connecting", c.ConnectorState().State)

	s := New(&Settings{Suspended: true}, nil, agent.NewEventBus(nil))
	assert.Equal(t, "suspended", s.ConnectorState().State)
}

func TestConnectorStateStartSuspended(t *testing.T) {
	c := New(&Settings{Suspended: true}, nil, agent.NewEventBus(nil))
	require.NoError(t, c.Start(context.Background()))
	assert.Equal(t, "suspended", c.ConnectorState().State)
}

func TestConnectorStateStartSuccess(t *testing.T) {
	c, _ := newTestConn(t, &Settings{}) // pre-wired api → success path
	require.NoError(t, c.Start(context.Background()))
	assert.Equal(t, "connected", c.ConnectorState().State)
}

func TestConnectorStateSuspendResume(t *testing.T) {
	c, _ := newTestConn(t, &Settings{})
	c.Suspend()
	assert.Equal(t, "suspended", c.ConnectorState().State)
	c.Resume()
	assert.Equal(t, "connected", c.ConnectorState().State)
}

func TestConnectorStateNotifiesOnChange(t *testing.T) {
	c, _ := newTestConn(t, &Settings{})
	fired := 0
	c.OnStateChange(func() { fired++ })

	c.Suspend() // connecting → suspended (fires)
	c.Suspend() // no change (no fire)
	c.Resume()  // suspended → connected (fires)
	assert.Equal(t, 2, fired, "hook fires only on an actual transition")
}
