package discord

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/agent"
)

// failOpenSession is a session whose Open always errors, to drive the connect
// failure → "error" transition.
type failOpenSession struct{ fakeSession }

func (f *failOpenSession) Open() error { return errors.New("gateway refused") }

func TestConnectorStateInitial(t *testing.T) {
	c := New(&Settings{Token: "t", GuildID: "g"}, nil, agent.NewEventBus(nil))
	assert.Equal(t, "connecting", c.ConnectorState().State)

	s := New(&Settings{Token: "t", GuildID: "g", Suspended: true}, nil, agent.NewEventBus(nil))
	assert.Equal(t, "suspended", s.ConnectorState().State)
}

func TestConnectorStateConnectSuccess(t *testing.T) {
	c, _ := newTestConn(t, &Settings{GuildID: "g"}) // pre-injected session
	require.NoError(t, c.Start(context.Background()))
	assert.Equal(t, "connected", c.ConnectorState().State)
}

func TestConnectorStateConnectError(t *testing.T) {
	c := New(&Settings{GuildID: "g"}, nil, agent.NewEventBus(nil))
	c.session = &failOpenSession{}
	t.Cleanup(func() { _ = c.Stop(context.Background()) })

	require.Error(t, c.Start(context.Background()))
	st := c.ConnectorState()
	assert.Equal(t, "error", st.State)
	assert.Equal(t, "gateway refused", st.Reason)
}

func TestConnectorStateSuspendResume(t *testing.T) {
	c, _ := newTestConn(t, &Settings{GuildID: "g"})
	c.Suspend()
	assert.Equal(t, "suspended", c.ConnectorState().State)
	c.Resume()
	assert.Equal(t, "connecting", c.ConnectorState().State)
}

func TestConnectorStateNotifiesOnChange(t *testing.T) {
	c, _ := newTestConn(t, &Settings{GuildID: "g"})
	fired := 0
	c.OnStateChange(func() { fired++ })

	c.Suspend() // connecting → suspended (fires)
	c.Resume()  // suspended → connecting (fires)
	assert.Equal(t, 2, fired, "hook fires only on an actual transition")
}
