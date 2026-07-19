package telegram

import (
	"context"
	"errors"
	"testing"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/agent"
)

func TestConnectorStateInitial(t *testing.T) {
	c := New(&Settings{Token: "t"}, nil, agent.NewEventBus(nil))
	assert.Equal(t, "connecting", c.ConnectorState().State)

	s := New(&Settings{Token: "t", Suspended: true}, nil, agent.NewEventBus(nil))
	assert.Equal(t, "suspended", s.ConnectorState().State)
}

func TestConnectorStateStartSuspended(t *testing.T) {
	c := New(&Settings{Token: "t", Suspended: true}, nil, agent.NewEventBus(nil))
	require.NoError(t, c.Start(context.Background()))
	assert.Equal(t, "suspended", c.ConnectorState().State)
}

func TestConnectorStateStartSuccess(t *testing.T) {
	c, _ := newTestConn(t, &Settings{Token: "t"}) // pre-wired api → success path
	require.NoError(t, c.Start(context.Background()))
	assert.Equal(t, "connected", c.ConnectorState().State)
}

func TestConnectorStateStartError(t *testing.T) {
	old := newBotAPI
	newBotAPI = func(string) (*tgbotapi.BotAPI, error) { return nil, errors.New("bad token") }
	t.Cleanup(func() { newBotAPI = old })

	c := New(&Settings{Token: "t"}, nil, agent.NewEventBus(nil))
	t.Cleanup(func() { _ = c.Stop(context.Background()) })
	require.Error(t, c.Start(context.Background()))
	st := c.ConnectorState()
	assert.Equal(t, "error", st.State)
	assert.Equal(t, "bad token", st.Reason)
}

func TestConnectorStateSuspendResume(t *testing.T) {
	c, _ := newTestConn(t, &Settings{Token: "t"})
	c.Suspend()
	assert.Equal(t, "suspended", c.ConnectorState().State)
	c.Resume()
	assert.Equal(t, "connected", c.ConnectorState().State)
}

func TestConnectorStateNotifiesOnChange(t *testing.T) {
	c, _ := newTestConn(t, &Settings{Token: "t"})
	fired := 0
	c.OnStateChange(func() { fired++ })

	c.Suspend() // connecting → suspended (fires)
	c.Suspend() // no change (no fire)
	c.Resume()  // suspended → connected (fires)
	assert.Equal(t, 2, fired, "hook fires only on an actual transition")
}
