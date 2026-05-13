package agent_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/agent"
)

func TestNewInbound(t *testing.T) {
	env := agent.NewInbound("irc", "#test", "alice", "hello")

	assert.Equal(t, "irc", env.Connector)
	assert.Equal(t, "default", env.ConnectorInstance)
	assert.Equal(t, "#test", env.Channel)
	assert.Equal(t, "alice", env.Sender)
	assert.Equal(t, "hello", env.Text)
	assert.False(t, env.IsDirect)
	assert.NotZero(t, env.ID)
	assert.NotZero(t, env.ReceivedAt)
	require.NotNil(t, env.Metadata)
	assert.Empty(t, env.Metadata)
}

func TestReplyToChannelMessage(t *testing.T) {
	in := agent.NewInbound("irc", "#test", "alice", "!ping")
	out := agent.ReplyTo(in, "pong")

	assert.Equal(t, in.Connector, out.Connector)
	assert.Equal(t, in.ConnectorInstance, out.ConnectorInstance)
	assert.Equal(t, "#test", out.Channel, "channel reply must target the channel, not the sender")
	assert.Equal(t, "pong", out.Text)
	require.NotNil(t, out.ReplyTo)
	assert.Equal(t, in.ID, *out.ReplyTo)
	assert.NotEqual(t, in.ID, out.ID, "outbound must have its own ID")
}

func TestReplyToDirectMessage(t *testing.T) {
	in := agent.NewInbound("irc", "turborg", "alice", "ping")
	in.IsDirect = true

	out := agent.ReplyTo(in, "pong")

	assert.Equal(t, "alice", out.Channel,
		"DM reply must route back to the sender, not the original target")
}
