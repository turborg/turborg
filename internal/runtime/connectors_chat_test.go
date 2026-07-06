package runtime

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/agent"
	"github.com/turborg/turborg/internal/config"
)

func TestSinglePrimaryChatConnector(t *testing.T) {
	name, ok := singlePrimaryChatConnector([]string{"discord"})
	assert.True(t, ok)
	assert.Equal(t, "discord", name)

	name, ok = singlePrimaryChatConnector([]string{"telegram"})
	assert.True(t, ok)
	assert.Equal(t, "telegram", name)

	_, ok = singlePrimaryChatConnector([]string{"irc"})
	assert.False(t, ok, "IRC stays on the IRC-primary path")

	_, ok = singlePrimaryChatConnector([]string{"discord", "irc"})
	assert.False(t, ok, "a mix with IRC is not a dedicated chat container")

	_, ok = singlePrimaryChatConnector(nil)
	assert.False(t, ok)
}

func TestRequiresIRCSettings(t *testing.T) {
	assert.False(t, RequiresIRCSettings(&config.Settings{Connectors: []string{"slack"}}),
		"a dedicated chat container carries no IRC config")
	assert.True(t, RequiresIRCSettings(&config.Settings{Connectors: []string{"irc"}}))
	assert.True(t, RequiresIRCSettings(&config.Settings{}), "the quickstart default is IRC")
}

func TestBuildChatConnectorEachPlatform(t *testing.T) {
	a := agent.NewWithPrefix(nil, "!")

	t.Run("discord", func(t *testing.T) {
		t.Setenv("TURBORG_DISCORD_TOKEN", "tok")
		t.Setenv("TURBORG_DISCORD_GUILD_ID", "g1")
		conn, actor, nick, platform, err := buildChatConnector("discord", nil, a)
		require.NoError(t, err)
		assert.NotNil(t, conn)
		assert.NotNil(t, actor)
		assert.Equal(t, "Discord", platform)
		assert.NotEmpty(t, nick())
	})

	t.Run("telegram", func(t *testing.T) {
		t.Setenv("TURBORG_TELEGRAM_TOKEN", "tok")
		conn, actor, _, platform, err := buildChatConnector("telegram", nil, a)
		require.NoError(t, err)
		assert.NotNil(t, conn)
		assert.NotNil(t, actor)
		assert.Equal(t, "Telegram", platform)
	})

	t.Run("slack", func(t *testing.T) {
		t.Setenv("TURBORG_SLACK_BOT_TOKEN", "xoxb-1")
		t.Setenv("TURBORG_SLACK_APP_TOKEN", "xapp-1")
		conn, actor, _, platform, err := buildChatConnector("slack", nil, a)
		require.NoError(t, err)
		assert.NotNil(t, conn)
		assert.NotNil(t, actor)
		assert.Equal(t, "Slack", platform)
	})
}

func TestBuildChatConnectorInvalidSettings(t *testing.T) {
	a := agent.NewWithPrefix(nil, "!")
	t.Setenv("TURBORG_DISCORD_TOKEN", "")
	t.Setenv("TURBORG_DISCORD_GUILD_ID", "")
	_, _, _, _, err := buildChatConnector("discord", nil, a)
	require.Error(t, err, "missing required settings fail validation")
}

func TestBuildChatConnectorUnsupported(t *testing.T) {
	a := agent.NewWithPrefix(nil, "!")
	_, _, _, _, err := buildChatConnector("irc", nil, a)
	require.Error(t, err, "IRC is not a chat-platform connector")
}
