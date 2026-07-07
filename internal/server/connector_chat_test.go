package server

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDiscordSettingsFromConnectorSpec(t *testing.T) {
	cs := ConnectorSpec{
		Type:    "discord",
		Config:  map[string]any{"guild_id": "g1", "channels": []any{"c1", "c2"}, "suspended": true},
		Secrets: map[string]any{"bot_token": "tok"},
	}
	s, err := discordSettingsFromConnectorSpec(cs)
	require.NoError(t, err)
	assert.Equal(t, "tok", s.Token)
	assert.Equal(t, "g1", s.GuildID)
	assert.Equal(t, []string{"c1", "c2"}, s.Channels)
	assert.True(t, s.Suspended)
}

func TestDiscordSettingsRejectsInvalid(t *testing.T) {
	// No bot_token / guild_id → Validate fails and the mapper returns an error.
	_, err := discordSettingsFromConnectorSpec(ConnectorSpec{Type: "discord", Config: map[string]any{}, Secrets: map[string]any{}})
	require.Error(t, err)
}

func TestTelegramSettingsFromConnectorSpec(t *testing.T) {
	cs := ConnectorSpec{
		Type:    "telegram",
		Config:  map[string]any{"chats": []any{"100", "200"}, "suspended": true},
		Secrets: map[string]any{"bot_token": "tok"},
	}
	s, err := telegramSettingsFromConnectorSpec(cs)
	require.NoError(t, err)
	assert.Equal(t, "tok", s.Token)
	assert.Equal(t, []string{"100", "200"}, s.Chats)
	assert.True(t, s.Suspended)
}

func TestTelegramSettingsRejectsMissingToken(t *testing.T) {
	_, err := telegramSettingsFromConnectorSpec(ConnectorSpec{Type: "telegram", Config: map[string]any{}, Secrets: map[string]any{}})
	require.Error(t, err)
}

func TestSlackSettingsFromConnectorSpec(t *testing.T) {
	cs := ConnectorSpec{
		Type:    "slack",
		Config:  map[string]any{"channels": []any{"C1"}, "suspended": false},
		Secrets: map[string]any{"bot_token": "xoxb-1", "app_token": "xapp-1"},
	}
	s, err := slackSettingsFromConnectorSpec(cs)
	require.NoError(t, err)
	assert.Equal(t, "xoxb-1", s.BotToken)
	assert.Equal(t, "xapp-1", s.AppToken)
	assert.Equal(t, []string{"C1"}, s.Channels)
	assert.False(t, s.Suspended)
}

func TestSlackSettingsRejectsMissingTokens(t *testing.T) {
	// Bot token present but the app token (Socket Mode) missing → invalid.
	_, err := slackSettingsFromConnectorSpec(ConnectorSpec{
		Type:    "slack",
		Config:  map[string]any{},
		Secrets: map[string]any{"bot_token": "xoxb-1"},
	})
	require.Error(t, err)
}
