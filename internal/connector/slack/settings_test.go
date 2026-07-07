package slack

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadSettingsParsesEnv(t *testing.T) {
	t.Setenv("TURBORG_SLACK_BOT_TOKEN", "xoxb-1")
	t.Setenv("TURBORG_SLACK_APP_TOKEN", "xapp-1")
	t.Setenv("TURBORG_SLACK_CHANNELS", ` "C1" , C2 ,, 'C3' `)
	t.Setenv("TURBORG_SLACK_SUSPENDED", "true")

	s, err := LoadSettings()
	require.NoError(t, err)
	assert.Equal(t, "xoxb-1", s.BotToken)
	assert.Equal(t, "xapp-1", s.AppToken)
	assert.Equal(t, []string{"C1", "C2", "C3"}, s.Channels)
	assert.True(t, s.Suspended)
}

func TestLoadSettingsDefaults(t *testing.T) {
	t.Setenv("TURBORG_SLACK_BOT_TOKEN", "xoxb-1")
	t.Setenv("TURBORG_SLACK_APP_TOKEN", "xapp-1")
	t.Setenv("TURBORG_SLACK_CHANNELS", "")
	t.Setenv("TURBORG_SLACK_SUSPENDED", "")

	s, err := LoadSettings()
	require.NoError(t, err)
	assert.Empty(t, s.Channels)
	assert.False(t, s.Suspended)
}
