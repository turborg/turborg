package telegram

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadSettingsParsesEnv(t *testing.T) {
	t.Setenv("TURBORG_TELEGRAM_TOKEN", "tok")
	// Comma-separated with whitespace, quotes, and an empty entry.
	t.Setenv("TURBORG_TELEGRAM_CHATS", ` 100 , 200 ,, '300' `)
	t.Setenv("TURBORG_TELEGRAM_SUSPENDED", "1")

	s, err := LoadSettings()
	require.NoError(t, err)
	assert.Equal(t, "tok", s.Token)
	assert.Equal(t, []string{"100", "200", "300"}, s.Chats)
	assert.True(t, s.Suspended)
}

func TestLoadSettingsDefaults(t *testing.T) {
	t.Setenv("TURBORG_TELEGRAM_TOKEN", "tok")
	t.Setenv("TURBORG_TELEGRAM_CHATS", "")
	t.Setenv("TURBORG_TELEGRAM_SUSPENDED", "")

	s, err := LoadSettings()
	require.NoError(t, err)
	assert.Empty(t, s.Chats)
	assert.False(t, s.Suspended)
}
