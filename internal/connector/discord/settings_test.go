package discord

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadSettingsParsesEnv(t *testing.T) {
	t.Setenv("TURBORG_DISCORD_TOKEN", "tok")
	t.Setenv("TURBORG_DISCORD_GUILD_ID", "g1")
	// Comma-separated with surrounding whitespace, quotes, and an empty entry —
	// cleanList must trim and drop the blank.
	t.Setenv("TURBORG_DISCORD_CHANNELS", ` "c1" , c2 ,, 'c3' `)
	t.Setenv("TURBORG_DISCORD_SUSPENDED", "true")

	s, err := LoadSettings()
	require.NoError(t, err)
	assert.Equal(t, "tok", s.Token)
	assert.Equal(t, "g1", s.GuildID)
	assert.Equal(t, []string{"c1", "c2", "c3"}, s.Channels)
	assert.True(t, s.Suspended)
}

func TestLoadSettingsDefaults(t *testing.T) {
	// Only the required fields; the optional list + bool are absent.
	t.Setenv("TURBORG_DISCORD_TOKEN", "tok")
	t.Setenv("TURBORG_DISCORD_GUILD_ID", "g1")
	t.Setenv("TURBORG_DISCORD_CHANNELS", "")
	t.Setenv("TURBORG_DISCORD_SUSPENDED", "")

	s, err := LoadSettings()
	require.NoError(t, err)
	assert.Empty(t, s.Channels, "an unset allow-list yields no channels (respond everywhere)")
	assert.False(t, s.Suspended)
}
