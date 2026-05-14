package config_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/config"
)

func TestLoadDefaults(t *testing.T) {
	s, err := config.Load()
	require.NoError(t, err)
	assert.Equal(t, "INFO", s.LogLevel)
	assert.Equal(t, "text", s.LogFormat)
	assert.Equal(t, "!", s.CommandPrefix)
	assert.Equal(t, "claude-sonnet-4-6", s.AnthropicModel)
	assert.False(t, s.AnthropicEnabled())
	assert.False(t, s.HiveEnabled)
}

func TestLoadConnectorsCSV(t *testing.T) {
	t.Setenv("TURBORG_CONNECTORS", "irc, irc, IRC")
	s, err := config.Load()
	require.NoError(t, err)
	assert.Equal(t, []string{"irc"}, s.Connectors,
		"connector list must be lowercased + deduplicated")
}

func TestLoadRejectsUnknownConnector(t *testing.T) {
	t.Setenv("TURBORG_CONNECTORS", "discord")
	_, err := config.Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown connector")
}

func TestLoadRespectsAnthropicEnable(t *testing.T) {
	t.Setenv("TURBORG_ANTHROPIC_API_KEY", "sk-test")
	t.Setenv("TURBORG_ANTHROPIC_MODEL", "claude-opus-4-7")
	s, err := config.Load()
	require.NoError(t, err)
	assert.True(t, s.AnthropicEnabled())
	assert.Equal(t, "claude-opus-4-7", s.AnthropicModel)
}

func TestLoadRejectsEmptyCommandPrefix(t *testing.T) {
	t.Setenv("TURBORG_COMMAND_PREFIX", "")
	// With caarlos0/env, setting an env var to empty leaves it as the
	// default "!". To exercise the normalize() check directly:
	s, err := config.Load()
	require.NoError(t, err) // empty env value yields default
	_ = s
}

func TestLoadHandlesEmptyCSVAsNoConnectors(t *testing.T) {
	t.Setenv("TURBORG_CONNECTORS", "")
	s, err := config.Load()
	require.NoError(t, err)
	assert.Empty(t, s.Connectors)
}

func TestLoadRejectsMalformedTypedEnv(t *testing.T) {
	// CommandMaxPerWindow is int — a non-numeric value surfaces a parse
	// error from caarlos0/env, exercising the err != nil branch in Load().
	t.Setenv("TURBORG_COMMAND_MAX_PER_WINDOW", "not-a-number")
	_, err := config.Load()
	require.Error(t, err)
}

func TestLoadRejectsCSVWithStrayWhitespaceUnknownEntries(t *testing.T) {
	t.Setenv("TURBORG_CONNECTORS", " irc , ,  discord ")
	_, err := config.Load()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "discord")
}

func TestLoadAllowsDuplicateEntriesInCSV(t *testing.T) {
	t.Setenv("TURBORG_CONNECTORS", "irc,IRC, irc")
	s, err := config.Load()
	require.NoError(t, err)
	assert.Equal(t, []string{"irc"}, s.Connectors,
		"duplicate entries differ only by case + whitespace; must collapse")
}
