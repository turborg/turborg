package irc_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/connector/irc"
)

func TestLoadSettingsFromEnv(t *testing.T) {
	t.Setenv("TURBORG_IRC_HOSTNAME", "irc.libera.chat")
	t.Setenv("TURBORG_IRC_NICK", "turborg")
	t.Setenv("TURBORG_IRC_CHANNELS", "#a,#b, no-hash")
	t.Setenv("TURBORG_IRC_SASL_USER", "alice")
	t.Setenv("TURBORG_IRC_SASL_PASSWORD", "secret")
	t.Setenv("TURBORG_IRC_BOUNCER_PASSWORD", "hunter2")
	t.Setenv("TURBORG_IRC_USE_TLS", "false")
	t.Setenv("TURBORG_IRC_PORT", "6667")
	t.Setenv("TURBORG_IRC_READ_IDLE_TIMEOUT", "60s")
	t.Setenv("TURBORG_IRC_CLIENT_PING_INTERVAL", "30s")

	s, err := irc.LoadSettings()
	require.NoError(t, err)

	assert.Equal(t, "irc.libera.chat", s.Hostname)
	assert.Equal(t, "turborg", s.Nick)
	assert.Equal(t, 6667, s.Port)
	assert.False(t, s.UseTLS)
	assert.Equal(t, []string{"#a", "#b", "#no-hash"}, s.NormalizedChannels())
	assert.True(t, s.SASLEnabled())
	assert.True(t, s.BouncerEnabled())
	assert.Equal(t, "turborg", s.EffectiveUsername())
	assert.Equal(t, 60*time.Second, s.ReadIdleTimeout)
	assert.Equal(t, 30*time.Second, s.ClientPingInterval)
}

func TestLoadSettingsDefaults(t *testing.T) {
	t.Setenv("TURBORG_IRC_HOSTNAME", "irc.libera.chat")
	t.Setenv("TURBORG_IRC_NICK", "turborg")

	s, err := irc.LoadSettings()
	require.NoError(t, err)

	assert.Equal(t, 6697, s.Port)
	assert.True(t, s.UseTLS)
	assert.Equal(t, "turborg agent", s.RealName)
	assert.False(t, s.SASLEnabled())
	assert.False(t, s.BouncerEnabled())
	assert.Equal(t, 300*time.Second, s.ReadIdleTimeout)
	assert.Equal(t, 120*time.Second, s.ClientPingInterval)
	assert.True(t, s.CTCPAutoReply)
	assert.Equal(t, 3, s.CTCPMaxPerWindow)
}

func TestLoadSettingsRejectsBadInt(t *testing.T) {
	t.Setenv("TURBORG_IRC_HOSTNAME", "irc.libera.chat")
	t.Setenv("TURBORG_IRC_NICK", "turborg")
	t.Setenv("TURBORG_IRC_PORT", "not-a-port")
	_, err := irc.LoadSettings()
	require.Error(t, err, "non-integer port must produce a parse error")
}

func TestEffectiveUsernameOverride(t *testing.T) {
	s := &irc.Settings{Nick: "turborg", Username: "ident"}
	assert.Equal(t, "ident", s.EffectiveUsername())
}

func TestNormalizedChannelsHandlesPrefixes(t *testing.T) {
	s := &irc.Settings{Channels: []string{"#a", "&local", "+plus", "!safe", "bare", "  spaced  "}}
	got := s.NormalizedChannels()
	assert.Equal(t, []string{"#a", "&local", "+plus", "!safe", "#bare", "#spaced"}, got)
}

func TestNormalizedChannelsSkipsEmpty(t *testing.T) {
	s := &irc.Settings{Channels: []string{"", "#a", " ", "#b"}}
	assert.Equal(t, []string{"#a", "#b"}, s.NormalizedChannels())
}

func TestValidateRejectsBadPingInterval(t *testing.T) {
	s := &irc.Settings{ClientPingInterval: 300 * time.Second, ReadIdleTimeout: 300 * time.Second}
	require.Error(t, s.Validate())
}

func TestValidateAllowsZeroSentinels(t *testing.T) {
	s := &irc.Settings{ClientPingInterval: 0, ReadIdleTimeout: 0}
	assert.NoError(t, s.Validate())
}

func TestValidateAcceptsHealthyConfig(t *testing.T) {
	s := &irc.Settings{ClientPingInterval: 120 * time.Second, ReadIdleTimeout: 300 * time.Second}
	assert.NoError(t, s.Validate())
}

func TestLoadSettingsWebDefaults(t *testing.T) {
	t.Setenv("TURBORG_IRC_HOSTNAME", "irc.libera.chat")
	t.Setenv("TURBORG_IRC_NICK", "turborg")

	s, err := irc.LoadSettings()
	require.NoError(t, err)
	assert.Equal(t, "127.0.0.1", s.WebHost)
	assert.Equal(t, 8765, s.WebPort)
	assert.False(t, s.WebEnabled())
	assert.False(t, s.IdleShutdownEnabled())
}

func TestLoadSettingsRespectsWebToggle(t *testing.T) {
	t.Setenv("TURBORG_IRC_HOSTNAME", "irc.libera.chat")
	t.Setenv("TURBORG_IRC_NICK", "turborg")
	t.Setenv("TURBORG_IRC_WEB_PASSWORD", "hunter2")
	t.Setenv("TURBORG_IRC_WEB_PORT", "9000")
	t.Setenv("TURBORG_IRC_WEB_IDLE_SHUTDOWN_SECONDS", "30")

	s, err := irc.LoadSettings()
	require.NoError(t, err)
	assert.True(t, s.WebEnabled())
	assert.True(t, s.IdleShutdownEnabled())
	assert.Equal(t, 9000, s.WebPort)
	assert.Equal(t, 30, s.WebIdleShutdownSeconds)
}

func TestLoadSettingsRejectsMalformedWebPort(t *testing.T) {
	t.Setenv("TURBORG_IRC_HOSTNAME", "irc.libera.chat")
	t.Setenv("TURBORG_IRC_NICK", "turborg")
	t.Setenv("TURBORG_IRC_WEB_PORT", "not-a-number")
	_, err := irc.LoadSettings()
	require.Error(t, err)
}
