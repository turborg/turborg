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
	t.Setenv("TURBORG_IRC_QUIT_MESSAGE", "see ya")

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
	assert.Equal(t, 30*time.Second, s.PongTimeout,
		"PongTimeout defaults to 30s when no env override is supplied")
	assert.Equal(t, "see ya", s.QuitMessage)
	assert.Equal(t, "see ya", s.EffectiveQuitMessage())
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
	assert.Equal(t, "bye from turborg", s.QuitMessage,
		"default QUIT body is project-attributed for self-hosted users")
	assert.Equal(t, "bye from turborg", s.EffectiveQuitMessage())
}

func TestEffectiveQuitMessageFallsBackWhenEmpty(t *testing.T) {
	// Tests that build Settings by hand (bypassing LoadSettings + envDefault)
	// still get a sensible fallback rather than an empty trailing parameter.
	s := &irc.Settings{}
	assert.Equal(t, "bye from turborg", s.EffectiveQuitMessage())
}

func TestEffectiveQuitMessageOverride(t *testing.T) {
	s := &irc.Settings{QuitMessage: "turborg @ www.xshellz.com"}
	assert.Equal(t, "turborg @ www.xshellz.com", s.EffectiveQuitMessage())
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
	s := &irc.Settings{
		ClientPingInterval: 120 * time.Second,
		ReadIdleTimeout:    300 * time.Second,
		PongTimeout:        30 * time.Second,
	}
	assert.NoError(t, s.Validate())
}

func TestValidateRejectsPongTimeoutGEPingInterval(t *testing.T) {
	// A pong timeout >= the ping cadence lets the next tick race past
	// an already-stale outstanding token. Operators should narrow
	// PongTimeout below ClientPingInterval (default 30s vs 120s).
	s := &irc.Settings{
		ClientPingInterval: 30 * time.Second,
		ReadIdleTimeout:    300 * time.Second,
		PongTimeout:        30 * time.Second,
	}
	err := s.Validate()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pong_timeout")
}

func TestValidateAllowsPongTimeoutZero(t *testing.T) {
	// Zero disables the active probe; ReadIdleTimeout is still the
	// backstop. Operators who want the old detection envelope use this.
	s := &irc.Settings{
		ClientPingInterval: 120 * time.Second,
		ReadIdleTimeout:    300 * time.Second,
		PongTimeout:        0,
	}
	assert.NoError(t, s.Validate())
}

func TestValidateAllowsPongTimeoutWithoutPingInterval(t *testing.T) {
	// PongTimeout > 0 with ClientPingInterval == 0 is functionally
	// inert (no PINGs being sent → nothing to time out) but must not
	// trip validation. The connector handles the inert case at
	// goroutine-start time.
	s := &irc.Settings{
		ClientPingInterval: 0,
		ReadIdleTimeout:    300 * time.Second,
		PongTimeout:        30 * time.Second,
	}
	assert.NoError(t, s.Validate())
}
