package server

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func ircConfig(overrides map[string]any) ConnectorSpec {
	cfg := map[string]any{
		"network":  "irc.libera.chat:6697",
		"nick":     "bot",
		"channels": []any{"#a", "#b"},
	}
	for k, v := range overrides {
		cfg[k] = v
	}
	return ConnectorSpec{Type: "irc", Config: cfg, Secrets: map[string]any{}}
}

func TestSettingsFromConnectorSpec(t *testing.T) {
	cs := ircConfig(map[string]any{
		"use_tls":   false,
		"username":  "ident",
		"real_name": "Real Name",
		"auth_mode": "sasl",
	})
	cs.Secrets = map[string]any{"sasl_user": "u", "sasl_password": "p"}

	s, err := settingsFromConnectorSpec(cs)
	require.NoError(t, err)
	require.Equal(t, "irc.libera.chat", s.Hostname)
	require.Equal(t, 6697, s.Port)
	require.False(t, s.UseTLS)
	require.Equal(t, "bot", s.Nick)
	require.Equal(t, "ident", s.Username)
	require.Equal(t, "Real Name", s.RealName)
	require.Equal(t, "sasl", s.AuthMode)
	require.Equal(t, []string{"#a", "#b"}, s.Channels)
	require.Equal(t, "u", s.SASLUser)
	require.Equal(t, "p", s.SASLPassword)
}

func TestSettingsRealNameDefaults(t *testing.T) {
	s, err := settingsFromConnectorSpec(ircConfig(nil))
	require.NoError(t, err)
	require.Equal(t, "turborg agent", s.RealName)
	require.True(t, s.UseTLS, "use_tls defaults true when absent")
}

// TestSettingsAppliesOperationalDefaults: a spec-built Settings gets the same
// env-defaulted operational fields the dedicated path does — chiefly CTCP
// auto-reply (the pooled `/ctcp <bot> version` drift), plus liveness probing
// and reconnect escalation. UseTLS=false above proves ApplyDefaults doesn't
// clobber spec-set fields.
func TestSettingsAppliesOperationalDefaults(t *testing.T) {
	s, err := settingsFromConnectorSpec(ircConfig(nil))
	require.NoError(t, err)
	require.True(t, s.CTCPAutoReply, "CTCP auto-reply must default on for pooled too")
	require.Equal(t, 3, s.CTCPMaxPerWindow)
	require.Equal(t, 30, s.CTCPWindowSeconds)
	require.Equal(t, 20*time.Second, s.DialTimeout)
	require.Equal(t, 300*time.Second, s.ReadIdleTimeout)
	require.Equal(t, 120*time.Second, s.ClientPingInterval)
	require.Equal(t, 30*time.Second, s.PongTimeout)
	require.Equal(t, 200, s.BouncerWelcomeReplayDepth)
}

func TestSettingsBareHostDefaultsPort(t *testing.T) {
	s, err := settingsFromConnectorSpec(ircConfig(map[string]any{"network": "irc.oftc.net"}))
	require.NoError(t, err)
	require.Equal(t, "irc.oftc.net", s.Hostname)
	require.Equal(t, 6697, s.Port)
}

func TestSettingsRejectsEmptyNetwork(t *testing.T) {
	_, err := settingsFromConnectorSpec(ircConfig(map[string]any{"network": ""}))
	require.Error(t, err)
}

func TestSettingsRejectsBadPort(t *testing.T) {
	_, err := settingsFromConnectorSpec(ircConfig(map[string]any{"network": "host:nope"}))
	require.Error(t, err)
}

func TestSettingsRejectsMissingNick(t *testing.T) {
	cs := ircConfig(nil)
	delete(cs.Config, "nick")
	_, err := settingsFromConnectorSpec(cs)
	require.Error(t, err, "irc.Settings.Validate requires a nick")
}

func TestSplitNetwork(t *testing.T) {
	host, port, err := splitNetwork("example.net:7000")
	require.NoError(t, err)
	require.Equal(t, "example.net", host)
	require.Equal(t, 7000, port)
}
