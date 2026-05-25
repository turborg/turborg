package server

import (
	"testing"

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
