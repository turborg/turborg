package discord

import (
	"errors"
	"strings"

	"github.com/caarlos0/env/v11"
)

// Settings is the env-driven config for the Discord connector. Tag names define
// the TURBORG_DISCORD_* contract; the pooled runtime derives the same struct
// from a connector spec (see server.discordSettingsFromConnectorSpec).
//
// Required (no default): Token, GuildID. Channels is an allow-list (empty =
// respond in every channel). Suspended records the boot-time connect intent.
type Settings struct {
	// Token is the bot token used to authenticate the Gateway session.
	Token string `env:"TOKEN"`
	// GuildID is the snowflake of the single guild this connector bridges.
	GuildID string `env:"GUILD_ID"`
	// Channels is the channel-id allow-list. Empty = respond everywhere.
	Channels []string `env:"CHANNELS" envSeparator:","`
	// Suspended is the boot-time connect/disconnect intent (park on start).
	Suspended bool `env:"SUSPENDED"`
}

// LoadSettings parses the TURBORG_DISCORD_* environment into a Settings.
func LoadSettings() (*Settings, error) {
	s := &Settings{}
	if err := env.ParseWithOptions(s, env.Options{Prefix: "TURBORG_DISCORD_"}); err != nil {
		return nil, err
	}
	s.Channels = cleanList(s.Channels)
	return s, nil
}

// Validate enforces the fields the connector can't run without.
func (s *Settings) Validate() error {
	if strings.TrimSpace(s.Token) == "" {
		return errors.New("discord: bot token required")
	}
	if strings.TrimSpace(s.GuildID) == "" {
		return errors.New("discord: guild id required")
	}
	return nil
}

// cleanList trims whitespace/quotes and drops empty entries from a raw list.
func cleanList(in []string) []string {
	out := make([]string, 0, len(in))
	for _, v := range in {
		v = strings.Trim(strings.TrimSpace(v), `"'`)
		if v != "" {
			out = append(out, v)
		}
	}
	return out
}
