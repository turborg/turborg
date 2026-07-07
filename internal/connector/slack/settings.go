package slack

import (
	"errors"
	"strings"

	"github.com/caarlos0/env/v11"
)

// Settings is the env-driven config for the Slack connector. Tag names define
// the TURBORG_SLACK_* contract; the pooled runtime derives the same struct from
// a connector spec (see server.slackSettingsFromConnectorSpec).
//
// Required (no default): BotToken (xoxb-…), AppToken (xapp-…, for Socket Mode).
// Channels is an allow-list (empty = respond in every channel). Suspended
// records the boot-time connect intent.
type Settings struct {
	// BotToken is the xoxb- bot user token used for Web API calls.
	BotToken string `env:"BOT_TOKEN"`
	// AppToken is the xapp- app-level token used to open the Socket Mode WS.
	AppToken string `env:"APP_TOKEN"`
	// Channels is the channel-id allow-list. Empty = respond everywhere.
	Channels []string `env:"CHANNELS" envSeparator:","`
	// Suspended is the boot-time connect/disconnect intent (park on start).
	Suspended bool `env:"SUSPENDED"`
}

// LoadSettings parses the TURBORG_SLACK_* environment into a Settings.
func LoadSettings() (*Settings, error) {
	s := &Settings{}
	if err := env.ParseWithOptions(s, env.Options{Prefix: "TURBORG_SLACK_"}); err != nil {
		return nil, err
	}
	s.Channels = cleanList(s.Channels)
	return s, nil
}

// Validate enforces the fields the connector can't run without.
func (s *Settings) Validate() error {
	if strings.TrimSpace(s.BotToken) == "" {
		return errors.New("slack: bot token required")
	}
	if strings.TrimSpace(s.AppToken) == "" {
		return errors.New("slack: app token required")
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
