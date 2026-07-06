package telegram

import (
	"errors"
	"strings"

	"github.com/caarlos0/env/v11"
)

// Settings is the env-driven config for the Telegram connector. Tag names
// define the TURBORG_TELEGRAM_* contract; the pooled runtime derives the same
// struct from a connector spec (see server.telegramSettingsFromConnectorSpec).
//
// Required (no default): Token. Chats is an allow-list of chat ids (empty =
// respond in every chat). Suspended records the boot-time connect intent.
type Settings struct {
	// Token is the bot token used to authenticate with the Bot API.
	Token string `env:"TOKEN"`
	// Chats is the allowed chat-id list. Empty = respond everywhere.
	Chats []string `env:"CHATS" envSeparator:","`
	// Suspended is the boot-time connect/disconnect intent (park on start).
	Suspended bool `env:"SUSPENDED"`
}

// LoadSettings parses the TURBORG_TELEGRAM_* environment into a Settings.
func LoadSettings() (*Settings, error) {
	s := &Settings{}
	if err := env.ParseWithOptions(s, env.Options{Prefix: "TURBORG_TELEGRAM_"}); err != nil {
		return nil, err
	}
	s.Chats = cleanList(s.Chats)
	return s, nil
}

// Validate enforces the fields the connector can't run without.
func (s *Settings) Validate() error {
	if strings.TrimSpace(s.Token) == "" {
		return errors.New("telegram: bot token required")
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
