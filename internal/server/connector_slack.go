package server

import (
	"fmt"

	slackconn "github.com/turborg/turborg/internal/connector/slack"
)

// slackSettingsFromConnectorSpec maps a tenant's slack ConnectorSpec onto
// slack.Settings. Pure — no IO. The single-instance binary derives the same
// struct from env via slack.LoadSettings.
//
// Wire contract: config{channels[], suspended}, secrets{bot_token, app_token}.
func slackSettingsFromConnectorSpec(cs ConnectorSpec) (*slackconn.Settings, error) {
	s := &slackconn.Settings{
		BotToken:  stringField(cs.Secrets, "bot_token"),
		AppToken:  stringField(cs.Secrets, "app_token"),
		Channels:  stringSlice(cs.Config, "channels"),
		Suspended: boolField(cs.Config, "suspended", false),
	}
	if err := s.Validate(); err != nil {
		return nil, fmt.Errorf("slack connector: %w", err)
	}
	return s, nil
}
