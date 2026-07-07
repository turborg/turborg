package server

import (
	"fmt"

	telegramconn "github.com/turborg/turborg/internal/connector/telegram"
)

// telegramSettingsFromConnectorSpec maps a tenant's telegram ConnectorSpec onto
// telegram.Settings. Pure — no IO. The single-instance binary derives the same
// struct from env via telegram.LoadSettings.
//
// Wire contract: config{chats[], suspended}, secrets{bot_token}.
func telegramSettingsFromConnectorSpec(cs ConnectorSpec) (*telegramconn.Settings, error) {
	s := &telegramconn.Settings{
		Token:     stringField(cs.Secrets, "bot_token"),
		Chats:     stringSlice(cs.Config, "chats"),
		Suspended: boolField(cs.Config, "suspended", false),
	}
	if err := s.Validate(); err != nil {
		return nil, fmt.Errorf("telegram connector: %w", err)
	}
	return s, nil
}
