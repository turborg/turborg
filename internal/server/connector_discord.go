package server

import (
	"fmt"

	discordconn "github.com/turborg/turborg/internal/connector/discord"
)

// discordSettingsFromConnectorSpec maps a tenant's discord ConnectorSpec (the
// feed wire shape: config map + secrets map) onto discord.Settings. Pure — no
// IO. The single-instance binary derives the same struct from env via
// discord.LoadSettings; this is the pooled-mode equivalent sourced from a spec.
//
// Wire contract: config{guild_id, channels[], suspended}, secrets{bot_token}.
func discordSettingsFromConnectorSpec(cs ConnectorSpec) (*discordconn.Settings, error) {
	s := &discordconn.Settings{
		Token:     stringField(cs.Secrets, "bot_token"),
		GuildID:   stringField(cs.Config, "guild_id"),
		Channels:  stringSlice(cs.Config, "channels"),
		Suspended: boolField(cs.Config, "suspended", false),
	}
	if err := s.Validate(); err != nil {
		return nil, fmt.Errorf("discord connector: %w", err)
	}
	return s, nil
}
