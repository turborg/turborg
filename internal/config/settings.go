// Package config holds turborg's top-level Settings — the cross-cutting
// env-var contract that lives next to TURBORG_ (not TURBORG_IRC_,
// TURBORG_WEB_, etc., which live with their respective connectors).
//
// Connector-specific settings stay in their own packages so installing
// turborg without a particular connector does not require setting that
// connector's variables.
package config

import (
	"errors"
	"fmt"
	"strings"

	"github.com/caarlos0/env/v11"
)

// ValidConnectors is the closed set of connector names the runtime
// knows how to build. Widen alongside new arms in
// runtime.buildConnector.
var ValidConnectors = map[string]bool{
	"irc": true,
}

// Settings is the top-level turborg config. Connector-specific knobs
// (TURBORG_IRC_*, TURBORG_WEB_PASSWORD lives here for the gateway
// toggle, but everything WS-protocol-shaped is on web.Options) are
// loaded by their own packages — Settings stays narrow.
type Settings struct {
	LogLevel  string `env:"LOG_LEVEL"  envDefault:"INFO"`
	LogFormat string `env:"LOG_FORMAT" envDefault:"text"`

	CommandPrefix string `env:"COMMAND_PREFIX" envDefault:"!"`

	AnthropicAPIKey string `env:"ANTHROPIC_API_KEY"`
	AnthropicModel  string `env:"ANTHROPIC_MODEL" envDefault:"claude-sonnet-4-6"`

	HiveEnabled bool   `env:"HIVE_ENABLED" envDefault:"false"`
	HiveURL     string `env:"HIVE_URL"`

	// Connectors is a CSV in env (TURBORG_CONNECTORS=irc,discord). When
	// empty, the single-connector quickstart path enables IRC only.
	Connectors []string `env:"CONNECTORS" envSeparator:","`

	WebPassword              string `env:"WEB_PASSWORD"`
	WebHost                  string `env:"WEB_HOST"   envDefault:"127.0.0.1"`
	WebPort                  int    `env:"WEB_PORT"   envDefault:"8765"`
	WebMaxFailedAttempts     int    `env:"WEB_MAX_FAILED_ATTEMPTS" envDefault:"5"`
	WebFailureWindowSeconds  int    `env:"WEB_FAILURE_WINDOW_SECONDS" envDefault:"60"`
	WebLockoutSeconds        int    `env:"WEB_LOCKOUT_SECONDS" envDefault:"300"`
	WebIdleShutdownSeconds   int    `env:"WEB_IDLE_SHUTDOWN_SECONDS"`

	OwnerNick    string `env:"OWNER_NICK"`
	OwnerAccount string `env:"OWNER_ACCOUNT"`

	CommandMaxPerWindow  int `env:"COMMAND_MAX_PER_WINDOW"  envDefault:"5"`
	CommandWindowSeconds int `env:"COMMAND_WINDOW_SECONDS"  envDefault:"30"`
}

// Load parses TURBORG_* env vars into a Settings, normalizing the
// connector list (lowercase, deduplicated, validated against
// ValidConnectors).
func Load() (*Settings, error) {
	s := &Settings{}
	if err := env.ParseWithOptions(s, env.Options{Prefix: "TURBORG_"}); err != nil {
		return nil, err
	}
	if err := s.normalize(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *Settings) normalize() error {
	if s.CommandPrefix == "" {
		return errors.New("config: COMMAND_PREFIX must not be empty")
	}
	seen := map[string]bool{}
	out := s.Connectors[:0]
	for _, c := range s.Connectors {
		c = strings.ToLower(strings.TrimSpace(c))
		if c == "" || seen[c] {
			continue
		}
		if !ValidConnectors[c] {
			return fmt.Errorf("config: unknown connector %q (valid: irc)", c)
		}
		seen[c] = true
		out = append(out, c)
	}
	s.Connectors = out
	return nil
}

// AnthropicEnabled reports whether an Anthropic LLM provider should be
// wired. False means standalone mode — the !ask command is not
// registered.
func (s *Settings) AnthropicEnabled() bool { return s.AnthropicAPIKey != "" }

// WebEnabled reports whether the WS gateway should be started.
func (s *Settings) WebEnabled() bool { return s.WebPassword != "" }

// IdleShutdownEnabled reports whether the gateway's idle-shutdown timer
// is configured.
func (s *Settings) IdleShutdownEnabled() bool { return s.WebIdleShutdownSeconds > 0 }
