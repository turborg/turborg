// Package config holds turborg's top-level Settings — the cross-cutting
// env-var contract that lives under TURBORG_*. Per-connector knobs
// (TURBORG_IRC_*, etc.) live with their respective connectors. The
// gateway is a top-level control surface, not a connector, so its env
// lives here under TURBORG_GATEWAY_*.
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
// (TURBORG_IRC_*, …) are loaded by their own packages — Settings stays
// narrow and cross-cutting.
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

	OwnerNick    string `env:"OWNER_NICK"`
	OwnerAccount string `env:"OWNER_ACCOUNT"`

	CommandMaxPerWindow  int `env:"COMMAND_MAX_PER_WINDOW"  envDefault:"5"`
	CommandWindowSeconds int `env:"COMMAND_WINDOW_SECONDS"  envDefault:"30"`

	// Gateway is the bot's control-plane surface: WS protocol + bundled
	// reference UI. Today it streams IRC events; the env vars don't
	// promise a specific protocol or connector, so the same names
	// survive any future transport/connector additions.
	GatewayPassword             string `env:"GATEWAY_PASSWORD"`
	GatewayHost                 string `env:"GATEWAY_HOST" envDefault:"127.0.0.1"`
	GatewayPort                 int    `env:"GATEWAY_PORT" envDefault:"8765"`
	GatewayMaxFailedAttempts    int    `env:"GATEWAY_MAX_FAILED_ATTEMPTS" envDefault:"5"`
	GatewayFailureWindowSeconds int    `env:"GATEWAY_FAILURE_WINDOW_SECONDS" envDefault:"60"`
	GatewayLockoutSeconds       int    `env:"GATEWAY_LOCKOUT_SECONDS" envDefault:"300"`
	GatewayIdleShutdownSeconds  int    `env:"GATEWAY_IDLE_SHUTDOWN_SECONDS"`
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

// GatewayEnabled reports whether the control-plane gateway should be
// started. False = no listener, no UI, no /ws endpoint.
func (s *Settings) GatewayEnabled() bool { return s.GatewayPassword != "" }

// IdleShutdownEnabled reports whether the gateway's idle-shutdown timer
// is configured. Wired by the SaaS sidecar for free-tier containers.
func (s *Settings) IdleShutdownEnabled() bool { return s.GatewayIdleShutdownSeconds > 0 }
