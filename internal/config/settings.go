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

	// Operator policy controls. All optional. When absent the agent
	// behaves exactly as before this section existed — no network
	// whitelist, no channel cap, no identity lock, no outbound throttle
	// beyond what the connector ships with. Zero/empty values uniformly
	// mean "unrestricted" so absent-env and explicit-zero behave the same
	// way.

	// Plan is a free-form label. Not used at runtime; kept so the operator
	// can correlate runtime logs with whatever policy bundle they applied.
	Plan string `env:"PLAN"`

	// AllowedNetworks restricts the IRC upstream to a hostname allowlist.
	// Validated in runtime.Build against ircCfg.Hostname; empty = unrestricted.
	AllowedNetworks []string `env:"ALLOWED_NETWORKS" envSeparator:","`

	MaxChannels int `env:"MAX_CHANNELS"`

	// NickLocked, RealnameLocked, RealnameTemplate pin parts of the
	// agent's IRC identity. RealnameLocked + RealnameTemplate overwrites
	// ircCfg.RealName at boot. NickLocked surfaces to the bouncer command
	// policy which rejects /NICK from bouncer-attached clients.
	//
	// Ident (USER message ident) is not substituted here; operators wire
	// it via TURBORG_IRC_USERNAME directly.
	NickLocked       bool   `env:"NICK_LOCKED"`
	RealnameLocked   bool   `env:"REALNAME_LOCKED"`
	RealnameTemplate string `env:"REALNAME_TEMPLATE"`

	// MaxConnectorsPerAgent caps how many distinct connector types the
	// agent will wire up. 0 = unrestricted. Enforced at Load(); a
	// TURBORG_CONNECTORS list longer than this fails fast.
	MaxConnectorsPerAgent int `env:"MAX_CONNECTORS_PER_AGENT"`

	// Per-target outbound message throttle. Distinct from the existing
	// CommandMaxPerWindow / CTCPMaxPerWindow throttles, which govern
	// bot-replies and CTCP responses respectively.
	OutboundMaxPerWindow  int `env:"OUTBOUND_MAX_PER_WINDOW"`
	OutboundWindowSeconds int `env:"OUTBOUND_WINDOW_SECONDS"`

	// LLM caps. Validated here; runtime enforcement lands once an !ask-
	// style LLM-backed command is wired in (no LLM-driven commands ship
	// in turborg today).
	LLMInputTokensPerDay  int      `env:"LLM_INPUT_TOKENS_PER_DAY"`
	LLMOutputTokensPerDay int      `env:"LLM_OUTPUT_TOKENS_PER_DAY"`
	AllowedLLMModels      []string `env:"ALLOWED_LLM_MODELS" envSeparator:","`

	// CustomCommandsMax caps the dynamic-command registry. 0 = builtins
	// only, -1 = unrestricted. The custom-commands API itself is not yet
	// shipping; this knob is present so operators can pre-set it.
	CustomCommandsMax int `env:"CUSTOM_COMMANDS_MAX"`

	// OwnerDMNudgeEvery triggers a DM to the owner after every N outbound
	// messages. 0 = disabled. Used by operators who want a regular usage
	// summary delivered through IRC itself.
	OwnerDMNudgeEvery int `env:"OWNER_DM_NUDGE_EVERY"`
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

	// Plan-tier consistency. Hostname-vs-AllowedNetworks lives in
	// runtime.Build (needs ircCfg).
	if s.OutboundMaxPerWindow > 0 && s.OutboundWindowSeconds <= 0 {
		return errors.New("config: OUTBOUND_WINDOW_SECONDS must be > 0 when OUTBOUND_MAX_PER_WINDOW is set")
	}
	if s.LLMInputTokensPerDay < 0 {
		return errors.New("config: LLM_INPUT_TOKENS_PER_DAY must be >= 0 (0 = unrestricted)")
	}
	if s.LLMOutputTokensPerDay < 0 {
		return errors.New("config: LLM_OUTPUT_TOKENS_PER_DAY must be >= 0 (0 = unrestricted)")
	}
	if s.MaxChannels < 0 {
		return errors.New("config: MAX_CHANNELS must be >= 0 (0 = unrestricted)")
	}
	if s.MaxConnectorsPerAgent < 0 {
		return errors.New("config: MAX_CONNECTORS_PER_AGENT must be >= 0 (0 = unrestricted)")
	}
	// Cap-exceeded branch will land once a second valid connector exists.
	// Today the dedup loop above keeps len(s.Connectors) <= 1 with "irc"
	// the only ValidConnectors entry, so a cap-vs-count compare is unreachable.

	// AllowedNetworks: trim entries, drop empties. The hostname check
	// itself runs in runtime.Build.
	if len(s.AllowedNetworks) > 0 {
		cleaned := make([]string, 0, len(s.AllowedNetworks))
		for _, n := range s.AllowedNetworks {
			n = strings.TrimSpace(n)
			if n != "" {
				cleaned = append(cleaned, n)
			}
		}
		s.AllowedNetworks = cleaned
	}

	// AllowedLLMModels: same trim-and-drop-empties pass.
	if len(s.AllowedLLMModels) > 0 {
		cleaned := make([]string, 0, len(s.AllowedLLMModels))
		for _, m := range s.AllowedLLMModels {
			m = strings.TrimSpace(m)
			if m != "" {
				cleaned = append(cleaned, m)
			}
		}
		s.AllowedLLMModels = cleaned
	}

	return nil
}

// HostnameAllowed reports whether the given upstream hostname satisfies
// the plan's network whitelist. Empty whitelist = unrestricted (true).
func (s *Settings) HostnameAllowed(hostname string) bool {
	if len(s.AllowedNetworks) == 0 {
		return true
	}
	for _, allowed := range s.AllowedNetworks {
		if allowed == hostname {
			return true
		}
	}
	return false
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
