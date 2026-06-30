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

	// Owner identification — see internal/runtime/runtime.go's
	// BuildCommandGuard for the resolver semantics.
	//
	// OwnerMode picks which trust model the agent uses for !commands:
	//   - "none" (default): !commands are disabled entirely.
	//   - "self":           the bot's own nick is the owner. Useful for
	//                       personal-AI-assistant setups where the operator
	//                       attaches to the bouncer and IS the bot.
	//   - "external":       a separate nick (OwnerNick) controls the bot.
	//                       Verified via account-tag, with an optional
	//                       hostmask fallback for services-less networks.
	//
	// OwnerNick is the configured nick to trust when OwnerMode == "external".
	// OwnerAccount is an optional override for the account-tag match value;
	// defaults to OwnerNick when empty (the common case where the operator's
	// nick equals their NickServ account name).
	// OwnerHostmask is the optional services-less fallback (e.g. QuakeNet
	// private IRCDs). When set, an inbound message matching OwnerNick AND
	// the hostmask is trusted even without an account-tag.
	OwnerMode     string `env:"OWNER_MODE" envDefault:"none"`
	OwnerNick     string `env:"OWNER_NICK"`
	OwnerAccount  string `env:"OWNER_ACCOUNT"`
	OwnerHostmask string `env:"OWNER_HOSTMASK"`

	CommandMaxPerWindow  int `env:"COMMAND_MAX_PER_WINDOW"  envDefault:"5"`
	CommandWindowSeconds int `env:"COMMAND_WINDOW_SECONDS"  envDefault:"30"`

	// Gateway is the bot's web management surface: WS protocol + bundled
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

	// LLMInputTokensUsed / LLMOutputTokensUsed seed the rolling-window budget
	// with consumption already reported across the account for the current
	// window (sibling agents and previously-destroyed ones), supplied by the
	// operator at boot. They make the cap enforce per account/window instead of
	// resetting to zero on every restart/recreate. 0 = fresh window.
	LLMInputTokensUsed  int `env:"LLM_INPUT_TOKENS_USED"`
	LLMOutputTokensUsed int `env:"LLM_OUTPUT_TOKENS_USED"`

	// LLMBudgetURL is an optional endpoint the agent polls to keep the token
	// budget's account baseline current while it runs (the boot seed above is
	// only a point-in-time value). Point it at a service that tracks usage
	// across restarts; the agent sends its start time as `?since=` so the
	// response can exclude what it counts locally. Empty = no live refresh (the
	// boot seed is all there is, which is correct for single-agent installs).
	LLMBudgetURL string `env:"LLM_BUDGET_URL"`
	// LLMBudgetToken is the bearer sent on every budget poll. Defaults to
	// MessageSinkToken via normalize() when unset — both typically terminate at
	// the same endpoint with the same bearer.
	LLMBudgetToken string `env:"LLM_BUDGET_TOKEN"`
	// LLMBudgetRefreshSeconds is the poll interval. 0 uses the package default
	// (15s); values below the floor are clamped up by the refresher.
	LLMBudgetRefreshSeconds int `env:"LLM_BUDGET_REFRESH_SECONDS"`

	// CommandsURL is an optional endpoint the agent polls to hot-reload its
	// data-driven command set while it runs — no reconnect. Point it at a
	// service that serves the latest command set as JSON. Empty = no live
	// reload (the boot TURBORG_COMMANDS set is fixed). This gives a
	// single-instance agent the same in-place ReplaceDynamic the pooled runtime
	// already does from its tenant feed.
	CommandsURL string `env:"COMMANDS_URL"`
	// CommandsToken is the bearer sent on every commands poll. Defaults to
	// MessageSinkToken via normalize() when unset — same per-container endpoint.
	CommandsToken string `env:"COMMANDS_TOKEN"`
	// CommandsRefreshSeconds is the poll interval. 0 uses the package default;
	// values below the floor are clamped up by the refresher.
	CommandsRefreshSeconds int `env:"COMMANDS_REFRESH_SECONDS"`

	// ConfigURL is an optional endpoint the agent polls to hot-reload its live
	// IRC nick + channel set while it runs — no reconnect. Empty = no live
	// reload (the boot TURBORG_IRC_NICK / CHANNELS are fixed). This gives a
	// single-instance agent the same live nick/channel reconcile the pooled
	// runtime does from its tenant feed.
	ConfigURL string `env:"CONFIG_URL"`
	// ConfigToken is the bearer sent on every config poll. Defaults to
	// MessageSinkToken via normalize() when unset — same per-container endpoint.
	ConfigToken string `env:"CONFIG_TOKEN"`
	// ConfigRefreshSeconds is the poll interval. 0 uses the package default;
	// values below the floor are clamped up by the refresher.
	ConfigRefreshSeconds int `env:"CONFIG_REFRESH_SECONDS"`

	// CustomCommandsMax caps the dynamic-command registry. 0 = no
	// commands, -1 = unrestricted. Bounds the command set loaded from
	// TURBORG_COMMANDS (and any later runtime registrations).
	CustomCommandsMax int `env:"CUSTOM_COMMANDS_MAX"`

	// TBSummarizeMaxMessages caps how many channel messages /tb summarize
	// can consume per invocation. 0 = disabled.
	TBSummarizeMaxMessages int `env:"TB_SUMMARIZE_MAX_MESSAGES"`

	// LLM router. When LLMProvider or LLMAPIKey is set these select +
	// configure the LLM backend used by LLM-type commands, taking
	// precedence over the legacy TURBORG_ANTHROPIC_* envs. LLMProvider is
	// "anthropic" (default) or "openai_compat"; LLMBaseURL points at the
	// OpenAI-compatible endpoint root for the latter; LLMModel is the
	// default model when a command doesn't pin its own.
	LLMProvider string `env:"LLM_PROVIDER"`
	LLMBaseURL  string `env:"LLM_BASE_URL"`
	LLMAPIKey   string `env:"LLM_API_KEY"`
	LLMModel    string `env:"LLM_MODEL"`

	// Commands is the JSON-encoded set of data-driven commands the agent
	// dispatches: an array of {name,type,template,model,access,allowlist}
	// objects. Empty → the agent dispatches nothing. This is the per-
	// process transport for the same command set a multi-tenant deployment
	// delivers through its tenant feed.
	Commands string `env:"COMMANDS"`

	// Flows is the JSON-encoded set of declarative node-graph flows the agent
	// runs: an array of {name,trigger,nodes,edges} objects. Each flow wires
	// activity nodes (set/if/switch/say/effect/llm/webhook/…) into a graph the
	// agent walks when the flow's trigger fires. Empty → no flows. The graph is
	// declarative — nodes come from a fixed catalog; there is no arbitrary code
	// execution.
	Flows string `env:"FLOWS"`

	// OwnerDMNudgeEvery triggers a DM to the owner after every N outbound
	// messages. 0 = disabled. Used by operators who want a regular usage
	// summary delivered through IRC itself.
	OwnerDMNudgeEvery int `env:"OWNER_DM_NUDGE_EVERY"`

	// IgnoredNicks is the operator's chat-ignore list. Senders matching one of
	// these nicks (case-insensitive) are denied early in the command guard —
	// !commands never dispatch, future LLM triggers will skip them too. This is
	// per-bot policy, set by the operator. Changing it takes effect on the next
	// restart.
	//
	// Empty = no ignores. CSV in env (TURBORG_IGNORED_NICKS=alice,bob);
	// whitespace + case are normalized at guard-build time.
	IgnoredNicks []string `env:"IGNORED_NICKS" envSeparator:","`

	// ActivityURL is an optional webhook the agent POSTs to whenever
	// meaningful runtime activity occurs: the bot sending a message, a
	// bouncer client attaching, or a WS gateway client completing the
	// handshake. Empty = no posts. Payload is a single-field JSON object
	// `{"reason": "<id>"}` where <id> is one of bot_spoke, bouncer_attach,
	// ws_attach. Useful for operators wiring uptime / heartbeat dashboards
	// without parsing log streams.
	ActivityURL string `env:"ACTIVITY_URL"`

	// ActivityToken is sent as a bearer Authorization header on every
	// activity webhook POST. Optional.
	ActivityToken string `env:"ACTIVITY_TOKEN"`

	// StateWebhookURL is the endpoint the agent PUTs authoritative
	// per-connector state to (current connection status, joined
	// channels with any cached +k keys, preferred nick) whenever
	// that state changes. Payload is an idempotent JSON snapshot —
	// receivers overwrite-on-write. Empty = no PUTs, no goroutine
	// spawned. Distinct from ACTIVITY_URL: that one carries
	// fire-and-forget event signals; this one carries a full state
	// mirror.
	StateWebhookURL string `env:"STATE_WEBHOOK_URL"`

	// StateWebhookToken is sent as a bearer Authorization header on
	// every state-webhook PUT. Optional.
	StateWebhookToken string `env:"STATE_WEBHOOK_TOKEN"`

	// StateWebhookDebounceMs is the debounce window applied to the
	// state-webhook emitter. A bursty client mutation (e.g.
	// /join #a,#b,#c) coalesces into a single PUT so receivers see
	// one snapshot per change, not one per channel. 0 disables the
	// debounce (every state change fires immediately); the upper
	// bound keeps a misconfigured value from making a dashboard
	// view feel laggy.
	StateWebhookDebounceMs int `env:"STATE_WEBHOOK_DEBOUNCE_MS" envDefault:"250"`

	// MessageSinkURL is the endpoint the gateway POSTs durable
	// message batches to. Point it at a service that persists them
	// for long-term history. Empty = no durable mirror, the gateway's
	// in-memory ring is still the only history (already replayed on
	// (re)connect).
	MessageSinkURL string `env:"MESSAGE_SINK_URL"`

	// MessageSinkToken is sent as a bearer Authorization header on
	// every message-sink POST. Mirrors StateWebhookToken — the same
	// token can be reused when both endpoints terminate at the same
	// receiver.
	MessageSinkToken string `env:"MESSAGE_SINK_TOKEN"`

	// MessageStoreURL is the endpoint the bouncer (CHATHISTORY) +
	// gateway (history op, attach replay) GET to read historical
	// messages. Empty = no remote read backend: replay + scrollback
	// fall back to the in-process MemoryStore (capped at 200/channel,
	// lost across restarts). When set, operators point it at an HTTP
	// service that implements the contract documented on
	// messages.HTTPStore.
	MessageStoreURL string `env:"MESSAGE_STORE_URL"`

	// MessageStoreToken is the bearer the store endpoint expects on
	// every GET. Defaults to MessageSinkToken via normalize() when
	// unset — the write and read halves typically share the same
	// per-container token.
	MessageStoreToken string `env:"MESSAGE_STORE_TOKEN"`
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
	if err := s.normalizeConnectors(); err != nil {
		return err
	}
	if err := s.validatePolicyBounds(); err != nil {
		return err
	}
	s.AllowedNetworks = trimDropEmpties(s.AllowedNetworks)
	s.AllowedLLMModels = trimDropEmpties(s.AllowedLLMModels)

	// MessageStoreToken defaults to the sink token — both halves
	// usually terminate at the same endpoint with the same bearer.
	// Operators who want different tokens
	// for read vs. write set MESSAGE_STORE_TOKEN explicitly.
	if s.MessageStoreToken == "" {
		s.MessageStoreToken = s.MessageSinkToken
	}
	// The budget-refresh poll terminates at the same per-container endpoint as
	// the message sink, so it reuses that bearer unless overridden.
	if s.LLMBudgetToken == "" {
		s.LLMBudgetToken = s.MessageSinkToken
	}
	// The commands poll terminates at the same per-container endpoint, so it
	// reuses that bearer unless overridden.
	if s.CommandsToken == "" {
		s.CommandsToken = s.MessageSinkToken
	}
	// The config (nick/channels) poll terminates at the same per-container
	// endpoint, so it reuses that bearer unless overridden.
	if s.ConfigToken == "" {
		s.ConfigToken = s.MessageSinkToken
	}
	return nil
}

func (s *Settings) normalizeConnectors() error {
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
	// Cap-exceeded branch will land once a second valid connector exists.
	// Today the dedup loop above keeps len(s.Connectors) <= 1 with "irc"
	// the only ValidConnectors entry, so a cap-vs-count compare is unreachable.
	return nil
}

// validatePolicyBounds runs the numeric/range checks on operator-policy
// fields. Each check is independent — the function fails on the first
// error so the operator gets pointed at one problem at a time.
func (s *Settings) validatePolicyBounds() error {
	if s.OutboundMaxPerWindow > 0 && s.OutboundWindowSeconds <= 0 {
		return errors.New("config: OUTBOUND_WINDOW_SECONDS must be > 0 when OUTBOUND_MAX_PER_WINDOW is set")
	}
	if s.LLMInputTokensPerDay < 0 {
		return errors.New("config: LLM_INPUT_TOKENS_PER_DAY must be >= 0 (0 = unrestricted)")
	}
	if s.LLMOutputTokensPerDay < 0 {
		return errors.New("config: LLM_OUTPUT_TOKENS_PER_DAY must be >= 0 (0 = unrestricted)")
	}
	if s.LLMInputTokensUsed < 0 {
		return errors.New("config: LLM_INPUT_TOKENS_USED must be >= 0 (0 = fresh window)")
	}
	if s.LLMOutputTokensUsed < 0 {
		return errors.New("config: LLM_OUTPUT_TOKENS_USED must be >= 0 (0 = fresh window)")
	}
	if s.MaxChannels < 0 {
		return errors.New("config: MAX_CHANNELS must be >= 0 (0 = unrestricted)")
	}
	if s.MaxConnectorsPerAgent < 0 {
		return errors.New("config: MAX_CONNECTORS_PER_AGENT must be >= 0 (0 = unrestricted)")
	}
	if s.StateWebhookDebounceMs < 0 || s.StateWebhookDebounceMs > 5000 {
		return errors.New("config: STATE_WEBHOOK_DEBOUNCE_MS must be in [0, 5000]")
	}
	return nil
}

func trimDropEmpties(in []string) []string {
	if len(in) == 0 {
		return in
	}
	out := make([]string, 0, len(in))
	for _, v := range in {
		v = strings.TrimSpace(v)
		if v != "" {
			out = append(out, v)
		}
	}
	return out
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

// GatewayEnabled reports whether the web gateway should be started.
// False = no listener, no UI, no /ws endpoint.
func (s *Settings) GatewayEnabled() bool { return s.GatewayPassword != "" }

// IdleShutdownEnabled reports whether the gateway's idle-shutdown timer
// is configured.
func (s *Settings) IdleShutdownEnabled() bool { return s.GatewayIdleShutdownSeconds > 0 }

// ActivityEnabled reports whether the agent should POST runtime
// activity signals to a remote observer. False when ACTIVITY_URL is
// unset; the notifier in that case is a no-op.
func (s *Settings) ActivityEnabled() bool { return s.ActivityURL != "" }
