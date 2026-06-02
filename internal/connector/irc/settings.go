package irc

import (
	"errors"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"
)

// Settings is the env-driven config for the IRC connector. Tag names
// define the TURBORG_IRC_* contract — see docs/connectors/irc.md for
// the full operator-facing reference.
//
// Required fields (no default): Hostname, Nick. Everything else defaults
// to a sensible value or stays empty/disabled.
type Settings struct {
	Hostname string `env:"HOSTNAME,required"`
	Port     int    `env:"PORT"     envDefault:"6697"`
	UseTLS   bool   `env:"USE_TLS"  envDefault:"true"`

	// SourceIP, when set, is bound as the local address of the outbound IRC
	// connection so the host SNAT (matched by source subnet) egresses on the
	// tenant's assigned public IP. Empty = default route (single-IP hosts,
	// unconfigured). Set by the pooled runtime per-tenant; also overridable via
	// env for a dedicated/self-host bind.
	SourceIP string `env:"SOURCE_IP"`

	Nick     string `env:"NICK,required"`
	Username string `env:"USERNAME"`
	RealName string `env:"REAL_NAME" envDefault:"turborg agent"`

	ServerPassword   string `env:"SERVER_PASSWORD"`
	NickServPassword string `env:"NICKSERV_PASSWORD"`

	SASLUser     string `env:"SASL_USER"`
	SASLPassword string `env:"SASL_PASSWORD"`

	// AuthMode selects which network authentication scheme the
	// connector uses during registration. Values: "sasl", "nickserv",
	// "none". SASL and NickServ are mutually exclusive — the operator
	// picks one. Empty AuthMode is the legacy fallback: SASL active
	// when both creds present, NickServ active when its password is
	// set, otherwise no auth. Explicit "none" disables both even when
	// stale creds are still in the env.
	AuthMode string `env:"AUTH_MODE"`

	Channels []string `env:"CHANNELS" envSeparator:","`

	// DialTimeout bounds the TCP+TLS connect itself. HandshakeTimeout
	// covers the post-connect registration reads, not the dial, so a node
	// that accepts the TCP connection then stalls the TLS handshake (e.g.
	// a server zero-windowing us under connection throttling) needs this
	// separate bound or the connect hangs indefinitely.
	DialTimeout        time.Duration `env:"DIAL_TIMEOUT"         envDefault:"20s"`
	HandshakeTimeout   time.Duration `env:"HANDSHAKE_TIMEOUT"    envDefault:"30s"`
	ReadIdleTimeout    time.Duration `env:"READ_IDLE_TIMEOUT"    envDefault:"300s"`
	ClientPingInterval time.Duration `env:"CLIENT_PING_INTERVAL" envDefault:"120s"`

	// PongTimeout is the maximum time the connector will wait for a PONG
	// response to its most recent client-initiated PING before classifying
	// the upstream as dead and unwinding the session. Independent of
	// ReadIdleTimeout (which catches the "no inbound data at all" case) —
	// PongTimeout actively probes liveness on a schedule, dropping
	// silently-dead socket detection from ~ReadIdleTimeout to
	// ~ClientPingInterval + PongTimeout. 0 disables the active probe and
	// falls back to ReadIdleTimeout only. When ClientPingInterval is 0
	// the probe is also functionally disabled (no PINGs to time out).
	PongTimeout time.Duration `env:"PONG_TIMEOUT" envDefault:"30s"`

	// QuitMessage is the body sent on the IRC QUIT command during a
	// graceful shutdown. Renders inside parentheses on every channel
	// the agent was in (`* nick has quit (<reason>)`). Operators running
	// a hosted turborg fleet usually set this to a service-identifying
	// string so observers can tell which host / platform the bot was
	// running on; the default keeps it project-attributed for self-
	// hosted users.
	QuitMessage string `env:"QUIT_MESSAGE" envDefault:"bye from turborg"`

	BouncerPassword             string `env:"BOUNCER_PASSWORD"`
	BouncerHost                 string `env:"BOUNCER_HOST" envDefault:"127.0.0.1"`
	BouncerPort                 int    `env:"BOUNCER_PORT" envDefault:"31337"`
	BouncerRatelimitEnabled     bool   `env:"BOUNCER_RATELIMIT_ENABLED" envDefault:"true"`
	BouncerMaxFailedAttempts    int    `env:"BOUNCER_MAX_FAILED_ATTEMPTS" envDefault:"5"`
	BouncerFailureWindowSeconds int    `env:"BOUNCER_FAILURE_WINDOW_SECONDS" envDefault:"60"`
	BouncerLockoutSeconds       int    `env:"BOUNCER_LOCKOUT_SECONDS" envDefault:"300"`
	// BouncerWelcomeReplayDepth controls how many recent channel
	// messages the bouncer ships per joined channel on each fresh
	// client attach. Bumping it lets IRC clients without
	// `draft/chathistory` support (HexChat 2.16 et al) see a deeper
	// backfill on attach, at the cost of a slower welcome on every
	// reconnect. Capped at 2000 by the bouncer to keep welcome
	// bursts bounded.
	BouncerWelcomeReplayDepth int `env:"BOUNCER_WELCOME_REPLAY_DEPTH" envDefault:"200"`

	CTCPAutoReply     bool `env:"CTCP_AUTO_REPLY" envDefault:"true"`
	CTCPMaxPerWindow  int  `env:"CTCP_MAX_PER_WINDOW" envDefault:"3"`
	CTCPWindowSeconds int  `env:"CTCP_WINDOW_SECONDS" envDefault:"30"`

	// AIStrict requires the bot to hold channel-operator status (+o) in a
	// channel before AI commands that read its history (e.g. /tb summarize)
	// will run there. Off by default; set it on networks whose bot policy
	// requires operator consent for LLM processing of channel messages.
	AIStrict bool `env:"AI_STRICT"`

	// AIStrictMessage overrides the notice sent when an AI history command
	// is denied under AIStrict. Empty uses the neutral built-in default;
	// set it to a network's specific policy wording/URL.
	AIStrictMessage string `env:"AI_STRICT_MESSAGE"`

	// UpstreamWarnAfter is the dwell time in disconnected_transient
	// before the reconnect supervisor fires its operator-visible warn
	// hook. 0 disables the warn step.
	UpstreamWarnAfter time.Duration `env:"UPSTREAM_WARN_AFTER" envDefault:"10m"`

	// UpstreamPauseAfter is the dwell time in disconnected_transient
	// before the supervisor escalates to paused_idle and halts the
	// reconnect loop, requiring operator intervention to resume. 0
	// disables escalation (the supervisor retries indefinitely).
	UpstreamPauseAfter time.Duration `env:"UPSTREAM_PAUSE_AFTER" envDefault:"1h"`
}

// LoadSettings parses the TURBORG_IRC_* environment into a Settings.
// Missing required vars surface as ValidationError from caarlos0/env.
func LoadSettings() (*Settings, error) {
	s := &Settings{}
	if err := env.ParseWithOptions(s, env.Options{Prefix: "TURBORG_IRC_"}); err != nil {
		return nil, err
	}
	// Normalize Channels — accept both CSV (#a,#b) and a JSON-array
	// shape (["#a","#b"]) so the var is friendly to config systems
	// that emit lists. Tolerates leading "[" / trailing "]" /
	// surrounding quotes on individual entries.
	s.Channels = parseChannelList(strings.Join(s.Channels, ","))
	return s, nil
}

// parseChannelList accepts CSV or a single JSON-array-shaped string and
// returns a clean slice of channel names. Trims whitespace and any
// stray single/double quotes around individual entries.
func parseChannelList(raw string) []string {
	raw = strings.TrimSpace(raw)
	raw = strings.TrimPrefix(raw, "[")
	raw = strings.TrimSuffix(raw, "]")
	if raw == "" {
		return nil
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		p = strings.Trim(p, `"'`)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

// ApplyDefaults fills the operational fields a HAND-BUILT Settings leaves at
// their zero value with the same defaults the env loader applies — so a Settings
// built from a spec (the pooled runtime's settingsFromConnectorSpec) behaves
// identically to the env-loaded single-instance path (CTCP auto-reply on, liveness
// probing, reconnect escalation, …). The values mirror the `envDefault` tags
// above; this is the single place the hand-built path picks them up.
//
// It must NOT be called on an env-loaded Settings: the true-default bools below
// are forced on, which would clobber an operator's explicit `CTCP_AUTO_REPLY=false`
// (the env layer preserves that; a fill-zero defaulter can't tell "unset" from
// "explicit false"). The connector spec doesn't express these fields, so forcing
// the defaults is correct for the hand-built path.
func (s *Settings) ApplyDefaults() {
	setIfZero(&s.Port, 6697)
	setIfZero(&s.RealName, "turborg agent")
	setIfZero(&s.QuitMessage, "bye from turborg")
	setIfZero(&s.DialTimeout, 20*time.Second)
	setIfZero(&s.HandshakeTimeout, 30*time.Second)
	setIfZero(&s.ReadIdleTimeout, 300*time.Second)
	setIfZero(&s.ClientPingInterval, 120*time.Second)
	setIfZero(&s.PongTimeout, 30*time.Second)
	setIfZero(&s.UpstreamWarnAfter, 10*time.Minute)
	setIfZero(&s.UpstreamPauseAfter, time.Hour)
	setIfZero(&s.CTCPMaxPerWindow, 3)
	setIfZero(&s.CTCPWindowSeconds, 30)
	setIfZero(&s.BouncerHost, "127.0.0.1")
	setIfZero(&s.BouncerPort, 31337)
	setIfZero(&s.BouncerMaxFailedAttempts, 5)
	setIfZero(&s.BouncerFailureWindowSeconds, 60)
	setIfZero(&s.BouncerLockoutSeconds, 300)
	setIfZero(&s.BouncerWelcomeReplayDepth, 200)
	// true-default bools: a hand-built (spec) Settings leaves these false and the
	// spec doesn't carry them, so default them on to match env.
	s.CTCPAutoReply = true
	s.BouncerRatelimitEnabled = true
}

// setIfZero assigns def to *p only when *p is the zero value. Lets ApplyDefaults
// read as a flat default table instead of N if-blocks.
func setIfZero[T comparable](p *T, def T) {
	var zero T
	if *p == zero {
		*p = def
	}
}

// NormalizedChannels returns Channels with a '#' prefix added where the
// caller omitted one. Channels already starting with #/&/+/! are passed
// through unchanged.
func (s *Settings) NormalizedChannels() []string {
	out := make([]string, 0, len(s.Channels))
	for _, ch := range s.Channels {
		ch = strings.TrimSpace(ch)
		if ch == "" {
			continue
		}
		if strings.HasPrefix(ch, "#") ||
			strings.HasPrefix(ch, "&") ||
			strings.HasPrefix(ch, "+") ||
			strings.HasPrefix(ch, "!") {
			out = append(out, ch)
			continue
		}
		out = append(out, "#"+ch)
	}
	return out
}

// EffectiveUsername returns Username if set, otherwise Nick. The IRC
// USER command requires a non-empty ident; defaulting to the nick is
// the universally-accepted fallback when an operator doesn't set one.
func (s *Settings) EffectiveUsername() string {
	if s.Username != "" {
		return s.Username
	}
	return s.Nick
}

// EffectiveQuitMessage returns QuitMessage if set, otherwise the
// project-attributed fallback. Used so tests that build Settings by
// hand (bypassing LoadSettings + envDefault) still emit a sensible
// QUIT body instead of `QUIT :` with an empty trailing parameter.
func (s *Settings) EffectiveQuitMessage() string {
	if s.QuitMessage != "" {
		return s.QuitMessage
	}
	return "bye from turborg"
}

// SASLEnabled is true when the operator picked SASL as the auth mode
// and both credentials are present. Legacy fallback: when AuthMode is
// empty, infer SASL from the presence of both credentials so existing
// self-host configs keep working without an explicit AUTH_MODE.
func (s *Settings) SASLEnabled() bool {
	hasCreds := s.SASLUser != "" && s.SASLPassword != ""
	mode := strings.ToLower(strings.TrimSpace(s.AuthMode))
	switch mode {
	case "":
		return hasCreds
	case "sasl":
		return hasCreds
	default:
		return false
	}
}

// NickServEnabled is true when the operator picked NickServ as the
// auth mode and the password is present. Legacy fallback mirrors
// SASLEnabled: when AuthMode is empty, infer from credential presence.
func (s *Settings) NickServEnabled() bool {
	hasPassword := s.NickServPassword != ""
	mode := strings.ToLower(strings.TrimSpace(s.AuthMode))
	switch mode {
	case "":
		return hasPassword
	case "nickserv":
		return hasPassword
	default:
		return false
	}
}

// BouncerEnabled is true when the bouncer password is set.
func (s *Settings) BouncerEnabled() bool {
	return s.BouncerPassword != ""
}

// Validate runs cross-field checks that the env-tag layer can't express
// — e.g. client_ping_interval must be < read_idle_timeout.
func (s *Settings) Validate() error {
	if s.ClientPingInterval > 0 && s.ReadIdleTimeout > 0 && s.ClientPingInterval >= s.ReadIdleTimeout {
		return errors.New("irc: client_ping_interval must be less than read_idle_timeout")
	}
	if s.PongTimeout > 0 && s.ClientPingInterval > 0 && s.PongTimeout >= s.ClientPingInterval {
		return errors.New("irc: pong_timeout must be less than client_ping_interval")
	}
	return nil
}
