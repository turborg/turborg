package irc

import (
	"errors"
	"strings"
	"time"

	"github.com/caarlos0/env/v11"
)

// Settings is the env-driven config for the IRC connector. Tag names map
// to the TURBORG_IRC_* contract the xshellz sidecar already sets; every
// var name and default value matches Python core/connectors/irc/settings.py
// byte-for-byte so the SaaS spawner needs no changes during the port.
//
// Required fields (no default): Hostname, Nick. Everything else defaults
// to a sensible value or stays empty/disabled.
type Settings struct {
	Hostname string `env:"HOSTNAME,required"`
	Port     int    `env:"PORT"     envDefault:"6697"`
	UseTLS   bool   `env:"USE_TLS"  envDefault:"true"`

	Nick     string `env:"NICK,required"`
	Username string `env:"USERNAME"`
	RealName string `env:"REAL_NAME" envDefault:"turborg agent"`

	ServerPassword    string `env:"SERVER_PASSWORD"`
	NickServPassword  string `env:"NICKSERV_PASSWORD"`

	SASLUser     string `env:"SASL_USER"`
	SASLPassword string `env:"SASL_PASSWORD"`

	Channels []string `env:"CHANNELS" envSeparator:","`

	HandshakeTimeout   time.Duration `env:"HANDSHAKE_TIMEOUT"    envDefault:"30s"`
	ReadIdleTimeout    time.Duration `env:"READ_IDLE_TIMEOUT"    envDefault:"300s"`
	ClientPingInterval time.Duration `env:"CLIENT_PING_INTERVAL" envDefault:"120s"`

	BouncerPassword              string        `env:"BOUNCER_PASSWORD"`
	BouncerHost                  string        `env:"BOUNCER_HOST" envDefault:"127.0.0.1"`
	BouncerPort                  int           `env:"BOUNCER_PORT" envDefault:"31337"`
	BouncerRatelimitEnabled      bool          `env:"BOUNCER_RATELIMIT_ENABLED" envDefault:"true"`
	BouncerMaxFailedAttempts     int           `env:"BOUNCER_MAX_FAILED_ATTEMPTS" envDefault:"5"`
	BouncerFailureWindowSeconds  int           `env:"BOUNCER_FAILURE_WINDOW_SECONDS" envDefault:"60"`
	BouncerLockoutSeconds        int           `env:"BOUNCER_LOCKOUT_SECONDS" envDefault:"300"`

	CTCPAutoReply     bool `env:"CTCP_AUTO_REPLY" envDefault:"true"`
	CTCPMaxPerWindow  int  `env:"CTCP_MAX_PER_WINDOW" envDefault:"3"`
	CTCPWindowSeconds int  `env:"CTCP_WINDOW_SECONDS" envDefault:"30"`
}

// LoadSettings parses the TURBORG_IRC_* environment into a Settings.
// Missing required vars surface as ValidationError from caarlos0/env.
func LoadSettings() (*Settings, error) {
	s := &Settings{}
	if err := env.ParseWithOptions(s, env.Options{Prefix: "TURBORG_IRC_"}); err != nil {
		return nil, err
	}
	return s, nil
}

// NormalizedChannels returns Channels with a '#' prefix added where the
// caller omitted one. Matches Python normalized_channels().
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

// EffectiveUsername returns Username if set, otherwise Nick. Matches
// Python effective_username().
func (s *Settings) EffectiveUsername() string {
	if s.Username != "" {
		return s.Username
	}
	return s.Nick
}

// SASLEnabled is true when both credentials are present.
func (s *Settings) SASLEnabled() bool {
	return s.SASLUser != "" && s.SASLPassword != ""
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
	return nil
}
