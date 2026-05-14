// Package runtime composes a runnable Agent + (optional) Web gateway
// from environment-derived settings. The CLI calls Build, but the same
// functions are exposed for embedders (tests, alternate front-ends).
//
// Behavior matches Python core/runtime.py:
//   - Single-IRC quickstart path: when TURBORG_CONNECTORS is unset,
//     BuildAgent wires one IRC connector + builtins.
//   - Multi-connector path: when TURBORG_CONNECTORS=irc[,…] is set,
//     BuildMultiConnectorAgent wires every listed connector.
//   - Anthropic provider only attaches when TURBORG_ANTHROPIC_API_KEY
//     is present — the agent never fails for lack of one.
//   - Web gateway only attaches when TURBORG_IRC_WEB_PASSWORD is set.
package runtime

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/turborg/turborg/internal/agent"
	"github.com/turborg/turborg/internal/config"
	"github.com/turborg/turborg/internal/connector/irc"
	"github.com/turborg/turborg/internal/llm"
	"github.com/turborg/turborg/internal/llm/anthropic"
	"github.com/turborg/turborg/internal/version"
	"github.com/turborg/turborg/internal/web"
)

// AskSystemPrompt is the system prompt used for the built-in !ask
// command. Kept as a const so prompt-caching's prefix match stays
// stable across calls.
const AskSystemPrompt = "You are turborg, an IRC chatbot. Keep replies short and conversational — " +
	"most IRC clients show one line at a time. Avoid markdown."

// Built composes a fully-wired Agent (and optionally a Web gateway)
// from the given settings. The IRC connector and any other configured
// connectors are added; built-in commands are registered; the owner +
// throttle guard is installed.
type Built struct {
	Agent   *agent.Agent
	IRC     *irc.Connector
	Gateway *web.Gateway
	LLM     llm.Provider // nil when Anthropic is not configured
}

// Build wires the agent + connectors + gateway from settings. Callers
// run with `built.Agent.Run(ctx)` (and `built.Gateway.Serve(ctx)` when
// non-nil) — see the CLI for the mutually-stopping pair.
func Build(s *config.Settings, ircCfg *irc.Settings, log *slog.Logger) (*Built, error) {
	if log == nil {
		log = slog.Default()
	}

	provider, err := buildLLM(s)
	if err != nil {
		return nil, err
	}

	a := agent.NewWithPrefix(log, s.CommandPrefix)

	ircConn := irc.New(ircCfg, log, a.Events)
	a.AddConnector(ircConn)

	if len(s.Connectors) > 1 {
		for _, name := range s.Connectors {
			if name == "irc" {
				continue
			}
			// Future arms land here. Closed-set validation in config
			// ensures only known names reach this point.
			return nil, fmt.Errorf("runtime: connector %q listed in TURBORG_CONNECTORS but not yet implemented in Go", name)
		}
	}

	RegisterBuiltinCommands(a, provider)
	a.Commands.SetGuard(BuildCommandGuard(s))

	built := &Built{Agent: a, IRC: ircConn, LLM: provider}

	if ircCfg.WebEnabled() {
		gw, err := buildGateway(ircCfg, ircConn, log)
		if err != nil {
			return nil, err
		}
		gw.Subscribe(a.Events)
		built.Gateway = gw
	}

	return built, nil
}

func buildLLM(s *config.Settings) (llm.Provider, error) {
	if !s.AnthropicEnabled() {
		return nil, nil //nolint:nilnil // explicit "no provider" signal
	}
	p, err := anthropic.New(anthropic.Settings{
		APIKey:            s.AnthropicAPIKey,
		Model:             s.AnthropicModel,
		CacheSystemPrompt: true,
	})
	if err != nil {
		return nil, fmt.Errorf("runtime: anthropic: %w", err)
	}
	return p, nil
}

func buildGateway(ircCfg *irc.Settings, ircConn *irc.Connector, log *slog.Logger) (*web.Gateway, error) {
	verifier, err := web.NewStaticPasswordVerifier(ircCfg.WebPassword)
	if err != nil {
		return nil, fmt.Errorf("runtime: web verifier: %w", err)
	}
	rl, err := irc.NewRateLimiter(
		ircCfg.WebMaxFailedAttempts,
		time.Duration(ircCfg.WebFailureWindowSeconds)*time.Second,
		time.Duration(ircCfg.WebLockoutSeconds)*time.Second,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("runtime: web ratelimit: %w", err)
	}
	opts := web.Options{
		Host:        ircCfg.WebHost,
		Port:        ircCfg.WebPort,
		Verifier:    verifier,
		RateLimiter: rl,
		Log:         log,
	}
	if ircCfg.IdleShutdownEnabled() {
		opts.IdleShutdownSeconds = ircCfg.WebIdleShutdownSeconds
		// Idle callback wired by the CLI — it needs the cancel func that
		// stops both halves. Runtime can't supply it without knowing the
		// CLI's ctx. Leaving OnIdleShutdown nil here means the gateway
		// logs and no-ops; the CLI installs the real callback after Build.
	}
	gw, err := web.New(ircConn, ircConn, opts)
	if err != nil {
		return nil, err
	}
	return gw, nil
}

// RegisterBuiltinCommands installs ping, version, help, and (when an
// LLM is configured) ask. Idempotent for the same registry — duplicate
// names just overwrite the prior handler. Builtins call ReplyTo so
// DM-routing works out of the box.
func RegisterBuiltinCommands(a *agent.Agent, provider llm.Provider) {
	a.Commands.Register("version", versionCmd, nil)
	if provider != nil {
		a.Commands.Register("ask", askCmd(provider, a.Log()), nil)
	}
}

func versionCmd(_ context.Context, env *agent.InboundEnvelope, _ []string) (*agent.OutboundEnvelope, error) {
	return agent.ReplyTo(env, "turborg "+version.Version), nil
}

func askCmd(provider llm.Provider, log *slog.Logger) agent.CommandHandler {
	return func(ctx context.Context, env *agent.InboundEnvelope, args []string) (*agent.OutboundEnvelope, error) {
		question := strings.TrimSpace(strings.Join(args, " "))
		if question == "" {
			return agent.ReplyTo(env, "usage: !ask <question>"), nil
		}
		answer, err := provider.Ask(ctx, question,
			llm.WithSystem(AskSystemPrompt),
			llm.WithMaxTokens(512),
		)
		if err != nil {
			log.Warn("ask failed", "err", err)
			return agent.ReplyTo(env, "sorry, that broke: "+err.Error()), nil
		}
		return agent.ReplyTo(env, collapseWhitespace(answer)), nil
	}
}

func collapseWhitespace(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

// BuildCommandGuard composes the owner-only + per-sender throttle into
// a single CommandGuard. Returns nil when neither owner check nor
// throttle is configured — the registry then skips the call entirely.
//
// Owner checks fail closed when an account tag is required but missing
// (e.g. a services-less network or a client without the account-tag
// capability). This is the security stance the Python implementation
// converged on after the deferred-tag bug.
func BuildCommandGuard(s *config.Settings) agent.CommandGuard {
	ownerNick := strings.ToLower(strings.TrimSpace(s.OwnerNick))
	ownerAccount := strings.TrimSpace(s.OwnerAccount)

	var throttle *irc.Throttle
	if s.CommandMaxPerWindow > 0 && s.CommandWindowSeconds > 0 {
		t, err := irc.NewThrottle(
			s.CommandMaxPerWindow,
			time.Duration(s.CommandWindowSeconds)*time.Second,
			nil,
		)
		if err == nil {
			throttle = t
		}
	}

	if ownerNick == "" && ownerAccount == "" && throttle == nil {
		return nil
	}

	return func(env *agent.InboundEnvelope) bool {
		sender := strings.ToLower(env.Sender)
		account, _ := env.Metadata["account"].(string)
		if ownerAccount != "" && account != ownerAccount {
			return false
		}
		if ownerNick != "" && sender != ownerNick {
			return false
		}
		scope := account
		if scope == "" {
			scope = sender
		}
		if scope == "" {
			scope = "anon"
		}
		if throttle != nil {
			return throttle.Allow(scope)
		}
		return true
	}
}

// LoadIRCSettings wraps irc.LoadSettings with a helpful error that
// names the TURBORG_IRC_ prefix the user is expected to set.
func LoadIRCSettings() (*irc.Settings, error) {
	s, err := irc.LoadSettings()
	if err != nil {
		return nil, fmt.Errorf("runtime: loading TURBORG_IRC_* settings: %w", err)
	}
	if err := s.Validate(); err != nil {
		return nil, err
	}
	return s, nil
}

// Run is the mutually-stopping pair used by the CLI. Either side
// returning (cleanly or with an error) cancels the shared context, so
// the other unwinds in milliseconds. Returns the first non-nil error.
func Run(ctx context.Context, b *Built) error {
	if b.Gateway == nil {
		return b.Agent.Run(ctx)
	}

	type result struct {
		from string
		err  error
	}
	results := make(chan result, 2)
	rootCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		err := b.Agent.Run(rootCtx)
		results <- result{from: "agent", err: err}
	}()
	go func() {
		err := b.Gateway.Serve(rootCtx)
		results <- result{from: "gateway", err: err}
	}()

	// Cross-stop: whichever side returns first cancels the other.
	first := <-results
	cancel()
	b.Gateway.Stop()
	second := <-results

	if first.err != nil && !errors.Is(first.err, context.Canceled) {
		return fmt.Errorf("%s: %w", first.from, first.err)
	}
	if second.err != nil && !errors.Is(second.err, context.Canceled) {
		return fmt.Errorf("%s: %w", second.from, second.err)
	}
	return nil
}
