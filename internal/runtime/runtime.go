// Package runtime composes a runnable Agent + (optional) Web gateway
// from environment-derived settings. The CLI calls Build, but the same
// functions are exposed for embedders (tests, alternate front-ends).
//
// Wiring rules:
//   - Single-IRC quickstart path: when TURBORG_CONNECTORS is unset,
//     Build wires one IRC connector + builtins.
//   - Multi-connector path: when TURBORG_CONNECTORS=irc[,…] is set,
//     Build wires every listed connector.
//   - Anthropic provider only attaches when TURBORG_ANTHROPIC_API_KEY
//     is present — the agent never fails for lack of one.
//   - Gateway only attaches when TURBORG_GATEWAY_PASSWORD is set.
package runtime

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/turborg/turborg/internal/activity"
	"github.com/turborg/turborg/internal/agent"
	"github.com/turborg/turborg/internal/config"
	"github.com/turborg/turborg/internal/connector/irc"
	"github.com/turborg/turborg/internal/llm"
	"github.com/turborg/turborg/internal/llm/anthropic"
	"github.com/turborg/turborg/internal/messages"
	"github.com/turborg/turborg/internal/messagesink"
	"github.com/turborg/turborg/internal/statepush"
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
	Agent     *agent.Agent
	IRC       *irc.Connector
	Gateway   *web.Gateway
	LLM       llm.Provider       // nil when Anthropic is not configured
	Activity  *activity.Notifier // never nil; no-op when ACTIVITY_URL is unset
	StatePush *statepush.Emitter // never nil; inert no-op when STATE_WEBHOOK_URL is unset
}

// Build wires the agent + connectors + gateway from settings. Callers
// run with `built.Agent.Run(ctx)` (and `built.Gateway.Serve(ctx)` when
// non-nil) — see the CLI for the mutually-stopping pair.
func Build(s *config.Settings, ircCfg *irc.Settings, log *slog.Logger) (*Built, error) {
	if log == nil {
		log = slog.Default()
	}

	if err := applyOperatorPolicy(s, ircCfg); err != nil {
		return nil, err
	}

	provider, err := buildLLM(s)
	if err != nil {
		return nil, err
	}

	a := agent.NewWithPrefix(log, s.CommandPrefix)
	a.Commands.SetMaxDynamic(s.CustomCommandsMax)

	notifier := activity.New(s.ActivityURL, s.ActivityToken, log)

	ircConn := irc.New(ircCfg, log, a.Events)
	ircConn.SetClientLimits(irc.ClientLimits{
		NickLocked:     s.NickLocked,
		RealnameLocked: s.RealnameLocked,
		MaxChannels:    s.MaxChannels,
	})
	if notifier.Enabled() {
		// Bind the notifier into the connector + bouncer. The Hook method
		// keeps the IRC package free of an activity-package import — it
		// only sees a func(string).
		ircConn.SetActivityHook(notifier.Hook)
		ircConn.SetBouncerAttachHook(notifier.Hook)
	}

	// Per-target outbound throttle, when configured. Single instance
	// shared between the bouncer (consults for attached-client PRIVMSG)
	// and the WS gateway (consults for `say` op) so a user with both
	// surfaces open shares one bucket per target.
	if s.OutboundMaxPerWindow > 0 && s.OutboundWindowSeconds > 0 {
		t, err := irc.NewThrottle(
			s.OutboundMaxPerWindow,
			time.Duration(s.OutboundWindowSeconds)*time.Second,
			nil,
		)
		if err != nil {
			return nil, fmt.Errorf("runtime: outbound throttle: %w", err)
		}
		ircConn.SetOutboundThrottle(t)
	}

	// Owner-DM nudge: when the operator has set both a target owner nick
	// and a positive interval, the connector DMs the owner every N
	// outbound PRIVMSGs with a usage summary. Daily counter resets at
	// UTC midnight inside the nudge itself.
	if nudge := irc.NewOwnerNudge(s.OwnerNick, s.OwnerDMNudgeEvery); nudge != nil {
		ircConn.SetOwnerNudge(nudge)
	}

	// State-webhook emitter. Mirrors authoritative per-connector
	// state (current connection status + joined channels + preferred
	// nick) to the configured STATE_WEBHOOK_URL endpoint whenever
	// that state changes. When STATE_WEBHOOK_URL is unset the
	// emitter is an inert no-op (no goroutine, no PUTs); the
	// snapshot builder closure is harmless either way.
	stateClient := statepush.NewClient(s.StateWebhookURL, s.StateWebhookToken, log)
	stateEmitter := statepush.NewEmitter(
		stateClient,
		buildIRCSnapshot(ircConn),
		time.Duration(s.StateWebhookDebounceMs)*time.Millisecond,
		log,
	)
	wireStatePushEmitter(ircConn, stateEmitter)

	// Build the shared messages.Store before connectors register
	// for events, so the IRC connector's bouncer + the gateway both
	// see the same store. The store also picks up Submit calls from
	// the EventBus subscriber wired further down.
	store, sink := buildMessageStore(s, log)
	ircConn.SetMessageStore(store)
	ircConn.SetBouncerWelcomeReplayDepth(clampReplayDepth(ircCfg.BouncerWelcomeReplayDepth))

	a.AddConnector(ircConn)

	// Single EventBus subscriber feeds the store for every channel
	// message the agent observes. Filters at submit-time: only
	// channel-sigil targets count (DMs don't enter replay history).
	if store != nil {
		a.Events.Subscribe(agent.EventMessage, makeStoreSubmitter(store, log))
		a.Events.Subscribe(agent.EventMessageSent, makeStoreSubmitter(store, log))
	}
	_ = sink // referenced for lifecycle parity; closing happens with the agent

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

	built := &Built{
		Agent:     a,
		IRC:       ircConn,
		LLM:       provider,
		Activity:  notifier,
		StatePush: stateEmitter,
	}

	if s.GatewayEnabled() {
		gw, err := buildGateway(s, ircConn, a, log, notifier, store)
		if err != nil {
			return nil, err
		}
		gw.Subscribe(a.Events)
		built.Gateway = gw
	}

	return built, nil
}

// applyOperatorPolicy runs cross-cutting policy checks at boot. Catches
// misconfigurations the env layer alone can't see — e.g. ALLOWED_NETWORKS
// set but IRC_HOSTNAME points outside the allowlist. Mutates ircCfg in
// place when the policy forces a value (realname lock).
//
// All checks are no-ops when the corresponding policy env is unset, so
// operators who don't configure anything see no behavior change.
func applyOperatorPolicy(s *config.Settings, ircCfg *irc.Settings) error {
	if ircCfg != nil && !s.HostnameAllowed(ircCfg.Hostname) {
		return fmt.Errorf("runtime: IRC hostname %q is not in TURBORG_ALLOWED_NETWORKS=%s",
			ircCfg.Hostname, strings.Join(s.AllowedNetworks, ","))
	}

	// Realname lock: template overrides whatever value arrived via
	// TURBORG_IRC_REAL_NAME. Done before the connector is constructed so
	// the constructor sees the locked value as its initial state.
	if ircCfg != nil && s.RealnameLocked && s.RealnameTemplate != "" {
		ircCfg.RealName = s.RealnameTemplate
	}

	return nil
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

func buildGateway(s *config.Settings, ircConn *irc.Connector, a *agent.Agent, log *slog.Logger, notifier *activity.Notifier, store messages.Store) (*web.Gateway, error) {
	verifier, err := web.NewStaticPasswordVerifier(s.GatewayPassword)
	if err != nil {
		return nil, fmt.Errorf("runtime: gateway verifier: %w", err)
	}
	rl, err := irc.NewRateLimiter(
		s.GatewayMaxFailedAttempts,
		time.Duration(s.GatewayFailureWindowSeconds)*time.Second,
		time.Duration(s.GatewayLockoutSeconds)*time.Second,
		nil,
	)
	if err != nil {
		return nil, fmt.Errorf("runtime: gateway ratelimit: %w", err)
	}
	opts := web.Options{
		Host:         s.GatewayHost,
		Port:         s.GatewayPort,
		Verifier:     verifier,
		RateLimiter:  rl,
		Log:          log,
		MessageStore: store,
	}
	if notifier.Enabled() {
		opts.OnClientAttached = notifier.Hook
	}
	if s.IdleShutdownEnabled() {
		opts.IdleShutdownSeconds = s.GatewayIdleShutdownSeconds
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

// buildMessageStore picks the right Store implementation from
// settings. Returns the chosen Store plus the underlying sink (if
// any) so the caller can own its lifecycle.
//
//   - MESSAGE_STORE_URL + MESSAGE_SINK_URL both set → HTTPStore
//     (durable read + write through accounts-api).
//   - MESSAGE_SINK_URL set, MESSAGE_STORE_URL unset → MemoryStore for
//     reads but writes still mirror through the sink (legacy half).
//   - Neither set → MemoryStore only (self-host default).
func buildMessageStore(s *config.Settings, log *slog.Logger) (messages.Store, *messagesink.Sink) {
	if log == nil {
		log = slog.Default()
	}
	sink := messagesink.New(s.MessageSinkURL, s.MessageSinkToken, log)
	if sink != nil {
		log.Info("message sink enabled", "endpoint", s.MessageSinkURL)
	}
	if hs := messages.NewHTTPStore(s.MessageStoreURL, s.MessageStoreToken, sink, log); hs != nil {
		log.Info("message store enabled (HTTP)", "endpoint", s.MessageStoreURL)
		return hs, sink
	}
	// Fall back to in-process. When a sink IS configured but no store
	// URL, the MemoryStore still serves attach replay + scrollback
	// locally; the sink keeps mirroring writes for whatever consumer
	// runs on the other side.
	return messages.NewMemoryStore(0), sink
}

// makeStoreSubmitter returns an EventBus handler that mirrors every
// channel-targeted EventMessage / EventMessageSent into the shared
// store. DMs are filtered out at this seam (channel must start with
// a channel sigil) so replay history stays channel-only.
func makeStoreSubmitter(store messages.Store, log *slog.Logger) func(ctx context.Context, ev *agent.Event) {
	return func(ctx context.Context, ev *agent.Event) {
		channel, _ := ev.Fields["channel"].(string)
		nick, _ := ev.Fields["sender"].(string)
		text, _ := ev.Fields["text"].(string)
		// MESSAGE_SENT from agent command-dispatch carries the
		// envelope, not the explicit fields — peek for both shapes.
		if env, ok := ev.Fields["envelope"].(*agent.OutboundEnvelope); ok && env != nil {
			if channel == "" {
				channel = env.Channel
			}
			if text == "" {
				text = env.Text
			}
		}
		if env, ok := ev.Fields["envelope"].(*agent.InboundEnvelope); ok && env != nil {
			if channel == "" {
				channel = env.Channel
			}
			if nick == "" {
				nick = env.Sender
			}
			if text == "" {
				text = env.Text
			}
		}
		if channel == "" || !isChannelTarget(channel) {
			return
		}
		if err := store.Submit(ctx, messages.Message{
			Channel: channel,
			Nick:    nick,
			Text:    text,
			TS:      time.Now(),
		}); err != nil {
			log.Debug("store submit", "err", err, "channel", channel)
		}
	}
}

// clampReplayDepth keeps the operator's TURBORG_IRC_BOUNCER_WELCOME_
// REPLAY_DEPTH inside a sane window. 1..2000 are accepted; out-of-band
// values fall back to the package default rather than rejecting boot.
func clampReplayDepth(n int) int {
	const def = 200
	const max = 2000
	if n < 1 {
		return def
	}
	if n > max {
		return max
	}
	return n
}

func isChannelTarget(s string) bool {
	if s == "" {
		return false
	}
	switch s[0] {
	case '#', '&', '+', '!':
		return true
	}
	return false
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
// capability). Failing open would let any nick spoof the owner the
// moment the account-tag pipeline went unavailable, which is exactly
// when defensive gating matters most.
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

// buildIRCSnapshot returns a snapshot builder closed over ircConn. The
// builder is called by the state-webhook emitter each time the
// debounce window fires; the value reflects the connector's
// authoritative state at that moment.
//
// Nick precedence: the queued preferred-nick wins when non-empty (it
// represents the user's stated intent for the next registration);
// fall through to the live current-nick otherwise.
func buildIRCSnapshot(ircConn *irc.Connector) statepush.SnapshotBuilder {
	return func() statepush.Snapshot {
		machine := ircConn.UpstreamState()
		nick := ircConn.PreferredNick()
		if nick == "" {
			nick = ircConn.CurrentNick()
		}
		wanted := ircConn.WantedChannels().Snapshot()
		channels := make([]statepush.ChannelSnapshot, 0, len(wanted))
		for _, w := range wanted {
			channels = append(channels, statepush.NewChannelSnapshot(w.Name, w.Key))
		}
		return statepush.Snapshot{
			Connectors: map[string]statepush.ConnectorSnapshot{
				ircConn.Name(): {
					State:    string(machine.State()),
					Since:    machine.EnteredAt().UTC(),
					Channels: channels,
					Nick:     nick,
				},
			},
		}
	}
}

// wireStatePushEmitter attaches the emitter's NotifyChange to the
// three authoritative-state sources on the IRC connector: state-
// machine transitions, wanted-channels mutations, and preferred-nick
// changes. Safe to call even when the emitter is inert (no-op
// NotifyChange is fine, and the subscriptions register but never
// fire anything useful).
func wireStatePushEmitter(ircConn *irc.Connector, emitter *statepush.Emitter) {
	if ircConn == nil || emitter == nil {
		return
	}
	notify := emitter.NotifyChange
	ircConn.UpstreamState().Subscribe(func(irc.UpstreamStateChange) { notify() })
	ircConn.WantedChannels().SetOnChange(notify)
	ircConn.SetPreferredNickChangeHook(notify)
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
	if b.StatePush != nil {
		defer b.StatePush.Stop()
	}
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
