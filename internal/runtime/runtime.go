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
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/turborg/turborg/internal/activity"
	"github.com/turborg/turborg/internal/agent"
	"github.com/turborg/turborg/internal/commands"
	"github.com/turborg/turborg/internal/config"
	"github.com/turborg/turborg/internal/connector/irc"
	"github.com/turborg/turborg/internal/llm"
	"github.com/turborg/turborg/internal/messages"
	"github.com/turborg/turborg/internal/messagesink"
	"github.com/turborg/turborg/internal/statepush"
	"github.com/turborg/turborg/internal/web"
)

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

	cmds, err := parseCommands(s.Commands)
	if err != nil {
		return nil, err
	}

	a := agent.NewWithPrefix(log, s.CommandPrefix)

	notifier := activity.New(s.ActivityURL, s.ActivityToken, log)
	ircConn := irc.New(ircCfg, log, a.Events)

	// State-webhook emitter (transport: PUT to STATE_WEBHOOK_URL, which the
	// sidecar mirrors to accounts-api). Mirrors authoritative per-connector
	// state whenever it changes; inert no-op when STATE_WEBHOOK_URL is unset.
	// Wired before WireCommon so the connector's change hooks are observed from
	// boot. This is the dedicated transport; the pooled runtime POSTs directly
	// to the control plane instead.
	stateClient := statepush.NewClient(s.StateWebhookURL, s.StateWebhookToken, log)
	stateEmitter := statepush.NewEmitter(
		stateClient,
		buildIRCSnapshot(ircConn),
		time.Duration(s.StateWebhookDebounceMs)*time.Millisecond,
		log,
	)
	wireStatePushEmitter(ircConn, stateEmitter)

	// Shared message store, built before WireCommon so the connector's bouncer
	// and the gateway see the same instance.
	store, sink := buildMessageStore(s, log)
	_ = sink // referenced for lifecycle parity; closing happens with the agent

	// Dedicated activity transport: a per-event POST to the local sidecar via
	// the Notifier. Only wired when configured; the pooled runtime supplies its
	// own coalescing hook instead (per-event POSTs to the control plane don't
	// scale across a pool).
	var activityHook func(string)
	if notifier.Enabled() {
		activityHook = notifier.Hook
	}

	// The connector-agnostic, transport-independent wiring — builtins, owner
	// guard, throttles, nudge, store submitters. The pooled runtime calls this
	// same WireCommon from its tenant builder, so the two modes can't drift.
	if err := WireCommon(a, ircConn, CommonParams{
		CustomCommandsMax: s.CustomCommandsMax,
		Commands:          cmds,
		Limits: irc.ClientLimits{
			NickLocked:             s.NickLocked,
			RealnameLocked:         s.RealnameLocked,
			MaxChannels:            s.MaxChannels,
			TBSummarizeMaxMessages: s.TBSummarizeMaxMessages,
		},
		Owner: GuardParams{
			OwnerMode:            s.OwnerMode,
			OwnerNick:            s.OwnerNick,
			OwnerAccount:         s.OwnerAccount,
			OwnerHostmask:        s.OwnerHostmask,
			IgnoredNicks:         s.IgnoredNicks,
			BotNick:              ircCfg.Nick,
			CommandMaxPerWindow:  s.CommandMaxPerWindow,
			CommandWindowSeconds: s.CommandWindowSeconds,
		},
		OutboundMaxPerWindow:      s.OutboundMaxPerWindow,
		OutboundWindowSeconds:     s.OutboundWindowSeconds,
		OwnerNick:                 s.OwnerNick,
		OwnerDMNudgeEvery:         s.OwnerDMNudgeEvery,
		BouncerWelcomeReplayDepth: ircCfg.BouncerWelcomeReplayDepth,
		LLM:                       provider,
		ActivityHook:              activityHook,
		Store:                     store,
	}, log); err != nil {
		return nil, err
	}

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

	built := &Built{
		Agent:     a,
		IRC:       ircConn,
		LLM:       provider,
		Activity:  notifier,
		StatePush: stateEmitter,
	}

	if s.GatewayEnabled() {
		gw, err := buildGateway(s, ircConn, a, log, notifier, store, provider)
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

// buildLLM mints the agent's LLM provider from settings. The unified LLM
// router config (TURBORG_LLM_*) takes precedence when a provider kind or
// API key is set; otherwise it falls back to the legacy TURBORG_ANTHROPIC_*
// envs so existing deployments keep working unchanged.
func buildLLM(s *config.Settings) (llm.Provider, error) {
	if s.LLMProvider != "" || s.LLMAPIKey != "" {
		return BuildLLMProvider(s.LLMProvider, s.LLMBaseURL, s.LLMAPIKey, s.LLMModel)
	}
	return NewAnthropicProvider(s.AnthropicAPIKey, s.AnthropicModel)
}

// parseCommands decodes the TURBORG_COMMANDS JSON array into command
// definitions. Empty input is not an error — it just means no commands.
func parseCommands(raw string) ([]commands.Definition, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, nil
	}
	var defs []commands.Definition
	if err := json.Unmarshal([]byte(raw), &defs); err != nil {
		return nil, fmt.Errorf("runtime: parsing TURBORG_COMMANDS: %w", err)
	}
	return defs, nil
}

func buildGateway(s *config.Settings, ircConn *irc.Connector, a *agent.Agent, log *slog.Logger, notifier *activity.Notifier, store messages.Store, llmProvider llm.Provider) (*web.Gateway, error) {
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
		LLMProvider:  llmProvider,
		TBSummarizeMaxMessages: s.TBSummarizeMaxMessages,
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
// EventMessage / EventMessageSent into the shared store. DMs land
// alongside channel messages — the row's `channel` field carries the
// peer's nick for the DM case (set by handlePrivmsg's IsDirect branch)
// so each conversation has its own scrollback bucket.
//
// botNick is consulted when the event payload doesn't carry an explicit
// sender — specifically EventMessageSent from agent.handle, where the
// OutboundEnvelope has no Sender field (bot identity is implicit). The
// receiving side validates nick as non-empty, so without this fallback
// command replies (!ping → pong) silently 422'd at accounts-api and
// never landed in the durable history. The callback shape lets us
// re-resolve the nick on every event — handy when the bot renames
// mid-session (NICK changes); avoids stamping the original boot-time
// nick onto every later reply.
func makeStoreSubmitter(store messages.Store, botNick func() string, log *slog.Logger) func(ctx context.Context, ev *agent.Event) {
	return func(ctx context.Context, ev *agent.Event) {
		channel, _ := ev.Fields["channel"].(string)
		nick, _ := ev.Fields["sender"].(string)
		text, _ := ev.Fields["text"].(string)
		// MESSAGE_SENT from agent command-dispatch carries the
		// envelope, not the explicit fields — peek for both shapes.
		isOutbound := false
		if env, ok := ev.Fields["envelope"].(*agent.OutboundEnvelope); ok && env != nil {
			isOutbound = true
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
		// Outbound messages don't carry a sender — fall back to the
		// bot's own current nick so the row attributes correctly.
		if nick == "" && isOutbound && botNick != nil {
			nick = botNick()
		}
		// Drop only when we lack the minimum fields to write a valid
		// row. The receiver's validator requires non-empty channel +
		// nick + text; persisting with any of those blank would 422
		// silently. Channel may be a sigil-prefixed channel name OR a
		// nick (DM target) — both are legitimate scrollback buckets.
		if channel == "" || nick == "" || text == "" {
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

// BuildCommandGuard composes the owner-trust check and per-sender
// throttle into a single CommandGuard. Returns nil when the configured
// owner mode is "none" AND no throttle is configured — the command
// registry then skips the guard entirely.
//
// Three owner modes are supported. The default is "none" — !commands
// are inert until the operator opts in.
//
//   - "none":     refuse every !command. Bots running as pure relays,
//                 log-only agents, or personal-bouncer use cases sit
//                 here. Throttle still runs against anonymous scope so
//                 a misconfigured throttle gate doesn't silently allow.
//   - "self":     trust messages where the sender's nick equals the
//                 bot's own nick. Useful for personal AI assistants
//                 where the operator attaches via the bouncer and IS
//                 the bot. The IRC network won't allow nick collisions,
//                 so the sender check is sufficient on its own.
//   - "external": trust messages from OwnerNick, verified via the
//                 IRCv3 account-tag. OwnerAccount overrides the
//                 expected account name (defaults to OwnerNick). When
//                 the network has no services and no account-tag
//                 arrives, fall back to hostmask matching against
//                 OwnerHostmask when it's set. Without either, deny.
//
// Owner checks fail closed: ambiguous signals never resolve to "trust".
// This matters most precisely when the verification pipeline is
// degraded — a missing account-tag must not become a free pass.
func BuildCommandGuard(s *config.Settings, ircCfg *irc.Settings) agent.CommandGuard {
	var botNick string
	if ircCfg != nil {
		botNick = ircCfg.Nick
	}
	return BuildCommandGuardFromParams(GuardParams{
		OwnerMode:            s.OwnerMode,
		OwnerNick:            s.OwnerNick,
		OwnerAccount:         s.OwnerAccount,
		OwnerHostmask:        s.OwnerHostmask,
		IgnoredNicks:         s.IgnoredNicks,
		BotNick:              botNick,
		CommandMaxPerWindow:  s.CommandMaxPerWindow,
		CommandWindowSeconds: s.CommandWindowSeconds,
	})
}

// buildIgnoredSet normalises the configured ignore list into a fast
// lookup set. Lowercase + trim + drop empties. Returns nil when no
// usable entries — callers short-circuit on the nil set so the no-op
// case stays branch-free.
func buildIgnoredSet(raw []string) map[string]struct{} {
	if len(raw) == 0 {
		return nil
	}
	out := make(map[string]struct{}, len(raw))
	for _, n := range raw {
		lower := strings.ToLower(strings.TrimSpace(n))
		if lower == "" {
			continue
		}
		out[lower] = struct{}{}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// isIgnoredSender returns true when the envelope's sender nick is in
// the user-configured ignore set. Nil set (no ignores configured) is
// the hot-path: returns false without allocating.
func isIgnoredSender(set map[string]struct{}, env *agent.InboundEnvelope) bool {
	if set == nil || env == nil {
		return false
	}
	sender := strings.ToLower(strings.TrimSpace(env.Sender))
	if sender == "" {
		return false
	}
	_, hit := set[sender]
	return hit
}

// ownerCheck applies the per-mode identity test. Returns true only when
// the inbound envelope is from a trusted operator under the configured
// mode; otherwise false. Fails closed on missing / ambiguous signals.
func ownerCheck(env *agent.InboundEnvelope, mode, botNick, ownerNick, ownerAccount, ownerHostmask string) bool {
	switch mode {
	case "self":
		sender := strings.ToLower(env.Sender)
		return botNick != "" && sender == botNick
	case "external":
		sender := strings.ToLower(env.Sender)
		if ownerNick == "" || sender != ownerNick {
			return false
		}
		return verifyExternalOwner(env, ownerAccount, ownerHostmask)
	default:
		// "none" and any unknown value (operator typo) — deny.
		return false
	}
}

// verifyExternalOwner runs the account-tag → hostmask cascade. account-
// tag wins when present (the strongest signal); hostmask is the
// services-less fallback. Without either, deny.
func verifyExternalOwner(env *agent.InboundEnvelope, ownerAccount, ownerHostmask string) bool {
	account, _ := env.Metadata["account"].(string)
	if account != "" {
		// Lowercase compare — see ownerCheck for why.
		return strings.ToLower(account) == ownerAccount
	}
	if ownerHostmask == "" {
		return false
	}
	prefix, _ := env.Metadata["prefix"].(string)
	return hostmaskMatches(strings.ToLower(prefix), ownerHostmask)
}

// throttleScope picks the bucket key for the per-sender throttle.
// Prefers the IRCv3 account-tag (stable across nick changes); falls
// back to the inbound sender; finally a shared "anon" bucket for the
// degenerate case where neither was set.
func throttleScope(env *agent.InboundEnvelope) string {
	if scope, _ := env.Metadata["account"].(string); scope != "" {
		return scope
	}
	if sender := strings.ToLower(env.Sender); sender != "" {
		return sender
	}
	return "anon"
}

// hostmaskMatches tests whether an inbound IRC prefix (nick!ident@host,
// already lowercased) matches a hostmask pattern with `*` glob
// wildcards. The pattern is operator-supplied so we keep the surface
// tiny: only `*` is interpreted (matches any sequence including empty).
// Anything else is a literal byte match. Both sides are case-folded by
// the caller — IRC hostnames are case-insensitive.
func hostmaskMatches(prefix, pattern string) bool {
	if pattern == "" {
		return false
	}
	// Strip the leading ':' some IRCD parsers include.
	prefix = strings.TrimPrefix(prefix, ":")
	parts := strings.Split(pattern, "*")
	// No wildcards → literal compare.
	if len(parts) == 1 {
		return prefix == pattern
	}
	// First fragment must prefix-match.
	if !strings.HasPrefix(prefix, parts[0]) {
		return false
	}
	prefix = prefix[len(parts[0]):]
	// Walk middle fragments left-to-right.
	for i := 1; i < len(parts)-1; i++ {
		idx := strings.Index(prefix, parts[i])
		if idx < 0 {
			return false
		}
		prefix = prefix[idx+len(parts[i]):]
	}
	// Last fragment must suffix-match what's left.
	return strings.HasSuffix(prefix, parts[len(parts)-1])
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
