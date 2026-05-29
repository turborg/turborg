package runtime

import (
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/turborg/turborg/internal/agent"
	"github.com/turborg/turborg/internal/commands"
	"github.com/turborg/turborg/internal/connector/irc"
	"github.com/turborg/turborg/internal/llm"
	"github.com/turborg/turborg/internal/llm/anthropic"
	"github.com/turborg/turborg/internal/llm/openaicompat"
	"github.com/turborg/turborg/internal/messages"
)

// CommonParams are the per-tenant inputs the shared agent wiring consumes,
// independent of where they were sourced (env for the dedicated CLI, the
// tenant feed for the pooled runtime) and independent of transport (port-bound
// vs listenerless bouncer/gateway, sidecar-PUT vs control-plane-POST state).
//
// It carries the wiring that historically drifted between the two runtimes:
// the dedicated path built it in runtime.Build, the pooled path hand-rolled a
// subset in server.Tenant.buildConnectors, so connector features wired in one
// place silently went missing in the other. WireCommon is now the single
// owner of all of it.
type CommonParams struct {
	// CustomCommandsMax caps the dynamic-command registry (0 = builtins only).
	CustomCommandsMax int

	// Limits pins identity + channel ceilings on the connector.
	Limits irc.ClientLimits

	// Owner is the !command trust policy (owner mode + per-sender throttle).
	Owner GuardParams

	// Per-target outbound throttle. Both > 0 to enable.
	OutboundMaxPerWindow  int
	OutboundWindowSeconds int

	// Owner-DM nudge: DM the owner every N outbound PRIVMSGs. Both fields must
	// be set (non-empty nick + positive interval) to enable.
	OwnerNick         string
	OwnerDMNudgeEvery int

	// BouncerWelcomeReplayDepth is clamped to a sane window before use.
	BouncerWelcomeReplayDepth int

	// Commands is the tenant's data-driven command set, swapped into the
	// registry via ReplaceDynamic. Empty → the agent dispatches nothing.
	Commands []commands.Definition

	// LLM provider powering LLM-type commands. Nil → those commands are
	// skipped at build time. Shared across pooled tenants (a stateless HTTP
	// client) and per-process for dedicated.
	LLM llm.Provider

	// ActivityHook fires on connector activity (bouncer attach, message
	// traffic). Nil → activity hooks are not attached. Dedicated passes its
	// Notifier.Hook (a per-event POST to the local sidecar); pooled passes a
	// hook that marks the tenant active in the pool's coalescing aggregator
	// (a per-event POST per tenant would hammer the control plane).
	ActivityHook func(reason string)

	// Store backs bouncer welcome replay + web-shell scrollback + CHATHISTORY.
	// Nil → no store wiring (no submitters, connector keeps its zero value).
	Store messages.Store
}

// GuardParams are the primitive inputs to the !command guard, lifted out of
// config.Settings so the pooled runtime (which has no config.Settings) can
// build the identical guard from tenant-spec fields.
type GuardParams struct {
	OwnerMode     string
	OwnerNick     string
	OwnerAccount  string
	OwnerHostmask string
	IgnoredNicks  []string
	// BotNick is the connector's own nick (the "self" owner-mode anchor).
	BotNick              string
	CommandMaxPerWindow  int
	CommandWindowSeconds int
}

// WireCommon installs the connector-agnostic agent wiring shared by the
// dedicated (cmd/turborg) and pooled (internal/server) runtimes onto an
// already-constructed agent + IRC connector: client limits, outbound throttle,
// owner nudge, message store + event submitters, builtin commands, and the
// command guard. It calls a.AddConnector itself.
//
// The caller owns everything that genuinely differs by mode — agent prefix,
// gateway (listener vs router), bouncer listener mode, state-push transport —
// and constructs the deps (LLM/activity/store) from its own config source.
//
// No process globals or shared mutable state: invoked once per dedicated
// process and N times per pooled process, each call producing a fully
// independent set of throttles/guards/submitters.
func WireCommon(a *agent.Agent, ircConn *irc.Connector, p CommonParams, log *slog.Logger) error {
	if log == nil {
		log = slog.Default()
	}

	a.Commands.SetMaxDynamic(p.CustomCommandsMax)
	ircConn.SetClientLimits(p.Limits)

	if p.ActivityHook != nil {
		ircConn.SetActivityHook(p.ActivityHook)
		ircConn.SetBouncerAttachHook(p.ActivityHook)
	}

	if p.OutboundMaxPerWindow > 0 && p.OutboundWindowSeconds > 0 {
		t, err := irc.NewThrottle(
			p.OutboundMaxPerWindow,
			time.Duration(p.OutboundWindowSeconds)*time.Second,
			nil,
		)
		if err != nil {
			return fmt.Errorf("runtime: outbound throttle: %w", err)
		}
		ircConn.SetOutboundThrottle(t)
	}

	if nudge := irc.NewOwnerNudge(p.OwnerNick, p.OwnerDMNudgeEvery); nudge != nil {
		ircConn.SetOwnerNudge(nudge)
	}

	if p.Store != nil {
		ircConn.SetMessageStore(p.Store)
	}
	ircConn.SetBouncerWelcomeReplayDepth(clampReplayDepth(p.BouncerWelcomeReplayDepth))

	a.AddConnector(ircConn)

	if p.Store != nil {
		botNick := ircConn.CurrentNick
		a.Events.Subscribe(agent.EventMessage, makeStoreSubmitter(p.Store, botNick, log))
		a.Events.Subscribe(agent.EventMessageSent, makeStoreSubmitter(p.Store, botNick, log))
	}

	// Registry-wide guard: the ignore list + per-sender throttle, applied to
	// every command. Per-command access (owner / allowlist / everyone) is
	// layered on top by the guard each Definition carries.
	a.Commands.SetGuard(buildRegistryGuard(p.Owner))

	// Install the tenant's data-driven commands. The dynamic set is fully
	// owned by ReplaceDynamic, so a later hot reload swaps it atomically.
	built := commands.Build(p.Commands, p.LLM, func(d commands.Definition) agent.CommandGuard {
		return PerCommandGuard(string(d.Access), d.Allowlist, p.Owner)
	}, log)
	a.Commands.ReplaceDynamic(built)
	return nil
}

// BuildLLMProvider mints an llm.Provider from a provider kind + endpoint +
// key + default model. It is the single dispatcher both runtimes use so the
// dedicated env loader and the pooled process select backends identically.
//
//   - "" / "anthropic": the Anthropic provider (back-compat default).
//   - "openai_compat":  any OpenAI-Chat-Completions-compatible backend
//     (the base URL selects which one).
//
// Returns (nil, nil) when no API key is configured — the agent never fails
// for lack of an LLM; LLM-type commands are simply skipped.
func BuildLLMProvider(kind, baseURL, apiKey, model string) (llm.Provider, error) {
	switch strings.ToLower(strings.TrimSpace(kind)) {
	case "", "anthropic":
		return NewAnthropicProvider(apiKey, model)
	case "openai_compat", "openai":
		if apiKey == "" {
			return nil, nil //nolint:nilnil // explicit "no provider" signal
		}
		p, err := openaicompat.New(openaicompat.Settings{
			APIKey:  apiKey,
			BaseURL: baseURL,
			Model:   model,
		})
		if err != nil {
			return nil, fmt.Errorf("runtime: openai-compatible provider: %w", err)
		}
		return p, nil
	default:
		return nil, fmt.Errorf("runtime: unknown LLM provider kind %q", kind)
	}
}

// NewAnthropicProvider builds the Anthropic LLM provider from an API key +
// model. Returns (nil, nil) when the key is empty — the agent never fails for
// lack of an LLM; !ask is simply not registered. Shared by the dedicated env
// loader and the pooled process so both mint the provider identically.
func NewAnthropicProvider(apiKey, model string) (llm.Provider, error) {
	if apiKey == "" {
		return nil, nil //nolint:nilnil // explicit "no provider" signal
	}
	p, err := anthropic.New(anthropic.Settings{
		APIKey:            apiKey,
		Model:             model,
		CacheSystemPrompt: true,
	})
	if err != nil {
		return nil, fmt.Errorf("runtime: anthropic: %w", err)
	}
	return p, nil
}

// BuildCommandGuardFromParams composes the owner-trust check and per-sender
// throttle into a single CommandGuard from primitive inputs. See
// BuildCommandGuard for the per-mode semantics; this is the same logic with
// its config.Settings dependency removed.
//
// It is retained as the legacy single-gate guard (ignore list + owner-trust
// + throttle) for embedders that want one owner-only gate across every
// command. The data-driven path instead layers buildRegistryGuard (ignore +
// throttle) under per-command PerCommandGuards (access policy), so different
// commands can carry different access.
func BuildCommandGuardFromParams(p GuardParams) agent.CommandGuard {
	base := buildRegistryGuard(p)
	owner := PerCommandGuard(string(commands.AccessOwner), nil, p)
	return func(env *agent.InboundEnvelope) bool {
		return base(env) && owner(env)
	}
}

// buildRegistryGuard returns the registry-wide guard: the ignore list and
// the per-sender command throttle, applied to every command regardless of
// its access policy. Per-command access is layered on top via the guard
// each command carries. The returned guard is always non-nil so the
// registry runs it (it allows when neither ignores nor a throttle are set).
func buildRegistryGuard(p GuardParams) agent.CommandGuard {
	ignoredSenders := buildIgnoredSet(p.IgnoredNicks)

	var throttle *irc.Throttle
	if p.CommandMaxPerWindow > 0 && p.CommandWindowSeconds > 0 {
		if t, err := irc.NewThrottle(
			p.CommandMaxPerWindow,
			time.Duration(p.CommandWindowSeconds)*time.Second,
			nil,
		); err == nil {
			throttle = t
		}
	}

	return func(env *agent.InboundEnvelope) bool {
		if isIgnoredSender(ignoredSenders, env) {
			return false
		}
		if throttle == nil {
			return true
		}
		return throttle.Allow(throttleScope(env))
	}
}

// PerCommandGuard builds the access-policy guard for a single command from
// its access mode + allowlist and the agent's owner-trust params. It is
// layered under the registry-wide guard (ignore list + throttle):
//
//   - "everyone":  nil — anyone may trigger it (the registry guard still runs).
//   - "owner":     only the verified owner (see ownerCheck).
//   - "allowlist": the owner OR a nick/account on the command's allowlist.
//   - unknown:     treated as "owner" — fail safe, never wider than owner.
//
// Owner-trust resolution (mode, account override, hostmask fallback) is
// identical to BuildCommandGuard; only the gate's breadth differs by access.
func PerCommandGuard(access string, allowlist []string, p GuardParams) agent.CommandGuard {
	mode := strings.ToLower(strings.TrimSpace(p.OwnerMode))
	if mode == "" {
		mode = "none"
	}
	ownerNick := strings.ToLower(strings.TrimSpace(p.OwnerNick))
	ownerAccount := strings.ToLower(strings.TrimSpace(p.OwnerAccount))
	if ownerAccount == "" {
		ownerAccount = ownerNick
	}
	ownerHostmask := strings.ToLower(strings.TrimSpace(p.OwnerHostmask))
	botNick := strings.ToLower(strings.TrimSpace(p.BotNick))

	owner := func(env *agent.InboundEnvelope) bool {
		return ownerCheck(env, mode, botNick, ownerNick, ownerAccount, ownerHostmask)
	}

	switch strings.ToLower(strings.TrimSpace(access)) {
	case string(commands.AccessEveryone):
		return nil
	case string(commands.AccessAllowlist):
		set := buildIgnoredSet(allowlist) // same normalization: lowercase + trim + drop empties
		return func(env *agent.InboundEnvelope) bool {
			return owner(env) || matchesAllowlist(set, env)
		}
	default: // "owner" + unknown
		return owner
	}
}

// matchesAllowlist reports whether the envelope's IRCv3 account-tag or
// sender nick is on the (already-normalized) allowlist set. Account-tag is
// preferred (stable across nick changes); sender nick is the fallback.
func matchesAllowlist(set map[string]struct{}, env *agent.InboundEnvelope) bool {
	if set == nil || env == nil {
		return false
	}
	if acct, _ := env.Metadata["account"].(string); acct != "" {
		if _, ok := set[strings.ToLower(strings.TrimSpace(acct))]; ok {
			return true
		}
	}
	sender := strings.ToLower(strings.TrimSpace(env.Sender))
	if sender == "" {
		return false
	}
	_, ok := set[sender]
	return ok
}
