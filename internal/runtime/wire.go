package runtime

import (
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/turborg/turborg/internal/agent"
	"github.com/turborg/turborg/internal/connector/irc"
	"github.com/turborg/turborg/internal/llm"
	"github.com/turborg/turborg/internal/llm/anthropic"
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

	// LLM provider powering !ask. Nil → !ask is not registered. Shared across
	// pooled tenants (a stateless HTTP client) and per-process for dedicated.
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

	RegisterBuiltinCommands(a, p.LLM)
	a.Commands.SetGuard(BuildCommandGuardFromParams(p.Owner))
	return nil
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
// its config.Settings dependency removed so the pooled runtime can call it.
func BuildCommandGuardFromParams(p GuardParams) agent.CommandGuard {
	mode := strings.ToLower(strings.TrimSpace(p.OwnerMode))
	if mode == "" {
		mode = "none"
	}

	ignoredSenders := buildIgnoredSet(p.IgnoredNicks)

	ownerNick := strings.ToLower(strings.TrimSpace(p.OwnerNick))
	ownerAccount := strings.ToLower(strings.TrimSpace(p.OwnerAccount))
	if ownerAccount == "" {
		ownerAccount = ownerNick
	}
	ownerHostmask := strings.ToLower(strings.TrimSpace(p.OwnerHostmask))
	botNick := strings.ToLower(strings.TrimSpace(p.BotNick))

	var throttle *irc.Throttle
	if p.CommandMaxPerWindow > 0 && p.CommandWindowSeconds > 0 {
		t, err := irc.NewThrottle(
			p.CommandMaxPerWindow,
			time.Duration(p.CommandWindowSeconds)*time.Second,
			nil,
		)
		if err == nil {
			throttle = t
		}
	}

	return func(env *agent.InboundEnvelope) bool {
		if isIgnoredSender(ignoredSenders, env) {
			return false
		}
		if !ownerCheck(env, mode, botNick, ownerNick, ownerAccount, ownerHostmask) {
			return false
		}
		if throttle == nil {
			return true
		}
		return throttle.Allow(throttleScope(env))
	}
}
