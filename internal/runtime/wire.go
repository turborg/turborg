package runtime

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/turborg/turborg/internal/agent"
	"github.com/turborg/turborg/internal/commands"
	"github.com/turborg/turborg/internal/connector/irc"
	"github.com/turborg/turborg/internal/flow"
	"github.com/turborg/turborg/internal/llm"
	"github.com/turborg/turborg/internal/llm/anthropic"
	"github.com/turborg/turborg/internal/llm/openaicompat"
	"github.com/turborg/turborg/internal/messages"
	"github.com/turborg/turborg/internal/skill"
)

// CommonParams are the per-tenant inputs the shared agent wiring consumes,
// independent of where they were sourced (env for the single-instance CLI, the
// tenant feed for the pooled runtime) and independent of transport (port-bound
// vs listenerless bouncer/gateway, PUT vs POST state).
//
// It carries the wiring that historically drifted between the two runtimes:
// the single-instance path built it in runtime.Build, the pooled path hand-rolled a
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

	// Commands is the tenant's data-driven skill set, swapped into the command
	// registry (command-kind skills) and the skill engine/scheduler (event /
	// match / schedule skills). Empty → the agent dispatches nothing.
	Commands []skill.Skill

	// Flows is the tenant's declarative node-graph flow set, run by the flow
	// engine on event/match triggers. Empty → no flows.
	Flows []flow.Flow

	// Platform seeds the connector-agnostic {platform} template placeholder
	// (and its IRC {network} alias) — the transport label a static skill can
	// echo back. The IRC server hostname today; a Discord server / Slack
	// workspace / "Web" once those connectors land.
	Platform string

	// LLM provider powering LLM-type commands. Nil → those commands are
	// skipped at build time. Shared across pooled tenants (a stateless HTTP
	// client) and per-process for single-instance.
	LLM llm.Provider

	// LLMInputCap / LLMOutputCap are the rolling-24h token budget caps.
	// Both > 0 to enable enforcement via BudgetedProvider. 0 = unrestricted.
	LLMInputCap  int
	LLMOutputCap int

	// LLMInputUsed / LLMOutputUsed seed the budget with consumption already
	// reported across the account for the rolling window (sibling agents and
	// previously-destroyed ones), so the cap is enforced per account/window
	// rather than per agent-instance. 0 = fresh window. See TokenBudget.Seed.
	LLMInputUsed  int
	LLMOutputUsed int

	// ActivityHook fires on owner-presence signals the bouncer raises:
	// a client attaching, and a periodic presence heartbeat while a client
	// stays attached. Nil → the attach hook is not wired. The single-instance
	// path passes its Notifier.Hook (a per-event POST to ACTIVITY_URL); pooled
	// passes a hook that marks the tenant active in the pool's coalescing
	// aggregator (a per-event POST per tenant would hammer the control
	// plane). The bot's own outbound traffic is NOT activity and has no
	// hook — only genuine owner presence resets the idle timer.
	ActivityHook func(reason string)

	// Store backs bouncer welcome replay + web-shell scrollback + CHATHISTORY.
	// Nil → no store wiring (no submitters, connector keeps its zero value).
	Store messages.Store

	// SkillStore backs durable skill/flow state (counters, per-user values,
	// history). Shared by the skill engine and the flow engine so a value a
	// skill writes is visible to a flow of the same namespace. Nil → each engine
	// falls back to its own in-process store (state lost on restart).
	SkillStore skill.Store
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

// Wiring is the live skill machinery WireCommon constructs alongside the
// command registry: the event/match Engine (subscribed to the agent's bus) and
// the schedule Scheduler. The caller supervises Scheduler.Run and reaches both
// from a hot reload (ApplySkills) to swap the skill set in place.
type Wiring struct {
	Engine    *skill.Engine
	Scheduler *skill.Scheduler
	Flows     *flow.Engine
}

// WireCommon installs the connector-agnostic agent wiring shared by the
// single-instance (cmd/turborg) and pooled (internal/server) runtimes onto an
// already-constructed agent + IRC connector: client limits, outbound throttle,
// owner nudge, message store + event submitters, builtin commands, the command
// guard, and the skill engine + scheduler. It calls a.AddConnector itself and
// returns the live Wiring so the caller can supervise the scheduler and hot-
// reload the skill set.
//
// The caller owns everything that genuinely differs by mode — agent prefix,
// gateway (listener vs router), bouncer listener mode, state-push transport —
// and constructs the deps (LLM/activity/store) from its own config source.
//
// No process globals or shared mutable state: invoked once per single-instance
// process and N times per pooled process, each call producing a fully
// independent set of throttles/guards/submitters/engine.
func WireCommon(a *agent.Agent, ircConn *irc.Connector, p CommonParams, log *slog.Logger) (*Wiring, error) {
	if log == nil {
		log = slog.Default()
	}

	a.Commands.SetMaxDynamic(p.CustomCommandsMax)
	ircConn.SetClientLimits(p.Limits)

	if p.ActivityHook != nil {
		ircConn.SetBouncerAttachHook(p.ActivityHook)
	}

	if p.OutboundMaxPerWindow > 0 && p.OutboundWindowSeconds > 0 {
		t, err := irc.NewThrottle(
			p.OutboundMaxPerWindow,
			time.Duration(p.OutboundWindowSeconds)*time.Second,
			nil,
		)
		if err != nil {
			return nil, fmt.Errorf("runtime: outbound throttle: %w", err)
		}
		ircConn.SetOutboundThrottle(t)
	}

	if nudge := irc.NewOwnerNudge(p.OwnerNick, p.OwnerDMNudgeEvery); nudge != nil {
		ircConn.SetOwnerNudge(nudge)
	}

	if p.Store != nil {
		ircConn.SetMessageStore(p.Store)
	}

	// Wrap the LLM provider with budget enforcement when caps are set.
	// The wrapped provider is used everywhere: commands, /tb, gateway.
	// Idempotent — if the runtime already wrapped it (so the gateway can
	// share the same budget instance), this is a no-op.
	provider := BuildBudgetedProvider(a, p.LLM, p.LLMInputCap, p.LLMOutputCap, p.LLMInputUsed, p.LLMOutputUsed, log)

	if provider != nil {
		ircConn.SetLLMProvider(provider)
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
	// layered on top by the guard each skill carries.
	a.Commands.SetGuard(buildRegistryGuard(p.Owner))

	// Skill engine (event/match) + scheduler, sharing the same provider and
	// owner/platform render context as the command path. The engine acts on the
	// network through the connector-agnostic IRC Actor and is subscribed to the
	// agent's bus; the scheduler is supervised by the caller.
	engine := skill.NewEngine(skill.Options{
		Actor:     irc.NewActor(ircConn),
		Provider:  provider,
		Store:     p.SkillStore,
		Platform:  p.Platform,
		Owner:     p.Owner.OwnerNick,
		MaxSkills: p.CustomCommandsMax,
		Log:       log,
	})
	engine.Subscribe(a.Events)
	scheduler := skill.NewScheduler(engine, log)

	// Flow engine: the node-graph layer above single-shot skills, sharing the
	// same actor/provider/owner/platform context and subscribed to the same bus.
	flowEngine := flow.NewEngine(flow.Options{
		Actor:    irc.NewActor(ircConn),
		Provider: provider,
		Store:    p.SkillStore,
		Platform: p.Platform,
		Owner:    p.Owner.OwnerNick,
		MaxFlows: p.CustomCommandsMax,
		Log:      log,
	})
	flowEngine.Subscribe(a.Events)
	wiring := &Wiring{Engine: engine, Scheduler: scheduler, Flows: flowEngine}

	// Install the tenant's data-driven skill set. The command registry's
	// dynamic set, the engine's event/match set, and the scheduler's schedule
	// set are each fully owned by their Replace primitive, so a later hot reload
	// swaps all three atomically.
	ApplySkills(a, wiring, p.Commands, provider, p.Owner, p.Platform, log)
	ApplyFlows(wiring, p.Flows)
	return wiring, nil
}

// ApplySkills rebuilds the data-driven skill set from skills and swaps it into
// the three live surfaces in place — an atomic, no-reconnect hot reload:
// command-kind skills into the command registry (ReplaceDynamic), event/match
// skills into the engine (ReplaceSkills), and schedule skills into the
// scheduler (ReplaceSkills). It is the single source of truth for turning skill
// definitions into live behavior, shared by initial wiring (WireCommon), the
// pooled runtime's feed-driven reload, and the single-instance runtime's
// refresher, so they can't drift. The registry-wide ignore/throttle guard set
// at wire time is untouched; per-command access (owner / allowlist / everyone)
// is reapplied from each skill.
func ApplySkills(a *agent.Agent, w *Wiring, skills []skill.Skill, provider llm.Provider, owner GuardParams, platform string, log *slog.Logger) {
	if log == nil {
		log = slog.Default()
	}
	built := skill.Build(skills, provider, func(s skill.Skill) agent.CommandGuard {
		return PerCommandGuard(string(s.Access), s.Allowlist, owner)
	}, platform, owner.OwnerNick, log)
	a.Commands.ReplaceDynamic(built)
	if w != nil {
		if w.Engine != nil {
			w.Engine.ReplaceSkills(skills)
		}
		if w.Scheduler != nil {
			w.Scheduler.ReplaceSkills(skills)
		}
	}
}

// ApplyFlows swaps the engine's node-graph flow set in place — an atomic,
// no-reconnect hot reload, shared by initial wiring and any flow refresher.
func ApplyFlows(w *Wiring, flows []flow.Flow) {
	if w != nil && w.Flows != nil {
		w.Flows.ReplaceFlows(flows)
	}
}

// ApplyRegistryGuard rebuilds the registry-wide guard (ignore list + per-sender
// command throttle) from p and swaps it onto a running agent. It lets an
// operator's ignore-list edit take effect in place — no connection teardown —
// the same way ApplyCommands hot-swaps the command set. The swap is atomic via
// the registry's internal lock, so it is safe to call while the agent
// dispatches. The throttle is rebuilt from p, so its window counters reset;
// that is acceptable for an infrequent settings change and avoids threading a
// live throttle through the swap.
func ApplyRegistryGuard(a *agent.Agent, p GuardParams) {
	a.Commands.SetGuard(buildRegistryGuard(p))
}

// BuildBudgetedProvider wraps an llm.Provider with rolling-24h budget
// enforcement when caps are set. The onUsage callback emits the
// structured llm_usage log (for external scraping) and publishes
// EventLLMUsage on the agent's bus (broadcast to WS clients).
//
// Idempotent: a provider that is already a *llm.BudgetedProvider is
// returned unchanged, so the runtime can build it once and share the
// same budget instance between the gateway and WireCommon. nil in →
// nil out; caps both 0 → unwrapped (no enforcement).
//
// usedInput/usedOutput seed the rolling window with consumption already
// reported across the account for the same window (see TokenBudget.Seed),
// so the cap is enforced per account/window rather than per agent-instance.
// Both 0 → no prior usage, fresh window.
func BuildBudgetedProvider(a *agent.Agent, provider llm.Provider, inputCap, outputCap, usedInput, usedOutput int, log *slog.Logger) llm.Provider {
	if provider == nil {
		return nil
	}
	if _, ok := provider.(*llm.BudgetedProvider); ok {
		return provider
	}
	if inputCap <= 0 && outputCap <= 0 {
		return provider
	}
	if log == nil {
		log = slog.Default()
	}
	budget := llm.NewTokenBudget()
	budget.SetBaseline(usedInput, usedOutput)
	model := provider.Model()
	return llm.NewBudgetedProvider(provider, budget, inputCap, outputCap, func(u llm.Usage) {
		inTotal, outTotal := budget.Totals()
		// cached_tokens (prefix-cache subset of input) and model let a downstream
		// meter price the call per model with cache crediting.
		log.Info("llm_usage",
			"input_tokens", u.InputTokens,
			"output_tokens", u.OutputTokens,
			"cached_tokens", u.CachedTokens,
			"model", model,
			"input_total", inTotal,
			"output_total", outTotal,
		)
		a.Events.Publish(context.Background(), &agent.Event{
			Type: agent.EventLLMUsage,
			Time: time.Now(),
			Fields: map[string]any{
				"input_tokens":  u.InputTokens,
				"output_tokens": u.OutputTokens,
				"cached_tokens": u.CachedTokens,
				"model":         model,
				"input_total":   inTotal,
				"output_total":  outTotal,
				"input_cap":     inputCap,
				"output_cap":    outputCap,
			},
		})
	})
}

// BuildLLMProvider mints an llm.Provider from a provider kind + endpoint +
// key + default model. It is the single dispatcher both runtimes use so the
// single-instance env loader and the pooled process select backends identically.
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
// lack of an LLM; !ask is simply not registered. Shared by the single-instance env
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
