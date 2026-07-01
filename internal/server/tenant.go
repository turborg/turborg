package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"reflect"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/turborg/turborg/internal/agent"
	"github.com/turborg/turborg/internal/connector/irc"
	"github.com/turborg/turborg/internal/flow"
	"github.com/turborg/turborg/internal/ident"
	"github.com/turborg/turborg/internal/llm"
	"github.com/turborg/turborg/internal/messages"
	"github.com/turborg/turborg/internal/messagesink"
	"github.com/turborg/turborg/internal/runtime"
	"github.com/turborg/turborg/internal/safe"
	"github.com/turborg/turborg/internal/skill"
	"github.com/turborg/turborg/internal/statepush"
	"github.com/turborg/turborg/internal/web"
)

// TenantStatus is the supervised lifecycle phase of a tenant.
type TenantStatus int

const (
	// StatusRunning: the tenant's work loop is executing.
	StatusRunning TenantStatus = iota
	// StatusQuarantined: the work loop panicked; the tenant is paused for a
	// backoff interval before the supervisor revives it.
	StatusQuarantined
)

func (s TenantStatus) String() string {
	switch s {
	case StatusRunning:
		return "running"
	case StatusQuarantined:
		return "quarantined"
	default:
		return "unknown"
	}
}

// Tenant is one isolated agent inside the pooled process, self-supervised so
// a panic in its work loop quarantines ONLY this tenant — never the process
// or its siblings (M3). The agent + connectors + event bus it owns are
// tenant-private; a Tenant must never reference another tenant's state.
//
// Caveat (tracked for plan E4): only panics on the tenant's own work
// goroutine are recovered here. Panics in goroutines the agent/connectors
// spawn internally are caught once those packages adopt the same goSafe
// discipline; until then this is the supervision boundary, not a guarantee.
type Tenant struct {
	ID  string
	log *slog.Logger

	// work is the unit of supervised work, re-invoked on each (re)start.
	// Injected so tests can substitute a panicking or controllable body.
	work func(context.Context) error

	quarantineBase time.Duration

	cancel context.CancelFunc
	done   chan struct{}
	// restartCh signals the supervisor to tear down the current run and
	// re-run with the latest spec (M7 hot reload). Buffered(1) + non-blocking
	// send coalesces rapid updates into a single pending restart.
	restartCh chan struct{}

	mu              sync.Mutex
	spec            TenantSpec
	status          TenantStatus
	failures        int
	lastErr         error
	quarantineUntil time.Time
	// runCancel cancels the in-flight work run so update() can restart it.
	runCancel context.CancelFunc
	// ircConn is the live IRC connector for the current run, captured in
	// buildConnectors and cleared when the run ends. The bouncer router reaches
	// it via ServeBouncerConn to deliver an attached client to this tenant. nil
	// between runs (quarantined / restarting) → an inbound conn is closed.
	ircConn *irc.Connector
	// agentRef is the live agent for the current run, captured in defaultWork
	// and cleared when the run ends. update() reaches it via reloadCommands to
	// swap the command set in place (ReplaceDynamic) when ONLY the commands
	// changed — no reconnect. nil between runs → reload falls back to restart.
	agentRef *agent.Agent
	// wiring is the live skill engine + scheduler for the current run, captured
	// in buildConnectors and cleared when the run ends. reloadCommands swaps the
	// skill set in place through it (engine/scheduler ReplaceSkills) alongside
	// the command registry. nil between runs.
	wiring *runtime.Wiring
	// gateway is the live web shell for the current run, built in
	// buildConnectors when the spec carries a GatewayToken and cleared when the
	// run ends. The web router reaches it via ServeWS to upgrade an attached
	// browser client. nil between runs / when the tenant has no web shell → an
	// inbound WS request is closed with 404.
	gateway *web.Gateway
	// stateEmitter mirrors this tenant's connector state (upstream status,
	// channels, nick) to the control plane on every transition, so a downstream
	// UI can reflect connection status for pooled tenants the same way it does
	// for single-instance ones. Inert when no control plane is configured;
	// Stop()ped at run end.
	stateEmitter *statepush.Emitter

	// controlPlaneURL/Token are where the state emitter POSTs (per tenant:
	// <url>/turborgs/<id>/state). Set once at construction.
	controlPlaneURL   string
	controlPlaneToken string

	// llmProvider is the shared LLM provider handed down from the Server.
	// Nil → LLM-type commands are skipped for this tenant.
	llmProvider llm.Provider

	// activity is the pool-wide aggregator this tenant marks itself active in
	// (bouncer attach, message traffic). Nil when no control plane is set.
	activity *activityAggregator

	// idents is the shared (pool-wide) source-port → ident registry the
	// connector reports into so an external RFC-1413 responder can name this
	// tenant. Never nil (the Server always builds one).
	idents *ident.Registry

	// messageSink is the durable-message writer for the current run, owning a
	// background flush goroutine. Captured so defaultWork can Close it on run
	// end (goleak-clean across restarts). nil between runs / no control plane.
	messageSink *messagesink.Sink
}

// startTenant launches a self-supervising tenant under a child of parent.
// workFactory builds the body to run (and re-run after a panic) from the
// constructed Tenant; quarantineBase is the first backoff step (doubled per
// consecutive failure, capped).
func startTenant(parent context.Context, spec TenantSpec, log *slog.Logger, quarantineBase time.Duration, workFactory func(*Tenant) func(context.Context) error, controlPlaneURL, controlPlaneToken string, llmProvider llm.Provider, activity *activityAggregator, idents *ident.Registry) *Tenant {
	ctx, cancel := context.WithCancel(parent)
	t := &Tenant{
		ID:                spec.TurborgID,
		log:               log.With("turborg_id", spec.TurborgID),
		quarantineBase:    quarantineBase,
		cancel:            cancel,
		done:              make(chan struct{}),
		restartCh:         make(chan struct{}, 1),
		spec:              spec,
		status:            StatusRunning,
		controlPlaneURL:   controlPlaneURL,
		controlPlaneToken: controlPlaneToken,
		llmProvider:       llmProvider,
		activity:          activity,
		idents:            idents,
	}
	t.work = workFactory(t)
	safe.Go("supervise/"+spec.TurborgID, func() { t.supervise(ctx) })
	return t
}

// supervise runs the tenant's work loop, recovering panics and quarantining
// (with exponential backoff) before reviving, and restarting the run when the
// spec changes (M7). Returns when the parent ctx is cancelled.
func (t *Tenant) supervise(parent context.Context) {
	defer close(t.done)

	for {
		if parent.Err() != nil {
			return
		}

		// Per-run context so update() can cancel just this run (restart)
		// without tearing down the tenant.
		runCtx, cancel := context.WithCancel(parent)
		t.setRunCancel(cancel)
		panicked := t.runOnce(runCtx)
		cancel()

		if parent.Err() != nil {
			return
		}

		if panicked {
			backoff := t.enterQuarantine()
			t.log.Warn("tenant quarantined after panic", "backoff", backoff, "failures", t.Failures())
			select {
			case <-parent.Done():
				return
			case <-t.restartCh:
				t.markRunning()
				t.log.Info("restarting quarantined tenant after config change")
			case <-time.After(backoff):
				t.markRunning()
				t.log.Info("reviving quarantined tenant")
			}
			continue
		}

		// Work ended without panic: either update() cancelled the run to
		// apply a config change, or the work returned on its own (e.g. no
		// runnable connectors). Wait for a restart signal or cancellation.
		select {
		case <-parent.Done():
			return
		case <-t.restartCh:
			t.log.Info("restarting tenant after config change")
		}
	}
}

func (t *Tenant) setRunCancel(cancel context.CancelFunc) {
	t.mu.Lock()
	t.runCancel = cancel
	t.mu.Unlock()
}

// runOnce executes the work body once, recovering any panic. Returns true if
// the body panicked.
func (t *Tenant) runOnce(ctx context.Context) (panicked bool) {
	defer func() {
		if r := recover(); r != nil {
			panicked = true
			t.setErr(fmt.Errorf("panic: %v", r))
			t.log.Error("recovered tenant panic", "panic", r, "stack", string(debug.Stack()))
		}
	}()

	if err := t.work(ctx); err != nil && !errors.Is(err, context.Canceled) {
		t.setErr(err)
		t.log.Error("tenant work returned error", "err", err)
	}
	return false
}

// enterQuarantine records the panic, bumps the failure count, computes the
// next backoff (base * 2^(failures-1), capped at 32× base), stamps the
// quarantine deadline, and returns the backoff duration.
func (t *Tenant) enterQuarantine() time.Duration {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.failures++
	backoff := t.quarantineBase << min(t.failures-1, 5) // cap at 32× base
	t.status = StatusQuarantined
	t.quarantineUntil = time.Now().Add(backoff)
	return backoff
}

func (t *Tenant) markRunning() {
	t.mu.Lock()
	t.status = StatusRunning
	t.mu.Unlock()
}

func (t *Tenant) setErr(err error) {
	t.mu.Lock()
	t.lastErr = err
	t.mu.Unlock()
}

// Status reports the tenant's current supervision phase.
func (t *Tenant) Status() TenantStatus {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.status
}

// Failures reports how many times the work loop has panicked.
func (t *Tenant) Failures() int {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.failures
}

// defaultTenantWork builds the production work body: a tenant-owned agent
// with this tenant's connectors, run under the tenant context. Rebuilt on
// each (re)start so a revived tenant gets a fresh agent.
func (t *Tenant) defaultWork() func(context.Context) error {
	return func(ctx context.Context) error {
		t.mu.Lock()
		prefix := t.spec.CommandPrefix
		t.mu.Unlock()
		// NewWithPrefix defaults an empty prefix to "!", matching the single-instance
		// path (where the env default fills it) so free tenants behave the same.
		a := agent.NewWithPrefix(t.log, prefix)
		t.buildConnectors(a)
		t.mu.Lock()
		t.agentRef = a
		var sched *skill.Scheduler
		if t.wiring != nil {
			sched = t.wiring.Scheduler
		}
		t.mu.Unlock()
		t.log.Info("tenant attached", "connectors", t.connectorTypes())

		// Drive schedule-trigger skills under a child context for the life of
		// this run. schedCancel + the wait keep the goroutine goleak-clean across
		// restarts (it always exits before defaultWork returns).
		schedCtx, schedCancel := context.WithCancel(ctx)
		schedDone := make(chan struct{})
		if sched != nil {
			safe.Go("skill-scheduler/"+t.ID, func() {
				defer close(schedDone)
				_ = sched.Run(schedCtx)
			})
		} else {
			close(schedDone)
		}

		err := a.Run(ctx)
		schedCancel()
		<-schedDone
		// Run ended (ctx cancelled / restart) — the connector's bouncer, web
		// gateway, and state emitter are stopping, so drop the handles. A late
		// inbound conn/WS then closes cleanly instead of hitting a torn-down run,
		// and the emitter's goroutine is drained (goleak-clean across restarts).
		t.mu.Lock()
		gw := t.gateway
		em := t.stateEmitter
		sink := t.messageSink
		t.ircConn = nil
		t.gateway = nil
		t.stateEmitter = nil
		t.messageSink = nil
		t.agentRef = nil
		t.wiring = nil
		t.mu.Unlock()
		if gw != nil {
			gw.Stop()
		}
		// Flush the final snapshot synchronously before Stop cancels the
		// debounce timer — otherwise a terminal state that lands ~1ms before
		// teardown (e.g. disconnected_banned) is dropped and the control
		// plane stays stuck at the prior state (e.g. registering). Safe on nil.
		flushCtx, flushCancel := context.WithTimeout(context.Background(), 5*time.Second)
		em.Flush(flushCtx)
		flushCancel()
		em.Stop() // safe on nil
		if sink != nil {
			// Drain + stop the sink's flush goroutine so restarts stay
			// goleak-clean. Bounded so a slow control plane can't hang teardown.
			closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			sink.Close(closeCtx)
			cancel()
		}
		return err
	}
}

// buildConnectors constructs this tenant's connectors from its spec and
// registers them on the tenant-owned agent. Unsupported types are logged and
// skipped (pooled mode ships connectors incrementally). A connector whose
// spec is invalid is skipped rather than failing the whole tenant.
func (t *Tenant) buildConnectors(a *agent.Agent) {
	t.mu.Lock()
	connectors := t.spec.Connectors
	caps := t.spec.PlanCapabilities
	gatewayToken := t.spec.GatewayToken
	ignoredNicks := t.spec.IgnoredNicks
	cmds := t.spec.Commands
	egressIP := t.spec.EgressIP
	t.mu.Unlock()

	for _, cs := range connectors {
		switch cs.Type {
		case "irc":
			settings, err := settingsFromConnectorSpec(cs)
			if err != nil {
				t.log.Error("skipping invalid irc connector", "err", err)
				continue
			}
			// Override the connector's ApplyDefaults with the per-instance values
			// the single-instance binary gets from env (QUIT brand + CTCP / bouncer
			// auth-failure tightening) — all sourced from the tenant feed's caps.
			t.applyTierSettings(settings, caps)
			// Bind the tenant's assigned egress IP so its outbound IRC leaves on
			// the right public IP (per-tenant egress round-robin). Empty/unresolved
			// → default route; see egress.go.
			if egressIP != "" {
				if src := defaultEgressResolver.resolveSourceIP(egressIP); src != "" {
					settings.SourceIP = src
				}
			}
			// Wire the connector to the tenant-owned agent bus: the bouncer's
			// outbound observer and the connector's join/part/topic/state events
			// publish here, and the web gateway subscribes to the same bus so the
			// web shell sees the full event stream (not just plain messages,
			// which arrive via the agent's inbound drain regardless).
			conn := irc.New(settings, t.log, a.Events)
			// Never reconnect faster than the upstream reaps our ghost nick
			// (avoids 433 self-collision storms on the shared egress IP).
			conn.SetReconnectFloor(irc.DefaultReconnectFloor)
			// Pooled runtime: the bouncer must not bind its own port — the pool
			// router feeds it connections via ServeBouncerConn after PROXY-v2
			// tenant resolution. Set before WireCommon adds the connector.
			conn.SetBouncerListenerless(true)
			// Report this tenant's upstream source port → ident so an external
			// RFC-1413 responder can name it (kills the ~ prefix + satisfies
			// "identd required" networks). Shared registry across all tenants.
			conn.SetIdentReporter(t.idents)
			// Boot already parked when the user has the link disconnected, so a
			// pool restart of a suspended tenant comes up parked rather than
			// flapping connect→quit. Live toggles arrive via applySuspend.
			conn.SetInitialSuspended(boolField(cs.Config, "suspended", false))

			// Durable message store (write-through to the control plane), so the
			// bouncer welcome replay, web-shell scrollback, and the user's
			// readable history survive a pool restart. Falls back to in-process
			// when no control plane is configured. The sink owns a flush
			// goroutine closed in defaultWork.
			store, sink := t.buildMessageStore()
			t.mu.Lock()
			t.messageSink = sink
			t.mu.Unlock()

			// The shared, connector-agnostic wiring — identical to the single-instance
			// runtime, which calls the same runtime.WireCommon. Gives pooled
			// tenants builtins (!ping/!version/!ask), the owner command guard,
			// plan limits + outbound throttle, owner nudge, activity reporting,
			// and message-store submitters, so the two modes can't drift. Owner-
			// trust config travels in the connector config (the feed reuses the
			// same connectorList() shape the single-instance spawn payload does).
			// Build the budgeted provider once so the same budget instance is
			// shared by the connector/commands (WireCommon) and the web gateway
			// below. WireCommon's own wrap is idempotent on this.
			cp := t.commonParams(cs, caps, settings.Nick, store, ignoredNicks, cmds)
			cp.Platform = settings.Hostname
			cp.Flows = t.spec.Flows
			budgetedProvider := runtime.BuildBudgetedProvider(a, cp.LLM, cp.LLMInputCap, cp.LLMOutputCap, cp.LLMInputUsed, cp.LLMOutputUsed, t.log)
			cp.LLM = budgetedProvider
			wiring, err := runtime.WireCommon(a, conn, cp, t.log)
			if err != nil {
				t.log.Error("skipping irc connector: wiring failed", "err", err)
				continue
			}

			t.mu.Lock()
			t.ircConn = conn
			t.wiring = wiring
			t.mu.Unlock()

			// Connector-state sync: mirror upstream status / channels / nick to
			// the control plane on every transition, so a downstream UI reflects
			// connection status for pooled tenants the same way it does for
			// single-instance ones. Inert + nil when no control plane.
			if em := buildTenantStateEmitter(conn, t.ID, t.controlPlaneURL, t.controlPlaneToken, t.log); em != nil {
				t.mu.Lock()
				t.stateEmitter = em
				t.mu.Unlock()
			}

			// Web shell: built only when the tenant carries a gateway token (the
			// feed expresses the "web shell enabled" capability by emitting the
			// token at all). Listenerless like the bouncer — the web
			// router dispatches `/c/<id>` to the shared Handler via ServeWS, so the
			// gateway never binds its own port.
			if gatewayToken != "" {
				var tbCap int
				if caps != nil {
					tbCap = caps.TBSummarizeMaxMessages
				}
				gwActivity := func(string) {
					if t.activity != nil {
						t.activity.Mark(t.ID)
					}
				}
				gw, err := buildTenantGateway(conn, gatewayToken, t.log, store, budgetedProvider, tbCap, gwActivity, wiring.FireWebhook)
				if err != nil {
					t.log.Error("skipping web gateway", "err", err)
					continue
				}
				gw.Subscribe(a.Events)
				t.mu.Lock()
				t.gateway = gw
				t.mu.Unlock()
			}
		default:
			t.log.Warn("connector type not supported in pooled mode yet", "type", cs.Type)
		}
	}
}

// applyTierSettings overrides the connector defaults ApplyDefaults filled with
// the per-instance values the single-instance binary receives from env — all
// sourced from the tenant feed's caps: the IRC QUIT brand, the CTCP throttle,
// and the bouncer auth-failure ceiling. Each guard keeps an unset value on the
// ApplyDefaults default rather than zeroing it.
func (t *Tenant) applyTierSettings(s *irc.Settings, caps *PlanCapabilities) {
	if caps == nil {
		return
	}
	if caps.QuitMessage != "" {
		s.QuitMessage = caps.QuitMessage
	}
	if caps.CTCPMaxPerWindow > 0 {
		s.CTCPMaxPerWindow = caps.CTCPMaxPerWindow
	}
	if caps.CTCPWindowSeconds > 0 {
		s.CTCPWindowSeconds = caps.CTCPWindowSeconds
	}
	if caps.BouncerMaxFailedAttempts > 0 {
		s.BouncerMaxFailedAttempts = caps.BouncerMaxFailedAttempts
	}
}

// commonParams maps a tenant's IRC connector spec + plan caps onto the
// runtime.CommonParams the shared agent wiring consumes — the single place
// pooled inputs become the same struct the single-instance runtime builds from env.
// Owner-trust fields live in the connector config (the feed reuses the
// single-instance connectorList() shape); identity limits, throttles, and the nudge
// interval come from the plan caps; ignored nicks + the LLM provider + the
// store + the activity hook are threaded from the tenant.
//
// CustomCommandsMax + Commands carry the tenant's data-driven command set
// and its cap, so pooled tenants get user-defined commands the same way the
// single-instance runtime does (from TURBORG_COMMANDS).
func (t *Tenant) commonParams(cs ConnectorSpec, caps *PlanCapabilities, botNick string, store messages.Store, ignoredNicks []string, cmds []skill.Skill) runtime.CommonParams {
	var limits irc.ClientLimits
	var outMax, outWin, nudge, cmdMax, cmdWin, customCmdMax, llmInCap, llmOutCap, llmInUsed, llmOutUsed int
	if caps != nil {
		limits = irc.ClientLimits{
			NickLocked:             caps.NickLocked,
			RealnameLocked:         caps.RealnameLocked,
			MaxChannels:            caps.MaxChannels,
			TBSummarizeMaxMessages: caps.TBSummarizeMaxMessages,
		}
		outMax, outWin = caps.OutboundMsgsPerWindow, caps.OutboundWindowSeconds
		nudge = caps.OwnerDMNudgeEvery
		cmdMax, cmdWin = caps.CommandMaxPerWindow, caps.CommandWindowSeconds
		customCmdMax = caps.CustomCommandsMax
		llmInCap, llmOutCap = caps.LLMInputTokensPerDay, caps.LLMOutputTokensPerDay
		llmInUsed, llmOutUsed = caps.LLMInputTokensUsed, caps.LLMOutputTokensUsed
	}

	// The strict-network AI gate is a per-connector network policy, not a
	// plan cap — source it from the connector spec so it applies regardless
	// of whether caps were supplied.
	limits.AIStrict = boolField(cs.Config, "ai_strict", false)
	limits.AIStrictMessage = stringField(cs.Config, "ai_strict_message")

	// Activity hook: mark this tenant active in the pool's coalescing
	// aggregator. Nil when no control plane is configured.
	var activityHook func(string)
	if t.activity != nil {
		activityHook = func(string) { t.activity.Mark(t.ID) }
	}

	ownerNick := stringField(cs.Config, "owner_nick")
	return runtime.CommonParams{
		CustomCommandsMax: customCmdMax,
		Commands:          cmds,
		Limits:            limits,
		Owner: runtime.GuardParams{
			OwnerMode:            stringField(cs.Config, "owner_mode"),
			OwnerNick:            ownerNick,
			OwnerAccount:         stringField(cs.Config, "owner_account"),
			OwnerHostmask:        stringField(cs.Config, "owner_hostmask"),
			IgnoredNicks:         ignoredNicks,
			BotNick:              botNick,
			CommandMaxPerWindow:  cmdMax,
			CommandWindowSeconds: cmdWin,
		},
		OutboundMaxPerWindow:  outMax,
		OutboundWindowSeconds: outWin,
		OwnerNick:             ownerNick,
		OwnerDMNudgeEvery:     nudge,
		LLM:                   t.llmProvider,
		LLMInputCap:           llmInCap,
		LLMOutputCap:          llmOutCap,
		LLMInputUsed:          llmInUsed,
		LLMOutputUsed:         llmOutUsed,
		ActivityHook:          activityHook,
		Store:                 store,
		SkillStore:            t.buildSkillStore(),
	}
}

// buildMessageStore builds this tenant's message store. With a control plane
// configured it's a write-through HTTP store pointed at the per-tenant control-
// plane endpoint (durable history that survives a pool restart + serves the
// user's readable log window); the returned sink owns a flush goroutine the
// caller must Close. Without a control plane it's an in-process MemoryStore
// (and a nil sink) — the OSS/file-source path.
func (t *Tenant) buildMessageStore() (messages.Store, *messagesink.Sink) {
	if t.controlPlaneURL == "" {
		return messages.NewMemoryStore(0), nil
	}
	base := strings.TrimRight(t.controlPlaneURL, "/") + "/turborgs/" + t.ID + "/messages"
	sink := messagesink.New(base, t.controlPlaneToken, t.log)
	if hs := messages.NewHTTPStore(base, t.controlPlaneToken, sink, t.log); hs != nil {
		return hs, sink
	}
	return messages.NewMemoryStore(0), sink
}

// buildSkillStore builds this tenant's durable skill/flow state backend. With a
// control plane configured it's an HTTP store pointed at the per-tenant
// control-plane base (so quiz scores, counters, seen-dbs, and history survive a
// pool restart); the store appends /state/{ns}/{key} per value. Without a
// control plane it returns nil and the engines keep state in-process — the
// OSS/file-source path.
func (t *Tenant) buildSkillStore() skill.Store {
	if t.controlPlaneURL == "" {
		return nil
	}
	base := strings.TrimRight(t.controlPlaneURL, "/") + "/turborg/" + t.ID
	if hs := skill.NewHTTPStore(base, t.controlPlaneToken, t.log); hs != nil {
		return hs
	}
	return nil
}

// update applies a new desired spec to a running tenant (M7 hot reload). An
// identical spec is a no-op. A change to ONLY the command set is applied in
// place — the live registry's dynamic commands are swapped via ReplaceDynamic
// with no reconnect, so attaching/editing/detaching a command takes effect
// within the feed-poll window without dropping the IRC session or the
// attached bouncer clients. Any other change (connectors, limits, owner
// config, ignored nicks, …) triggers a full reconnect, which is correct and
// simple and avoids silent state divergence.
func (t *Tenant) update(spec TenantSpec) {
	t.mu.Lock()
	if reflect.DeepEqual(t.spec, spec) {
		t.mu.Unlock()
		return
	}
	prev := t.spec
	t.spec = spec
	cancel := t.runCancel
	t.mu.Unlock()

	// In-place, no-reconnect changes. The LLM-usage baseline (llm_*_tokens_used)
	// is refreshed on every feed poll as tokens accrue, so it must NOT trigger a
	// reconnect — that would drop the IRC session every few seconds. For pooled
	// tenants the per-account cap equals the per-tenant cap (a pooled account
	// owns one turborg), so local in-process counting is exact and the baseline
	// only matters as the restart-recovery seed applied at build time; a live
	// change to it alone is a no-op here. A command-set change still reloads in
	// place. Anything else falls through to a restart.
	if liveUpdatableOnlyChange(prev, spec) && t.applyLiveUpdate(prev, spec) {
		return
	}

	t.log.Info("tenant spec changed; restarting", "connectors", t.connectorTypes())

	// Cancel the in-flight run, then signal the supervisor to re-run with the
	// new spec. Non-blocking send coalesces concurrent updates.
	if cancel != nil {
		cancel()
	}
	select {
	case t.restartCh <- struct{}{}:
	default:
	}
}

// applyLiveUpdate reconciles the live-updatable facets (command set, ignore
// list, nick/channels, connect/disconnect intent) of a spec change in place,
// with no reconnect. The caller has already established that ONLY such facets
// changed (liveUpdatableOnlyChange). Returns true when every changed facet
// applied; false means the tenant isn't running (a facet apply reported no live
// connector), so the caller falls back to a restart that re-seeds everything
// from the new spec at build time. A change confined to the usage baseline
// applies nothing and returns true (nothing to reconnect for).
func (t *Tenant) applyLiveUpdate(prev, spec TenantSpec) bool {
	commandsChanged := !commandSetsEqual(prev.Commands, spec.Commands)
	flowsChanged := !reflect.DeepEqual(prev.Flows, spec.Flows)
	ignoresChanged := !reflect.DeepEqual(prev.IgnoredNicks, spec.IgnoredNicks)
	nickChanged, channelsChanged := ircNickChannelsChanged(prev, spec)
	suspendChanged := ircSuspendChanged(prev, spec)
	if !commandsChanged && !flowsChanged && !ignoresChanged && !nickChanged && !channelsChanged && !suspendChanged {
		return true // only the usage baseline moved — nothing to reconnect for
	}
	// Apply each changed facet in place. The command set swaps via
	// ReplaceDynamic; the ignore list swaps the registry-wide guard; nick and
	// channels apply live via NICK / JOIN+PART; the connect/disconnect intent
	// parks/resumes the upstream link.
	okCommands := !commandsChanged || t.reloadCommands(spec.Commands)
	okFlows := !flowsChanged || t.reloadFlows(spec.Flows)
	okIgnores := !ignoresChanged || t.reloadGuard()
	okNickChan := (!nickChanged && !channelsChanged) || t.applyNickChannels(spec)
	okSuspend := !suspendChanged || t.applySuspend(spec)
	if okCommands && okFlows && okIgnores && okNickChan && okSuspend {
		t.log.Info("tenant settings reloaded in place",
			"commands", len(spec.Commands), "flows", len(spec.Flows), "ignored", len(spec.IgnoredNicks),
			"nick_changed", nickChanged, "channels_changed", channelsChanged,
			"suspend_changed", suspendChanged)
		return true
	}
	return false
}

// commandsOnlyChange reports whether two differing specs differ in nothing
// but their command set — the precondition for an in-place reload.
func commandsOnlyChange(a, b TenantSpec) bool {
	a.Commands = nil
	b.Commands = nil
	return reflect.DeepEqual(a, b)
}

// liveUpdatableOnlyChange reports whether two differing specs differ in nothing
// but fields that can be applied to a running tenant WITHOUT a reconnect: the
// command set, the ignore list, and the LLM-usage baseline (llm_*_tokens_used,
// which the feed refreshes continuously). The command set, the ignore list, and
// the baseline are zeroed before comparison so a change confined to them returns
// true. The ignore list only feeds the registry-wide command guard, which can be
// hot-swapped in place, so an ignore edit must not drop the connection.
func liveUpdatableOnlyChange(a, b TenantSpec) bool {
	a.Commands, b.Commands = nil, nil
	a.Flows, b.Flows = nil, nil
	a.IgnoredNicks, b.IgnoredNicks = nil, nil
	a.PlanCapabilities = capsWithoutTokenUsage(a.PlanCapabilities)
	b.PlanCapabilities = capsWithoutTokenUsage(b.PlanCapabilities)
	// nick + channels apply live (NICK / JOIN+PART) without a reconnect, so a
	// change confined to them must not restart — that's also what makes
	// syncing the user's live /nick and /join back into the desired config
	// safe (the echoed change reconciles to a no-op instead of looping).
	a.Connectors = stripLiveConnectorFields(a.Connectors)
	b.Connectors = stripLiveConnectorFields(b.Connectors)
	return reflect.DeepEqual(a, b)
}

// stripLiveConnectorFields returns the connectors with the IRC connector's
// live-applicable config keys (nick, channels, suspended) removed, so spec
// comparison ignores them. Deep-copies the affected config map so the caller's
// spec is untouched.
func stripLiveConnectorFields(conns []ConnectorSpec) []ConnectorSpec {
	out := make([]ConnectorSpec, len(conns))
	for i, c := range conns {
		out[i] = c
		if c.Type != "irc" || c.Config == nil {
			continue
		}
		cfg := make(map[string]any, len(c.Config))
		for k, v := range c.Config {
			if k == "nick" || k == "channels" || k == "suspended" {
				continue
			}
			cfg[k] = v
		}
		out[i].Config = cfg
	}
	return out
}

// ircNickChannelsChanged reports whether the IRC connector's desired nick or
// channel set moved between two specs.
func ircNickChannelsChanged(prev, next TenantSpec) (nick, channels bool) {
	p := firstIRCConnectorSpec(prev.Connectors)
	n := firstIRCConnectorSpec(next.Connectors)
	nick = stringField(p.Config, "nick") != stringField(n.Config, "nick")
	channels = !reflect.DeepEqual(stringSlice(p.Config, "channels"), stringSlice(n.Config, "channels"))
	return nick, channels
}

// ircSuspendChanged reports whether the IRC connector's user connect/disconnect
// intent moved between two specs.
func ircSuspendChanged(prev, next TenantSpec) bool {
	p := firstIRCConnectorSpec(prev.Connectors)
	n := firstIRCConnectorSpec(next.Connectors)
	return boolField(p.Config, "suspended", false) != boolField(n.Config, "suspended", false)
}

// applySuspend applies the spec's desired connect/disconnect intent to the live
// connector without a respawn: Suspend parks the upstream link, Resume
// reconnects, neither touches the bouncer / web gateway. Returns false when the
// tenant isn't running (no live connector), so the caller falls back to a
// restart that re-seeds the intent at build time via SetInitialSuspended.
func (t *Tenant) applySuspend(spec TenantSpec) bool {
	t.mu.Lock()
	conn := t.ircConn
	t.mu.Unlock()
	if conn == nil {
		return false
	}
	cs := firstIRCConnectorSpec(spec.Connectors)
	if boolField(cs.Config, "suspended", false) {
		conn.Suspend()
	} else {
		conn.Resume()
	}
	return true
}

// applyNickChannels applies the spec's desired nick and channels to the live
// connector without a reconnect. Returns false when the tenant isn't running
// (no live connector), so the caller falls back to a restart.
func (t *Tenant) applyNickChannels(spec TenantSpec) bool {
	t.mu.Lock()
	conn := t.ircConn
	t.mu.Unlock()
	if conn == nil {
		return false
	}
	cs := firstIRCConnectorSpec(spec.Connectors)
	conn.ApplyNick(stringField(cs.Config, "nick"))
	conn.ReconcileChannels(stringSlice(cs.Config, "channels"))
	return true
}

// capsWithoutTokenUsage returns a copy of caps with the live LLM-usage baseline
// fields zeroed, so spec comparisons can ignore them. Returns nil unchanged
// (never dereferences a nil pointer or mutates the caller's struct).
func capsWithoutTokenUsage(caps *PlanCapabilities) *PlanCapabilities {
	if caps == nil {
		return nil
	}
	c := *caps
	c.LLMInputTokensUsed = 0
	c.LLMOutputTokensUsed = 0
	return &c
}

// commandSetsEqual reports whether two command slices are identical.
func commandSetsEqual(a, b []skill.Skill) bool {
	return reflect.DeepEqual(a, b)
}

// reloadCommands swaps the live agent's dynamic command set for defs, rebuilt
// with the tenant's current owner-trust + LLM provider, without touching the
// connection. Returns false when the tenant isn't running (no live agent /
// connector), so the caller falls back to a restart. The per-command guards
// reuse the same owner-trust config the connector spec carries (the registry-
// wide ignore + throttle guard set at wire time is untouched).
func (t *Tenant) reloadCommands(defs []skill.Skill) bool {
	t.mu.Lock()
	a := t.agentRef
	conn := t.ircConn
	wiring := t.wiring
	cs := firstIRCConnectorSpec(t.spec.Connectors)
	ignoredNicks := t.spec.IgnoredNicks
	t.mu.Unlock()
	if a == nil || conn == nil {
		return false
	}

	owner := runtime.GuardParams{
		OwnerMode:     stringField(cs.Config, "owner_mode"),
		OwnerNick:     stringField(cs.Config, "owner_nick"),
		OwnerAccount:  stringField(cs.Config, "owner_account"),
		OwnerHostmask: stringField(cs.Config, "owner_hostmask"),
		IgnoredNicks:  ignoredNicks,
		BotNick:       conn.CurrentNick(),
	}
	// {platform} echoes the IRC server hostname; the connector spec carries it
	// as "host:port", so reuse the same split the spec→settings mapping does.
	platform, _, _ := splitNetwork(stringField(cs.Config, "network"))
	// Same in-place swap the single-instance runtime's refresher uses: command
	// registry + skill engine + scheduler, all atomic, no reconnect.
	runtime.ApplySkills(a, wiring, defs, t.llmProvider, owner, platform, t.log)
	return true
}

// reloadFlows swaps the live tenant's node-graph flow set in place — the same
// atomic, no-reconnect ApplyFlows the runtime uses at build time — without
// touching the connection. Returns false when the tenant isn't running (no live
// wiring), so the caller falls back to a restart that re-seeds flows at build.
func (t *Tenant) reloadFlows(flows []flow.Flow) bool {
	t.mu.Lock()
	wiring := t.wiring
	t.mu.Unlock()
	if wiring == nil {
		return false
	}
	runtime.ApplyFlows(wiring, flows)
	return true
}

// reloadGuard rebuilds the registry-wide guard (ignore list + per-sender
// command throttle) and swaps it onto the live agent without a reconnect, so an
// ignore-list edit takes effect in place. The throttle window is sourced from
// the current caps so it is preserved across the swap. Returns false when the
// tenant isn't running (no live agent), so the caller falls back to a restart.
func (t *Tenant) reloadGuard() bool {
	t.mu.Lock()
	a := t.agentRef
	caps := t.spec.PlanCapabilities
	ignoredNicks := t.spec.IgnoredNicks
	t.mu.Unlock()
	if a == nil {
		return false
	}

	var cmdMax, cmdWin int
	if caps != nil {
		cmdMax, cmdWin = caps.CommandMaxPerWindow, caps.CommandWindowSeconds
	}
	runtime.ApplyRegistryGuard(a, runtime.GuardParams{
		IgnoredNicks:         ignoredNicks,
		CommandMaxPerWindow:  cmdMax,
		CommandWindowSeconds: cmdWin,
	})
	return true
}

// firstIRCConnectorSpec returns the tenant's IRC connector spec (the only
// connector type pooled mode runs today), or a zero spec when none is present.
func firstIRCConnectorSpec(conns []ConnectorSpec) ConnectorSpec {
	for _, c := range conns {
		if c.Type == "irc" {
			return c
		}
	}
	return ConnectorSpec{}
}

// stop cancels the tenant and waits for its goroutine to drain.
func (t *Tenant) stop() {
	t.cancel()
	<-t.done
}

// ServeBouncerConn delivers one already-accepted client connection to this
// tenant's live IRC bouncer (the pooled router calls it after resolving the
// tenant from the PROXY-v2 authority). Closes the connection when the tenant
// has no running IRC connector (between runs / quarantined / no IRC connector
// configured); the client reconnects and the router retries.
func (t *Tenant) ServeBouncerConn(conn net.Conn) {
	t.mu.Lock()
	c := t.ircConn
	t.mu.Unlock()
	if c == nil {
		_ = conn.Close()
		return
	}
	c.ServeBouncerConn(conn)
}

// ServeWS hands one HTTP request (the web shell's `/ws` upgrade, addressed
// as `/c/<id>`) to this tenant's live web gateway. The web router calls
// it after resolving the tenant from the request path. Returns 404 when the
// tenant has no running gateway (between runs / quarantined / no web shell
// configured); the browser reconnects and the router retries. The request's
// `?token=` query is preserved — the shared gateway handler does auth + upgrade.
func (t *Tenant) ServeWS(w http.ResponseWriter, r *http.Request) {
	t.mu.Lock()
	gw := t.gateway
	t.mu.Unlock()
	if gw == nil {
		http.Error(w, "no web shell for tenant", http.StatusNotFound)
		return
	}
	// The shared mux routes on `/ws`; rewrite the path the router matched
	// (`/c/<id>`) to it. Query (the token) is untouched.
	r.URL.Path = "/ws"
	gw.Handler().ServeHTTP(w, r)
}

// ServeHook hands one inbound-webhook request (POST /c/<id>/hook/<name>) to this
// tenant's live web gateway. The web router calls it after resolving the tenant
// from the request path. Returns 404 when the tenant has no running gateway
// (between runs / quarantined / no web shell configured). The gateway's shared
// handler does the token auth, body cap, and trigger dispatch; the `{name}` path
// segment the router matched is preserved by rewriting to the gateway's own
// `/hook/<name>` route so PathValue resolves there.
func (t *Tenant) ServeHook(w http.ResponseWriter, r *http.Request) {
	t.mu.Lock()
	gw := t.gateway
	t.mu.Unlock()
	if gw == nil {
		http.Error(w, "no web shell for tenant", http.StatusNotFound)
		return
	}
	r.URL.Path = "/hook/" + r.PathValue("name")
	gw.Handler().ServeHTTP(w, r)
}

// connectorTypes lists the configured connector types, for logging.
func (t *Tenant) connectorTypes() []string {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]string, 0, len(t.spec.Connectors))
	for _, c := range t.spec.Connectors {
		out = append(out, c.Type)
	}
	return out
}
