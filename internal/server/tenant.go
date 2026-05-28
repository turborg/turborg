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
	"sync"
	"time"

	"github.com/turborg/turborg/internal/agent"
	"github.com/turborg/turborg/internal/connector/irc"
	"github.com/turborg/turborg/internal/llm"
	"github.com/turborg/turborg/internal/messages"
	"github.com/turborg/turborg/internal/runtime"
	"github.com/turborg/turborg/internal/safe"
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
	// gateway is the live web shell for the current run, built in
	// buildConnectors when the spec carries a GatewayToken and cleared when the
	// run ends. The web router reaches it via ServeWS to upgrade an attached
	// browser client. nil between runs / when the tenant has no web shell → an
	// inbound WS request is closed with 404.
	gateway *web.Gateway
	// stateEmitter mirrors this tenant's connector state (upstream status,
	// channels, nick) to the control plane on every transition, so accounts-api
	// drives appui's connector pill for pooled tenants the same way it does for
	// dedicated. Inert when no control plane is configured; Stop()ped at run end.
	stateEmitter *statepush.Emitter

	// controlPlaneURL/Token are where the state emitter POSTs (per tenant:
	// <url>/turborgs/<id>/state). Set once at construction.
	controlPlaneURL   string
	controlPlaneToken string

	// llmProvider is the shared !ask provider handed down from the Server.
	// Nil → !ask is not registered for this tenant.
	llmProvider llm.Provider
}

// startTenant launches a self-supervising tenant under a child of parent.
// workFactory builds the body to run (and re-run after a panic) from the
// constructed Tenant; quarantineBase is the first backoff step (doubled per
// consecutive failure, capped).
func startTenant(parent context.Context, spec TenantSpec, log *slog.Logger, quarantineBase time.Duration, workFactory func(*Tenant) func(context.Context) error, controlPlaneURL, controlPlaneToken string, llmProvider llm.Provider) *Tenant {
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
		// NewWithPrefix defaults an empty prefix to "!", matching the dedicated
		// path (where the env default fills it) so free tenants behave the same.
		a := agent.NewWithPrefix(t.log, prefix)
		t.buildConnectors(a)
		t.log.Info("tenant attached", "connectors", t.connectorTypes())
		err := a.Run(ctx)
		// Run ended (ctx cancelled / restart) — the connector's bouncer, web
		// gateway, and state emitter are stopping, so drop the handles. A late
		// inbound conn/WS then closes cleanly instead of hitting a torn-down run,
		// and the emitter's goroutine is drained (goleak-clean across restarts).
		t.mu.Lock()
		gw := t.gateway
		em := t.stateEmitter
		t.ircConn = nil
		t.gateway = nil
		t.stateEmitter = nil
		t.mu.Unlock()
		if gw != nil {
			gw.Stop()
		}
		em.Stop() // safe on nil
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
	t.mu.Unlock()

	for _, cs := range connectors {
		switch cs.Type {
		case "irc":
			settings, err := settingsFromConnectorSpec(cs)
			if err != nil {
				t.log.Error("skipping invalid irc connector", "err", err)
				continue
			}
			// Wire the connector to the tenant-owned agent bus: the bouncer's
			// outbound observer and the connector's join/part/topic/state events
			// publish here, and the web gateway subscribes to the same bus so the
			// web shell sees the full event stream (not just plain messages,
			// which arrive via the agent's inbound drain regardless).
			conn := irc.New(settings, t.log, a.Events)
			// Pooled runtime: the bouncer must not bind its own port — the pool
			// router feeds it connections via ServeBouncerConn after PROXY-v2
			// tenant resolution. Set before WireCommon adds the connector.
			conn.SetBouncerListenerless(true)

			// The shared, connector-agnostic wiring — identical to the dedicated
			// runtime, which calls the same runtime.WireCommon. Gives pooled
			// tenants builtins (!ping/!version/!ask), the owner command guard,
			// plan limits + outbound throttle, and message-store submitters, so
			// the two modes can't drift. Owner-trust config travels in the
			// connector config (the feed reuses the same connectorList() shape
			// the dedicated spawn payload does).
			if err := runtime.WireCommon(a, conn, t.commonParams(cs, caps, settings.Nick), t.log); err != nil {
				t.log.Error("skipping irc connector: wiring failed", "err", err)
				continue
			}

			t.mu.Lock()
			t.ircConn = conn
			t.mu.Unlock()

			// Connector-state sync: mirror upstream status / channels / nick to
			// the control plane on every transition, so appui's connector pill
			// works for pooled tenants the same way dedicated does (via the
			// single-tenant runtime emitter). Inert + nil when no control plane.
			if em := buildTenantStateEmitter(conn, t.ID, t.controlPlaneURL, t.controlPlaneToken, t.log); em != nil {
				t.mu.Lock()
				t.stateEmitter = em
				t.mu.Unlock()
			}

			// Web shell: built only when the tenant carries a gateway token (the
			// plan's `gatewayEnabled` capability is expressed by accounts-api
			// emitting the token at all). Listenerless like the bouncer — the web
			// router dispatches `/c/<id>` to the shared Handler via ServeWS, so the
			// gateway never binds its own port.
			if gatewayToken != "" {
				gw, err := buildTenantGateway(conn, gatewayToken, t.log)
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

// commonParams maps a tenant's IRC connector spec + plan caps onto the
// runtime.CommonParams the shared agent wiring consumes. Owner-trust fields
// live in the connector config (the feed reuses the dedicated connectorList()
// shape); identity limits + the outbound throttle come from the plan caps; the
// LLM provider is the pool-process-wide shared one.
//
// Fields the pooled feed doesn't carry yet — custom-commands cap, per-sender
// command throttle, owner-DM nudge interval, ignored nicks, and a durable
// message store — default to their zero values here (builtins-only, no
// throttle, no nudge, in-process MemoryStore). Phase 3 extends the feed +
// control plane to fill them, at which point this is the single place to map
// them through.
func (t *Tenant) commonParams(cs ConnectorSpec, caps *PlanCapabilities, botNick string) runtime.CommonParams {
	var limits irc.ClientLimits
	var outMax, outWin int
	if caps != nil {
		limits = irc.ClientLimits{
			NickLocked:     caps.NickLocked,
			RealnameLocked: caps.RealnameLocked,
			MaxChannels:    caps.MaxChannels,
		}
		outMax, outWin = caps.OutboundMsgsPerWindow, caps.OutboundWindowSeconds
	}
	return runtime.CommonParams{
		Limits: limits,
		Owner: runtime.GuardParams{
			OwnerMode:     stringField(cs.Config, "owner_mode"),
			OwnerNick:     stringField(cs.Config, "owner_nick"),
			OwnerAccount:  stringField(cs.Config, "owner_account"),
			OwnerHostmask: stringField(cs.Config, "owner_hostmask"),
			BotNick:       botNick,
		},
		OutboundMaxPerWindow:  outMax,
		OutboundWindowSeconds: outWin,
		OwnerNick:             stringField(cs.Config, "owner_nick"),
		LLM:                   t.llmProvider,
		Store:                 messages.NewMemoryStore(0),
	}
}

// update applies a new desired spec to a running tenant (M7, conservative
// hot reload). When the spec actually changed it restarts the tenant's run so
// the new connectors/limits take effect; an identical spec is a no-op.
//
// Conservative by design: any change triggers a full reconnect rather than a
// surgical JOIN/PART. The plan's aggressive in-place reload (no reconnect on
// channel-only edits) is a later refinement; reconnect-on-change is correct
// and simple, and avoids silent state divergence.
func (t *Tenant) update(spec TenantSpec) {
	t.mu.Lock()
	unchanged := reflect.DeepEqual(t.spec, spec)
	if unchanged {
		t.mu.Unlock()
		return
	}
	t.spec = spec
	cancel := t.runCancel
	t.mu.Unlock()

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

// ServeWS hands one HTTP request (the appui web shell's `/ws` upgrade, addressed
// as `/c/<turborg_id>`) to this tenant's live web gateway. The web router calls
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
