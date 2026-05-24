package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"runtime/debug"
	"sync"
	"time"

	"github.com/turborg/turborg/internal/agent"
	"github.com/turborg/turborg/internal/connector/irc"
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

	mu              sync.Mutex
	spec            TenantSpec
	status          TenantStatus
	failures        int
	lastErr         error
	quarantineUntil time.Time
}

// startTenant launches a self-supervising tenant under a child of parent.
// workFactory builds the body to run (and re-run after a panic) from the
// constructed Tenant; quarantineBase is the first backoff step (doubled per
// consecutive failure, capped).
func startTenant(parent context.Context, spec TenantSpec, log *slog.Logger, quarantineBase time.Duration, workFactory func(*Tenant) func(context.Context) error) *Tenant {
	ctx, cancel := context.WithCancel(parent)
	t := &Tenant{
		ID:             spec.TurborgID,
		log:            log.With("turborg_id", spec.TurborgID),
		quarantineBase: quarantineBase,
		cancel:         cancel,
		done:           make(chan struct{}),
		spec:           spec,
		status:         StatusRunning,
	}
	t.work = workFactory(t)
	go t.supervise(ctx)
	return t
}

// supervise runs the tenant's work loop, recovering panics and quarantining
// (with exponential backoff) before reviving. Returns when ctx is cancelled.
func (t *Tenant) supervise(ctx context.Context) {
	defer close(t.done)

	for {
		if ctx.Err() != nil {
			return
		}

		panicked := t.runOnce(ctx)

		if ctx.Err() != nil {
			return
		}

		if !panicked {
			// Work returned without panic and the tenant wasn't cancelled
			// (e.g. no runnable connectors). Idle until cancelled so the
			// Server's view of attached tenants stays consistent.
			<-ctx.Done()
			return
		}

		backoff := t.enterQuarantine()
		t.log.Warn("tenant quarantined after panic", "backoff", backoff, "failures", t.Failures())
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
			t.markRunning()
			t.log.Info("reviving quarantined tenant")
		}
	}
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
		a := agent.New(t.log)
		t.buildConnectors(a)
		t.log.Info("tenant attached", "connectors", t.connectorTypes())
		return a.Run(ctx)
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
	t.mu.Unlock()

	for _, cs := range connectors {
		switch cs.Type {
		case "irc":
			settings, err := settingsFromConnectorSpec(cs)
			if err != nil {
				t.log.Error("skipping invalid irc connector", "err", err)
				continue
			}
			conn := irc.New(settings, t.log, nil)
			if err := applyPlanLimits(conn, caps); err != nil {
				t.log.Error("skipping irc connector: invalid plan limits", "err", err)
				continue
			}
			a.AddConnector(conn)
		default:
			t.log.Warn("connector type not supported in pooled mode yet", "type", cs.Type)
		}
	}
}

// update applies a new desired spec to a running tenant. M3 swaps the stored
// spec and logs; connector-aware diffing (JOIN/PART, nick, rate limits) is
// the hot-reload milestone (M7).
func (t *Tenant) update(spec TenantSpec) {
	t.mu.Lock()
	t.spec = spec
	t.mu.Unlock()
	t.log.Info("tenant spec updated", "connectors", t.connectorTypes())
}

// stop cancels the tenant and waits for its goroutine to drain.
func (t *Tenant) stop() {
	t.cancel()
	<-t.done
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
