package server

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	"github.com/turborg/turborg/internal/agent"
	"github.com/turborg/turborg/internal/connector/irc"
)

// Tenant is one isolated agent inside the pooled process. M1 is lifecycle
// only: it owns a cancellable context and a supervised goroutine that starts,
// idles until cancelled, and stops cleanly. Connector wiring (M2), crash
// isolation (M3), and per-tenant limits (M4) build on this shell.
//
// Discipline rule (enforced from M2 on): anything a Tenant holds is
// tenant-owned; shared subsystems live on the Server. A Tenant must never
// reference another tenant's state.
type Tenant struct {
	ID  string
	log *slog.Logger

	cancel context.CancelFunc
	done   chan struct{}

	mu   sync.Mutex
	spec TenantSpec
}

// startTenant launches a tenant goroutine under a child of parent. The
// returned Tenant is running; call stop() to drain it.
func startTenant(parent context.Context, spec TenantSpec, log *slog.Logger) *Tenant {
	ctx, cancel := context.WithCancel(parent)
	t := &Tenant{
		ID:     spec.TurborgID,
		log:    log.With("turborg_id", spec.TurborgID),
		cancel: cancel,
		done:   make(chan struct{}),
		spec:   spec,
	}
	go t.run(ctx)
	return t
}

// run is the tenant's supervised lifecycle. M2: build a per-tenant agent,
// attach this tenant's connectors to it, and run it under the tenant's
// context. The agent + its connectors + their event bus are entirely
// tenant-owned — nothing is shared with other tenants (the isolation rule).
//
// If the agent run loop returns before cancellation (e.g. a tenant with no
// runnable connectors), the tenant still idles until cancelled so the
// Server's view of attached tenants stays consistent.
func (t *Tenant) run(ctx context.Context) {
	defer close(t.done)

	a := agent.New(t.log)
	t.buildConnectors(a)

	t.log.Info("tenant attached", "connectors", t.connectorTypes())
	if err := a.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		t.log.Error("tenant agent stopped with error", "err", err)
	}
	if ctx.Err() == nil {
		<-ctx.Done()
	}
	t.log.Info("tenant detaching")
}

// buildConnectors constructs this tenant's connectors from its spec and
// registers them on the tenant-owned agent. Unsupported types are logged and
// skipped (pooled mode ships connectors incrementally). A connector whose
// spec is invalid is skipped rather than failing the whole tenant.
func (t *Tenant) buildConnectors(a *agent.Agent) {
	t.mu.Lock()
	connectors := t.spec.Connectors
	t.mu.Unlock()

	for _, cs := range connectors {
		switch cs.Type {
		case "irc":
			settings, err := settingsFromConnectorSpec(cs)
			if err != nil {
				t.log.Error("skipping invalid irc connector", "err", err)
				continue
			}
			a.AddConnector(irc.New(settings, t.log, nil))
		default:
			t.log.Warn("connector type not supported in pooled mode yet", "type", cs.Type)
		}
	}
}

// update applies a new desired spec to a running tenant. M1 swaps the stored
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
