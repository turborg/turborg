package server

import (
	"context"
	"log/slog"
	"sync"
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

// run is the tenant's supervised lifecycle. M1: no connector behaviour —
// it logs attach, blocks until cancellation, then logs detach. Later
// milestones replace the idle wait with the connector run loop.
func (t *Tenant) run(ctx context.Context) {
	defer close(t.done)
	t.log.Info("tenant attached", "connectors", t.connectorTypes())
	<-ctx.Done()
	t.log.Info("tenant detaching")
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
