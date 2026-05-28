package server

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"sort"
	"sync"
	"time"

	"github.com/turborg/turborg/internal/llm"
	"github.com/turborg/turborg/internal/safe"
)

// defaultQuarantineBase is the first backoff step a tenant waits after a
// panic before the supervisor revives it (doubled per consecutive failure).
const defaultQuarantineBase = time.Second

// Server is the pooled runtime: it reconciles the live set of running
// Tenants against a TenantSource. One instance per process.
type Server struct {
	source TenantSource
	log    *slog.Logger

	// quarantineBase is the first crash-backoff step handed to each tenant.
	quarantineBase time.Duration
	// workFactory builds a tenant's work body. Defaults to the production
	// agent run; overridable in tests to inject panics or controllable work.
	workFactory func(*Tenant) func(context.Context) error

	// controlPlaneURL/Token point each pooled tenant's connector-state emitter
	// at accounts-api's /v1/internal/turborgs/<id>/state receiver. Empty URL
	// (the OSS/file-source path) leaves state-sync off — the emitter is inert.
	controlPlaneURL   string
	controlPlaneToken string

	// llmProvider powers the !ask builtin for every pooled tenant. One shared
	// stateless provider (built once from the pool process's own env), handed
	// to each tenant's agent wiring. Nil → !ask is not registered.
	llmProvider llm.Provider

	mu      sync.Mutex
	tenants map[string]*Tenant
}

// New builds a Server backed by the given tenant source.
func New(source TenantSource, log *slog.Logger) *Server {
	return &Server{
		source:         source,
		log:            log,
		quarantineBase: defaultQuarantineBase,
		workFactory:    func(t *Tenant) func(context.Context) error { return t.defaultWork() },
		tenants:        make(map[string]*Tenant),
	}
}

// SetControlPlane configures where pooled tenants POST their connector-state
// snapshots — accounts-api's internal receiver (TURBORG_CONTROL_PLANE_URL).
// Call before Run. An empty url leaves state-sync off (each tenant's emitter is
// inert), matching the OSS/file-source deployment that has no control plane.
func (s *Server) SetControlPlane(url, token string) {
	s.controlPlaneURL = url
	s.controlPlaneToken = token
}

// SetLLM installs the shared LLM provider that powers !ask for every tenant.
// Call before Run. Nil (the default) leaves !ask unregistered — the agent
// never fails for lack of a provider.
func (s *Server) SetLLM(p llm.Provider) {
	s.llmProvider = p
}

// Run boots every tenant from the source's initial snapshot, then applies
// streamed events until ctx is cancelled, at which point it drains all
// tenants and returns ctx.Err().
func (s *Server) Run(ctx context.Context) error {
	initial, err := s.source.Initial(ctx)
	if err != nil {
		return err
	}
	for _, spec := range initial {
		s.upsert(ctx, spec)
	}
	s.log.Info("pooled server booted", "tenants", s.Count())

	events, err := s.source.Watch(ctx)
	if err != nil {
		s.shutdown()
		return err
	}

	for {
		select {
		case <-ctx.Done():
			s.shutdown()
			return ctx.Err()
		case ev, ok := <-events:
			if !ok {
				// Source closed its stream (typically on ctx cancel).
				s.shutdown()
				if err := ctx.Err(); err != nil {
					return err
				}
				return errors.New("tenant source stream closed")
			}
			s.apply(ctx, ev)
		}
	}
}

// apply routes one source event to attach/update or detach.
func (s *Server) apply(ctx context.Context, ev TenantEvent) {
	switch ev.Kind {
	case TenantUpserted:
		s.upsert(ctx, ev.Spec)
	case TenantRemoved:
		s.remove(ev.TurborgID)
	default:
		s.log.Warn("unknown tenant event kind", "kind", int(ev.Kind))
	}
}

// upsert creates the tenant if new, or updates it in place. Idempotent:
// re-applying an identical spec is a cheap no-op update.
func (s *Server) upsert(ctx context.Context, spec TenantSpec) {
	if spec.TurborgID == "" {
		s.log.Warn("ignoring tenant spec with empty turborg_id")
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if existing, ok := s.tenants[spec.TurborgID]; ok {
		existing.update(spec)
		return
	}
	s.tenants[spec.TurborgID] = startTenant(ctx, spec, s.log, s.quarantineBase, s.workFactory, s.controlPlaneURL, s.controlPlaneToken, s.llmProvider)
}

// remove detaches a tenant, draining its goroutine. No-op when absent.
func (s *Server) remove(id string) {
	s.mu.Lock()
	t, ok := s.tenants[id]
	if ok {
		delete(s.tenants, id)
	}
	s.mu.Unlock()
	if ok {
		t.stop()
	}
}

// shutdown drains every tenant. Tenants stop concurrently; shutdown returns
// once all have drained.
func (s *Server) shutdown() {
	s.mu.Lock()
	tenants := make([]*Tenant, 0, len(s.tenants))
	for id, t := range s.tenants {
		tenants = append(tenants, t)
		delete(s.tenants, id)
	}
	s.mu.Unlock()

	var wg sync.WaitGroup
	for _, t := range tenants {
		wg.Add(1)
		safe.Go("drain/"+t.ID, func() {
			defer wg.Done()
			t.stop()
		})
	}
	wg.Wait()
	s.log.Info("pooled server drained", "tenants", len(tenants))
}

// Count returns the number of attached tenants.
func (s *Server) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.tenants)
}

// Has reports whether a tenant with the given id is attached.
func (s *Server) Has(id string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.tenants[id]
	return ok
}

// RouteBouncerConn hands one accepted client connection to the named tenant's
// bouncer. Returns false (and does not touch the conn) when no such tenant is
// attached, so the router can close it with an explanatory log. The actual
// bouncer delivery (and closing on a between-runs tenant) is the tenant's job.
func (s *Server) RouteBouncerConn(turborgID string, conn net.Conn) bool {
	s.mu.Lock()
	t, ok := s.tenants[turborgID]
	s.mu.Unlock()
	if !ok {
		return false
	}
	t.ServeBouncerConn(conn)
	return true
}

// RouteWS hands one HTTP request (the web shell's `/ws` upgrade) to the named
// tenant's web gateway. Returns false (and does not touch w) when no such tenant
// is attached, so the router can answer 404. The actual auth + WS upgrade (and
// the 404 on a between-runs tenant with no live gateway) is the tenant's job.
func (s *Server) RouteWS(turborgID string, w http.ResponseWriter, r *http.Request) bool {
	s.mu.Lock()
	t, ok := s.tenants[turborgID]
	s.mu.Unlock()
	if !ok {
		return false
	}
	t.ServeWS(w, r)
	return true
}

// Status returns a tenant's supervision phase, and whether it is attached.
func (s *Server) Status(id string) (TenantStatus, bool) {
	s.mu.Lock()
	t, ok := s.tenants[id]
	s.mu.Unlock()
	if !ok {
		return StatusRunning, false
	}
	return t.Status(), true
}

// TenantIDs returns the attached tenant ids, sorted for stable output.
func (s *Server) TenantIDs() []string {
	s.mu.Lock()
	ids := make([]string, 0, len(s.tenants))
	for id := range s.tenants {
		ids = append(ids, id)
	}
	s.mu.Unlock()
	sort.Strings(ids)
	return ids
}
