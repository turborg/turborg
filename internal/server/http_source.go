package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"reflect"
	"time"

	"github.com/turborg/turborg/internal/safe"
)

// HTTPSource feeds tenants from a control plane over HTTP — the production
// TenantSource. It fetches the snapshot of pooled tenants assigned to this host
// and watches for changes.
//
// Watch is poll-and-diff rather than SSE: it re-fetches the snapshot every
// Interval and emits the delta. That trades a few seconds of propagation
// latency for far less moving infrastructure (no long-lived stream or
// server-side broadcast) and trivially survives control-plane restarts. An
// SSE/long-poll upgrade can replace the poll loop later without changing the
// TenantSource contract.
type HTTPSource struct {
	// BaseURL is the internal API root, e.g. https://control-plane/v1/internal
	BaseURL string
	Bearer  string
	HostID  string

	// Interval is the poll cadence (default 5s).
	Interval time.Duration
	// Client is the HTTP client (default http.DefaultClient).
	Client *http.Client
	Log    *slog.Logger
}

type tenantsEnvelope struct {
	Tenants []TenantSpec `json:"tenants"`
}

// Initial fetches the current snapshot of this host's pooled tenants.
func (h *HTTPSource) Initial(ctx context.Context) ([]TenantSpec, error) {
	return h.fetch(ctx)
}

// Watch polls the snapshot on Interval and emits upsert/remove deltas. The
// returned channel closes when ctx is cancelled.
//
// The baseline is seeded synchronously before returning: the Server already
// booted the initial snapshot via Initial(), so Watch only emits changes
// observed from this point on (no redundant re-upsert of the booted set).
func (h *HTTPSource) Watch(ctx context.Context) (<-chan TenantEvent, error) {
	known := map[string]TenantSpec{}
	if specs, err := h.fetch(ctx); err == nil {
		for _, s := range specs {
			known[s.TurborgID] = s
		}
	}

	out := make(chan TenantEvent)
	safe.Go("httpsource-poll", func() { h.poll(ctx, out, known) })
	return out, nil
}

func (h *HTTPSource) poll(ctx context.Context, out chan<- TenantEvent, known map[string]TenantSpec) {
	defer close(out)

	interval := h.Interval
	if interval <= 0 {
		interval = 5 * time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			specs, err := h.fetch(ctx)
			if err != nil {
				h.logger().Warn("tenant snapshot poll failed", "err", err)
				continue
			}
			if !h.diff(ctx, known, specs, out) {
				return
			}
		}
	}
}

// diff emits upsert events for new/changed specs and remove events for specs
// that disappeared, then updates known in place. Returns false if ctx was
// cancelled mid-emit (caller should stop).
func (h *HTTPSource) diff(ctx context.Context, known map[string]TenantSpec, specs []TenantSpec, out chan<- TenantEvent) bool {
	seen := make(map[string]struct{}, len(specs))
	for _, s := range specs {
		seen[s.TurborgID] = struct{}{}
		if prev, ok := known[s.TurborgID]; ok && reflect.DeepEqual(prev, s) {
			continue
		}
		if !send(ctx, out, TenantEvent{Kind: TenantUpserted, Spec: s}) {
			return false
		}
		known[s.TurborgID] = s
	}
	for id := range known {
		if _, ok := seen[id]; ok {
			continue
		}
		if !send(ctx, out, TenantEvent{Kind: TenantRemoved, TurborgID: id}) {
			return false
		}
		delete(known, id)
	}
	return true
}

func send(ctx context.Context, out chan<- TenantEvent, ev TenantEvent) bool {
	select {
	case <-ctx.Done():
		return false
	case out <- ev:
		return true
	}
}

func (h *HTTPSource) fetch(ctx context.Context) ([]TenantSpec, error) {
	endpoint := fmt.Sprintf("%s/tenants?host_id=%s", h.BaseURL, url.QueryEscape(h.HostID))
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+h.Bearer)
	req.Header.Set("Accept", "application/json")

	resp, err := h.client().Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("tenant feed %s: status %d", endpoint, resp.StatusCode)
	}

	var env tenantsEnvelope
	if err := json.NewDecoder(resp.Body).Decode(&env); err != nil {
		return nil, fmt.Errorf("decode tenant feed: %w", err)
	}
	return env.Tenants, nil
}

func (h *HTTPSource) client() *http.Client {
	if h.Client != nil {
		return h.Client
	}
	return http.DefaultClient
}

func (h *HTTPSource) logger() *slog.Logger {
	if h.Log != nil {
		return h.Log
	}
	return slog.Default()
}
