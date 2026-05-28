package server

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"
)

// activityFlushInterval is how often the pool flushes its coalesced active-
// tenant set to the control plane. Coarse on purpose: last_active_at only gates
// the multi-day idle reaper, so minute granularity is ample and keeps the
// control-plane write rate to one bulk POST per interval per pool — not one per
// agent message across thousands of tenants.
const activityFlushInterval = 60 * time.Second

// activityHTTPTimeout bounds a single bulk-flush POST.
const activityHTTPTimeout = 10 * time.Second

// httpDoer is the slice of *http.Client the aggregator needs; swapped in tests.
type httpDoer interface {
	Do(*http.Request) (*http.Response, error)
}

// activityAggregator coalesces per-tenant activity signals into a coarse
// periodic bulk POST to the control plane's /turborgs/activity endpoint.
//
// Dedicated containers report activity to their host's sidecar (in-memory),
// which accounts-api pulls onto last_active_at. Pooled tenants have no sidecar
// hop, so the pool batches active tenant IDs here and flushes them on a timer.
// Without it a pooled tenant's last_active_at would never refresh and the idle
// reaper would kill an actively-used free tenant. Inert (Mark is a no-op, run
// returns immediately) when no control plane is configured.
type activityAggregator struct {
	url    string
	token  string
	client httpDoer
	log    *slog.Logger

	mu     sync.Mutex
	active map[string]struct{}
}

// newActivityAggregator returns an aggregator posting to
// <controlPlaneURL>/turborgs/activity, or nil when no control plane is set
// (the OSS/file-source path) so activity reporting is simply off.
func newActivityAggregator(controlPlaneURL, token string, log *slog.Logger) *activityAggregator {
	if controlPlaneURL == "" {
		return nil
	}
	if log == nil {
		log = slog.Default()
	}
	return &activityAggregator{
		url:    strings.TrimRight(controlPlaneURL, "/") + "/turborgs/activity",
		token:  token,
		client: &http.Client{Timeout: activityHTTPTimeout},
		log:    log,
		active: map[string]struct{}{},
	}
}

// Mark records a tenant as active since the last flush. Safe on a nil receiver
// (the no-control-plane path) and from many goroutines.
func (a *activityAggregator) Mark(turborgID string) {
	if a == nil || turborgID == "" {
		return
	}
	a.mu.Lock()
	a.active[turborgID] = struct{}{}
	a.mu.Unlock()
}

// run flushes the active set on a ticker until ctx is cancelled. Nil receiver
// returns immediately. The last sub-interval of activity is intentionally not
// flushed on shutdown: last_active_at was bumped at most one interval ago, which
// is irrelevant to the multi-day idle window, and skipping it keeps shutdown
// from blocking on a network call.
func (a *activityAggregator) run(ctx context.Context) {
	if a == nil {
		return
	}
	ticker := time.NewTicker(activityFlushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			a.flush(ctx)
		}
	}
}

// drain atomically swaps out the active set, returning the IDs to flush.
func (a *activityAggregator) drain() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	if len(a.active) == 0 {
		return nil
	}
	ids := make([]string, 0, len(a.active))
	for id := range a.active {
		ids = append(ids, id)
	}
	a.active = map[string]struct{}{}
	return ids
}

func (a *activityAggregator) flush(ctx context.Context) {
	ids := a.drain()
	if len(ids) == 0 {
		return
	}
	body, err := json.Marshal(map[string][]string{"turborg_ids": ids})
	if err != nil {
		a.log.Debug("activity marshal", "err", err)
		return
	}
	reqCtx, cancel := context.WithTimeout(ctx, activityHTTPTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, http.MethodPost, a.url, bytes.NewReader(body))
	if err != nil {
		a.log.Debug("activity request", "err", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	if a.token != "" {
		req.Header.Set("Authorization", "Bearer "+a.token)
	}
	resp, err := a.client.Do(req)
	if err != nil {
		a.log.Debug("activity flush", "err", err, "count", len(ids))
		return
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 400 {
		a.log.Debug("activity flush non-2xx", "status", resp.StatusCode, "count", len(ids))
	}
}
