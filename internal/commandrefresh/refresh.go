// Package commandrefresh keeps a single dedicated agent's data-driven command
// set current while it runs, without a reconnect.
//
// A dedicated agent boots with the command set baked into TURBORG_COMMANDS.
// That set is fixed for the life of the process unless something tells the
// running agent to reload it. This package polls an endpoint for the tenant's
// current command set and, when it changes, applies it in place via a
// caller-supplied callback (which rebuilds the handlers + ReplaceDynamic). It
// is the dedicated-runtime mirror of what the pooled runtime already does from
// its tenant feed, so the two share identical hot-reload semantics (an atomic
// in-place swap — no IRC reconnect).
//
// Wire contract the operator must serve at the configured URL:
//
//	GET <url>
//	  Authorization: Bearer <token>
//	  Status: 200 on success.
//	  Body: a JSON array of command definitions, the same wire shape as
//	        TURBORG_COMMANDS / the pooled tenant feed's `commands`:
//	        [{"name","type","template","instructions","access","allowlist"}, ...]
//
// Failures are non-fatal: the last applied set stays in force and the loop
// retries on the next tick.
package commandrefresh

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"reflect"
	"time"

	"github.com/turborg/turborg/internal/commands"
)

const (
	defaultInterval = 15 * time.Second
	minInterval     = 2 * time.Second
	requestTimeout  = 10 * time.Second
)

// Apply installs a new command set on the live agent. The runtime supplies a
// closure that rebuilds the handlers (with the agent's provider + owner-trust)
// and swaps them via CommandRegistry.ReplaceDynamic.
type Apply func(defs []commands.Definition)

// Refresher periodically pulls the tenant's command set and applies changes.
type Refresher struct {
	endpoint string
	token    string
	interval time.Duration
	apply    Apply
	client   *http.Client
	log      *slog.Logger

	last     []commands.Definition
	haveLast bool
}

// New returns nil when refresh is not configured (no endpoint/token or no
// apply callback) — so the caller can plug the result into an optional field
// and gate the feature with a single non-nil check. intervalSeconds <= 0 uses
// the default; values below the floor are clamped up.
func New(endpoint, token string, intervalSeconds int, apply Apply, log *slog.Logger) *Refresher {
	if endpoint == "" || token == "" || apply == nil {
		return nil
	}
	if log == nil {
		log = slog.Default()
	}
	interval := time.Duration(intervalSeconds) * time.Second
	switch {
	case intervalSeconds <= 0:
		interval = defaultInterval
	case interval < minInterval:
		interval = minInterval
	}
	return &Refresher{
		endpoint: endpoint,
		token:    token,
		interval: interval,
		apply:    apply,
		client:   &http.Client{Timeout: requestTimeout},
		log:      log.With("component", "command-refresh"),
	}
}

// Run reloads once immediately, then on every interval tick, until ctx is
// cancelled. Always returns nil (cancellation is the normal exit); the error
// return exists for symmetry with the other runnables the runtime supervises.
func (r *Refresher) Run(ctx context.Context) error {
	r.refreshOnce(ctx)
	t := time.NewTicker(r.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-t.C:
			r.refreshOnce(ctx)
		}
	}
}

func (r *Refresher) refreshOnce(ctx context.Context) {
	defs, err := r.fetch(ctx)
	if err != nil {
		// Keep the last applied set; a brief stale command set is safe.
		r.log.Debug("command refresh failed; keeping last set", "err", err)
		return
	}
	// Skip the swap when nothing changed, mirroring the pooled runtime's
	// commandSetsEqual gate so an unchanged poll is a no-op.
	if r.haveLast && reflect.DeepEqual(r.last, defs) {
		return
	}
	r.apply(defs)
	r.last = defs
	r.haveLast = true
	r.log.Info("commands reloaded in place", "commands", len(defs))
}

func (r *Refresher) fetch(ctx context.Context) ([]commands.Definition, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.endpoint, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+r.token)

	resp, err := r.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status %d", resp.StatusCode)
	}

	var defs []commands.Definition
	if err := json.NewDecoder(resp.Body).Decode(&defs); err != nil {
		return nil, fmt.Errorf("decode body: %w", err)
	}
	return defs, nil
}
