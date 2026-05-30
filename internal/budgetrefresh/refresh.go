// Package budgetrefresh keeps an LLM token budget's account baseline current
// for a single long-lived agent process.
//
// The in-process TokenBudget counts exactly what THIS process consumes, but
// the cap is meant to hold across the whole account/window — including sibling
// agents and this agent's own pre-restart usage. Those live in the control
// plane, not in memory. This package polls the control plane (via the local
// sidecar, the same transport the message store uses) and pushes the latest
// "everyone else" total into the budget as its baseline.
//
// Wire contract the operator must serve at the configured URL:
//
//	GET <url>?since=<unix_seconds>
//	  Authorization: Bearer <token>
//	  Status: 200 on success.
//	  Body: {"input_used": int, "output_used": int}
//	  - The response MUST exclude this agent's own usage recorded at or after
//	    `since` (this process's start), because that half is counted locally —
//	    so the two never double-count. Usage before `since` (prior incarnations
//	    of the same agent) stays included, which is what keeps a restart from
//	    resetting the window.
//
// Failures are non-fatal: the budget keeps its last baseline and the loop
// retries on the next tick. The baseline is a conservative seed, never the
// only thing standing between the agent and a runaway bill within a single
// process (local counting still applies), so a brief stale baseline is safe.
package budgetrefresh

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"time"
)

// baselineSetter is the slice of *llm.TokenBudget this package needs. Kept as
// an interface so the package doesn't import llm and stays trivially testable.
type baselineSetter interface {
	SetBaseline(input, output int)
}

const (
	defaultInterval = 15 * time.Second
	minInterval     = 2 * time.Second
	requestTimeout  = 10 * time.Second
)

// Refresher periodically pulls the account baseline and applies it to a budget.
type Refresher struct {
	endpoint string
	token    string
	interval time.Duration
	since    time.Time
	budget   baselineSetter
	client   *http.Client
	log      *slog.Logger
}

// New returns nil when refresh is not configured (no endpoint/token or no
// budget) — so the caller can plug the result straight into an optional field
// and a single non-nil check gates the whole feature. `since` is this
// process's start; it's sent on every request so the control plane can exclude
// usage this process counts locally. intervalSeconds <= 0 uses the default;
// values below the floor are clamped up.
func New(endpoint, token string, intervalSeconds int, since time.Time, budget baselineSetter, log *slog.Logger) *Refresher {
	if endpoint == "" || token == "" || budget == nil {
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
		since:    since,
		budget:   budget,
		client:   &http.Client{Timeout: requestTimeout},
		log:      log.With("component", "budget-refresh"),
	}
}

// Run refreshes once immediately, then on every interval tick, until ctx is
// cancelled. Always returns nil (cancellation is the normal exit); the error
// return exists for symmetry with the other runnables the runtime supervises.
func (r *Refresher) Run(ctx context.Context) error {
	r.refreshOnce(ctx) // tighten accuracy at boot rather than wait a full tick
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
	in, out, err := r.fetch(ctx)
	if err != nil {
		// Keep the last baseline; local counting still bounds this process.
		r.log.Debug("budget baseline refresh failed; keeping last value", "err", err)
		return
	}
	r.budget.SetBaseline(in, out)
}

func (r *Refresher) fetch(ctx context.Context) (input, output int, err error) {
	u, err := url.Parse(r.endpoint)
	if err != nil {
		return 0, 0, fmt.Errorf("parse endpoint: %w", err)
	}
	q := u.Query()
	q.Set("since", strconv.FormatInt(r.since.Unix(), 10))
	u.RawQuery = q.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		return 0, 0, err
	}
	req.Header.Set("Authorization", "Bearer "+r.token)

	resp, err := r.client.Do(req)
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return 0, 0, fmt.Errorf("unexpected status %d", resp.StatusCode)
	}

	var body struct {
		InputUsed  int `json:"input_used"`
		OutputUsed int `json:"output_used"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return 0, 0, fmt.Errorf("decode body: %w", err)
	}
	return body.InputUsed, body.OutputUsed, nil
}
