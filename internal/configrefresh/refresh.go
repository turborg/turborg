// Package configrefresh keeps a single-instance agent's live IRC nick and
// channel set current while it runs, without a reconnect.
//
// A single-instance agent boots with its nick + channels baked into
// TURBORG_IRC_NICK / TURBORG_IRC_CHANNELS. This package polls an endpoint for
// the current desired values and, when they change, applies them in place via
// a caller-supplied callback (ApplyNick + ReconcileChannels on the live
// connector). It is the single-instance mirror of what the pooled runtime
// already does from its tenant feed, so both apply nick/channel changes with
// no IRC reconnect.
//
// Wire contract the operator must serve at the configured URL:
//
//	GET <url>
//	  Authorization: Bearer <token>
//	  Status: 200 on success.
//	  Body: {"nick": "<nick>", "channels": ["#a", "#b", ...], "suspended": false}
//
// Failures are non-fatal: the last applied config stays in force and the loop
// retries on the next tick.
package configrefresh

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"reflect"
	"time"
)

const (
	defaultInterval = 15 * time.Second
	minInterval     = 2 * time.Second
	requestTimeout  = 10 * time.Second
)

// Config is the live-applicable IRC config the endpoint serves.
type Config struct {
	Nick     string   `json:"nick"`
	Channels []string `json:"channels"`
	// Suspended is the user's desired connect/disconnect intent. true parks
	// the upstream link (Suspend); false reconnects (Resume). Absent decodes
	// to false (connected), so an old control plane that omits the field keeps
	// the connector connected.
	Suspended bool `json:"suspended"`
}

// Apply installs a freshly-fetched config on the live connector. The runtime
// supplies a closure that calls ApplyNick + ReconcileChannels.
type Apply func(cfg Config)

// Refresher periodically pulls the desired nick/channels and applies changes.
type Refresher struct {
	endpoint string
	token    string
	interval time.Duration
	apply    Apply
	client   *http.Client
	log      *slog.Logger

	last     Config
	haveLast bool
}

// New returns nil when refresh is not configured (no endpoint/token or no
// apply callback) so the caller can gate the feature with a single non-nil
// check. intervalSeconds <= 0 uses the default; values below the floor are
// clamped up.
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
		log:      log.With("component", "config-refresh"),
	}
}

// Run reloads once immediately, then on every interval tick, until ctx is
// cancelled. Always returns nil (cancellation is the normal exit).
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
	cfg, err := r.fetch(ctx)
	if err != nil {
		// Keep the last applied config; a brief stale nick/channel set is safe.
		r.log.Debug("config refresh failed; keeping last", "err", err)
		return
	}
	// Skip the apply when nothing changed, so an unchanged poll is a no-op.
	if r.haveLast && reflect.DeepEqual(r.last, cfg) {
		return
	}
	r.apply(cfg)
	r.last = cfg
	r.haveLast = true
	r.log.Info("irc config reloaded in place", "nick", cfg.Nick, "channels", len(cfg.Channels))
}

func (r *Refresher) fetch(ctx context.Context) (Config, error) {
	var cfg Config
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, r.endpoint, nil)
	if err != nil {
		return cfg, err
	}
	req.Header.Set("Authorization", "Bearer "+r.token)

	resp, err := r.client.Do(req)
	if err != nil {
		return cfg, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return cfg, fmt.Errorf("unexpected status %d", resp.StatusCode)
	}
	if err := json.NewDecoder(resp.Body).Decode(&cfg); err != nil {
		return cfg, fmt.Errorf("decode body: %w", err)
	}
	return cfg, nil
}
