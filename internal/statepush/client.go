// Package statepush holds the HTTP plumbing turborg uses to mirror
// authoritative per-connector state to a configured webhook endpoint.
// The contract is documented at the env-var level — operators point
// TURBORG_STATE_WEBHOOK_URL at any HTTP receiver that wants an
// idempotent overwrite-on-write view of the agent's current state.
//
// Two concerns live here:
//
//   - Client: the shared HTTP primitive (PUT with bearer auth, JSON
//     body, retry policy). Future emitters that need the same
//     idempotent-overwrite semantics can reuse it instead of
//     reimplementing the retry/auth/timeout dance.
//   - Emitter: the debounced per-connector snapshot pusher built on
//     top of Client. Subscribes to the connector's state machine,
//     wanted-channels set, and preferred-nick slot, coalescing
//     bursts into a single PUT.
//
// Both surfaces are no-ops when no webhook URL is configured —
// self-host operators who don't run an observer see zero side
// effects.
package statepush

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

// HTTPDoer is the minimal slice of *http.Client the client consumes.
// Pulled out so tests can install a stub without spinning a real
// server.
type HTTPDoer interface {
	Do(req *http.Request) (*http.Response, error)
}

// defaultHTTPTimeout caps a single PUT attempt. Retries within the
// retry policy each get their own fresh timeout — the overall
// worst-case wall time is roughly attempts*defaultHTTPTimeout +
// sum(backoff).
const defaultHTTPTimeout = 5 * time.Second

// retryBackoffs is the per-retry sleep between attempts on transient
// failure. Two retries → three total attempts (initial + 2). The
// policy is "give a flapping observer two short chances and then
// drop"; the next change will fire a fresh PUT anyway, so dropping
// a stale snapshot is safe.
var retryBackoffs = []time.Duration{1 * time.Second, 2 * time.Second}

// Client posts JSON bodies to a configured webhook URL with bearer
// auth and a small retry policy. Construct one per emitter (or share
// — Put is safe for concurrent use).
type Client struct {
	url    string
	method string
	token  string
	http   HTTPDoer
	log    *slog.Logger

	// authErrLogMu / lastAuthErrLog rate-limit the warning emitted on
	// 401/403 responses so a misconfigured token doesn't flood the log
	// with one line per PUT attempt for the lifetime of the process.
	authErrLogMu   sync.Mutex
	lastAuthErrLog time.Time
}

// NewClient wires a client to the given webhook URL. token is sent
// as a bearer Authorization header when non-empty. A nil log
// defaults to slog.Default. The internal *http.Client uses
// defaultHTTPTimeout per attempt; install an HTTPDoer via
// SetHTTPClient for tests that want to capture requests.
func NewClient(url, token string, log *slog.Logger) *Client {
	return NewClientWithMethod(url, token, http.MethodPut, log)
}

// NewClientWithMethod is NewClient with an explicit HTTP method. The dedicated
// path PUTs to the sidecar (which re-POSTs to accounts-api), but the pooled
// runtime POSTs per-tenant state straight to accounts-api's
// /v1/internal/turborgs/<id>/state receiver (a POST route), so it needs POST.
func NewClientWithMethod(url, token, method string, log *slog.Logger) *Client {
	if log == nil {
		log = slog.Default()
	}
	if method == "" {
		method = http.MethodPut
	}
	return &Client{
		url:    url,
		method: method,
		token:  token,
		http:   &http.Client{Timeout: defaultHTTPTimeout},
		log:    log,
	}
}

// SetHTTPClient swaps the underlying HTTP doer. Used by tests;
// production callers leave the default *http.Client in place.
func (c *Client) SetHTTPClient(d HTTPDoer) {
	if c == nil {
		return
	}
	c.http = d
}

// URL returns the webhook URL the client posts to. Useful for tests
// and for log-line context.
func (c *Client) URL() string {
	if c == nil {
		return ""
	}
	return c.url
}

// Put marshals body to JSON and pushes it to the configured webhook
// with the configured retry policy. Returns nil on success (2xx) or
// on a non-retryable 4xx (logged and treated as "delivered" so the
// caller doesn't fall into a tight retry loop on a misconfigured
// observer). Returns the last error after retries are exhausted on
// transient failures.
//
// ctx bounds the whole attempt sequence: passing ctx.Done() shortens
// the wait between retries.
func (c *Client) Put(ctx context.Context, body any) error {
	if c == nil || c.url == "" {
		return nil
	}
	buf, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("statepush: marshal: %w", err)
	}

	var lastErr error
	for attempt := 0; attempt <= len(retryBackoffs); attempt++ {
		if attempt > 0 {
			sleep := retryBackoffs[attempt-1]
			t := time.NewTimer(sleep)
			select {
			case <-ctx.Done():
				t.Stop()
				return ctx.Err()
			case <-t.C:
			}
		}
		done, err := c.attempt(ctx, buf)
		if done {
			return err
		}
		lastErr = err
	}
	c.log.Warn("state webhook PUT dropped after retries",
		"url", c.url,
		"err", lastErr,
	)
	return lastErr
}

// attempt fires a single PUT. Returns (done=true, nil) on a
// successful 2xx, (done=true, err) on a non-retryable response,
// (done=false, err) on a transient error worth retrying.
func (c *Client) attempt(ctx context.Context, body []byte) (bool, error) {
	reqCtx, cancel := context.WithTimeout(ctx, defaultHTTPTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(reqCtx, c.method, c.url, bytes.NewReader(body))
	if err != nil {
		// Bad URL or context-level failure — not retryable.
		return true, fmt.Errorf("statepush: build request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.token != "" {
		req.Header.Set("Authorization", "Bearer "+c.token)
	}
	resp, err := c.http.Do(req)
	if err != nil {
		// Treat transport failures (including request-context timeout)
		// as transient — the next retry might succeed.
		if errors.Is(err, context.Canceled) {
			return true, err
		}
		return false, fmt.Errorf("statepush: transport: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode >= 200 && resp.StatusCode < 300 {
		return true, nil
	}
	if resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusForbidden {
		// Misconfigured token or stale URL — retrying won't help.
		// Rate-limit the log to once per minute so a chronically
		// misconfigured deployment doesn't drown its log stream.
		c.logAuthError(resp.StatusCode)
		return true, fmt.Errorf("statepush: auth rejected with status %d", resp.StatusCode)
	}
	if resp.StatusCode >= 400 && resp.StatusCode < 500 {
		// Other 4xx — payload was malformed or endpoint refused.
		// Log and treat as delivered so we don't tight-loop.
		c.log.Warn("state webhook PUT non-retryable response",
			"url", c.url,
			"status", resp.StatusCode,
		)
		return true, fmt.Errorf("statepush: rejected with status %d", resp.StatusCode)
	}
	// 5xx — retry.
	return false, fmt.Errorf("statepush: server error %d", resp.StatusCode)
}

func (c *Client) logAuthError(status int) {
	c.authErrLogMu.Lock()
	defer c.authErrLogMu.Unlock()
	now := time.Now()
	if now.Sub(c.lastAuthErrLog) < time.Minute {
		return
	}
	c.lastAuthErrLog = now
	c.log.Warn("state webhook auth rejected",
		"url", c.url,
		"status", status,
	)
}
