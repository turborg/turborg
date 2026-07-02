package skill

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/url"
	"time"
)

// HTTPStore is a durable Store backed by an external HTTP service. It gives
// skills and flows state that survives a process restart — quiz scores, a
// seen-database, counters, per-user history — by reading and writing each value
// over HTTP instead of holding it in memory. The per-key flood counter (Incr)
// stays in-process: its windows are short (seconds), it fires on the hot
// message path, and it never needs to outlive the process, so persisting it
// would add a round-trip for no benefit. Incr therefore delegates to an
// embedded MemoryStore.
//
// Every network operation degrades gracefully: a failed Get reports "not
// found", a failed Set or Delete is dropped with a debug log. A state-service
// outage never propagates an error into message handling — a skill that can't
// reach its backend behaves as if the key were simply absent.
//
// Namespacing mirrors the MemoryStore: the "skill" argument is the namespace
// (the skill or flow name) and "key" is the value key. They map onto the path
// {base}/state/{ns}/{key}.
//
// Wire contract the operator must serve at the configured base URL:
//
//	GET    <base>/state/<ns>/<key>
//	  Authorization: Bearer <token>
//	  Status: 200 with body {"value": "..."} when present;
//	          404 when absent or expired.
//
//	PUT    <base>/state/<ns>/<key>
//	  Authorization: Bearer <token>
//	  Content-Type: application/json
//	  Body: {"value": "...", "ttl_seconds": <int, optional>}
//	  Status: 2xx on success. A positive ttl_seconds expires the value
//	          after that many seconds; omitted / zero stores it without expiry.
//
//	DELETE <base>/state/<ns>/<key>
//	  Authorization: Bearer <token>
//	  Status: 2xx on success (also treated as success when the key was absent).
type HTTPStore struct {
	base   string
	token  string
	client *http.Client
	log    *slog.Logger

	// mem serves Incr (in-process sliding-window flood counters). It is never
	// consulted for Get/Set/Delete — those always go over HTTP.
	mem *MemoryStore
}

// httpStoreTimeout caps how long any single state operation may block. Get runs
// on the message-handling path, so a slow backend must not stall a reply; the
// timeout keeps a degraded state service from freezing the bot.
const httpStoreTimeout = 5 * time.Second

// NewHTTPStore returns nil when either base or token is empty — mirroring the
// convention the message HTTPStore uses, so the caller can plug the result into
// a Store-typed field behind a single non-nil check and fall back to a
// MemoryStore otherwise.
func NewHTTPStore(base, token string, log *slog.Logger) *HTTPStore {
	if base == "" || token == "" {
		return nil
	}
	if log == nil {
		log = slog.Default()
	}
	// Trim a trailing slash so joining "/state/..." never doubles it.
	for len(base) > 0 && base[len(base)-1] == '/' {
		base = base[:len(base)-1]
	}
	return &HTTPStore{
		base:   base,
		token:  token,
		client: &http.Client{Timeout: httpStoreTimeout},
		log:    log.With("component", "skill-store"),
		mem:    NewMemoryStore(),
	}
}

// endpoint builds the per-key URL. Both segments are path-escaped so a
// namespace or key containing a slash or other reserved byte maps to a single,
// unambiguous path segment on the wire.
func (s *HTTPStore) endpoint(ns, key string) string {
	return s.base + "/state/" + url.PathEscape(ns) + "/" + url.PathEscape(key)
}

// Get fetches the value for (skill,key). A missing key (404), any non-200
// status, or a network/decoding failure all report "not found" so callers treat
// a state outage the same as an absent key.
func (s *HTTPStore) Get(skill, key string) (string, bool) {
	ctx, cancel := context.WithTimeout(context.Background(), httpStoreTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.endpoint(skill, key), nil)
	if err != nil {
		s.log.Debug("skill state: build get request", "err", err, "ns", skill)
		return "", false
	}
	req.Header.Set("Authorization", "Bearer "+s.token)

	resp, err := s.client.Do(req)
	if err != nil {
		s.log.Debug("skill state: get", "err", err, "ns", skill)
		return "", false
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return "", false
	}

	var body struct {
		Value string `json:"value"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		s.log.Debug("skill state: decode get", "err", err, "ns", skill)
		return "", false
	}
	return body.Value, true
}

// Set stores val for (skill,key) via PUT. A positive ttl is sent as
// ttl_seconds; a non-positive ttl is omitted (store without expiry). Failures
// are logged and dropped — writes are best-effort so a state outage never
// breaks message handling.
func (s *HTTPStore) Set(skill, key, val string, ttl time.Duration) {
	payload := struct {
		Value      string `json:"value"`
		TTLSeconds int    `json:"ttl_seconds,omitempty"`
	}{Value: val}
	if ttl > 0 {
		payload.TTLSeconds = int(ttl / time.Second)
		// Round sub-second TTLs up to 1s so a small positive TTL doesn't
		// collapse to "no expiry".
		if payload.TTLSeconds == 0 {
			payload.TTLSeconds = 1
		}
	}
	buf, err := json.Marshal(payload)
	if err != nil {
		s.log.Debug("skill state: marshal set", "err", err, "ns", skill)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), httpStoreTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPut, s.endpoint(skill, key), bytes.NewReader(buf))
	if err != nil {
		s.log.Debug("skill state: build set request", "err", err, "ns", skill)
		return
	}
	req.Header.Set("Authorization", "Bearer "+s.token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		s.log.Debug("skill state: set", "err", err, "ns", skill)
		return
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		s.log.Debug("skill state: set non-2xx", "status", resp.StatusCode, "ns", skill)
	}
}

// Delete removes the value for (skill,key) via DELETE. Failures are logged and
// dropped for the same best-effort reason as Set.
func (s *HTTPStore) Delete(skill, key string) {
	ctx, cancel := context.WithTimeout(context.Background(), httpStoreTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodDelete, s.endpoint(skill, key), nil)
	if err != nil {
		s.log.Debug("skill state: build delete request", "err", err, "ns", skill)
		return
	}
	req.Header.Set("Authorization", "Bearer "+s.token)

	resp, err := s.client.Do(req)
	if err != nil {
		s.log.Debug("skill state: delete", "err", err, "ns", skill)
		return
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		s.log.Debug("skill state: delete non-2xx", "status", resp.StatusCode, "ns", skill)
	}
}

// Incr delegates to the embedded in-process counter — flood windows are short
// and per-process by design, so they are never persisted over HTTP.
func (s *HTTPStore) Incr(skill, key string, window time.Duration) (int, error) {
	return s.mem.Incr(skill, key, window)
}
