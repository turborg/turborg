// Package messagesink holds the HTTP plumbing turborg uses to forward
// recorded channel messages to a configured sink endpoint. In the
// xshellz SaaS deployment that endpoint is the sidecar's per-container
// /messages handler, which in turn batches upstream to accounts-api;
// in self-host the sink is unconfigured (TURBORG_MESSAGE_SINK_URL
// empty) and recording stops at the gateway's in-memory ring.
//
// The contract is documented at the env-var level — operators point
// TURBORG_MESSAGE_SINK_URL at any HTTP receiver that accepts the
// {messages: [...]} batch shape.
package messagesink

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

// Entry is the wire shape we POST to the sink. Mirrors the
// MessageEntry the sidecar handler expects and the
// IngestMessageEntry accounts-api ingests.
type Entry struct {
	MsgID   string         `json:"msg_id"`
	Channel string         `json:"channel"`
	Nick    string         `json:"nick"`
	Text    string         `json:"text"`
	Ts      string         `json:"ts"` // ISO 8601 microsecond precision
	Kind    string         `json:"kind,omitempty"`
	Meta    map[string]any `json:"meta,omitempty"`
}

// flush triggers + batch-size + interval. Tuned for the "1s/10 msgs"
// behaviour described in accounts-api's dev/PLAN-message-store.md.
// At low msg-rate channels the 1s ticker is the rate-limiting flush;
// at high msg-rate channels the size threshold kicks first and keeps
// per-batch latency bounded.
const (
	flushInterval  = 1 * time.Second
	flushBatchSize = 10
	// Backpressure ceiling. Sustained sink-side outages can't run
	// turborg out of memory — we drop oldest entries when full.
	bufferCap = 5000
	// Per-request timeout. Each flush gets its own context; sink
	// outage doesn't pin the gateway's flush goroutine.
	postTimeout = 5 * time.Second
)

// Sink batches entries and POSTs them to the configured endpoint. One
// instance per gateway. Goroutine-safe.
type Sink struct {
	endpoint string
	token    string
	client   *http.Client
	log      *slog.Logger

	mu         sync.Mutex
	buffer     []Entry
	bufferSize int

	flushCh chan struct{}
	doneCh  chan struct{}
	wg      sync.WaitGroup
}

// New returns nil when either input is empty — that's the "no sink
// configured" path the gateway treats as a no-op. Callers don't have
// to nil-check before Submit; Submit handles the nil-sink case.
func New(endpoint, token string, log *slog.Logger) *Sink {
	if endpoint == "" || token == "" {
		return nil
	}
	if log == nil {
		log = slog.Default()
	}
	s := &Sink{
		endpoint: endpoint,
		token:    token,
		client:   &http.Client{Timeout: postTimeout},
		log:      log,
		flushCh:  make(chan struct{}, 1),
		doneCh:   make(chan struct{}),
	}
	s.wg.Add(1)
	go s.run()
	return s
}

// Submit enqueues an entry for batched delivery. Safe to call from
// many goroutines (gateway's onMessageReceived + onMessageSent fan
// in here). Returns immediately; the flush happens on the next tick
// OR when the buffer hits flushBatchSize, whichever fires first.
//
// Nil receiver is a no-op so the gateway can hold a nil *Sink in its
// Options without a guard at the call site — matches the
// "unconfigured = silent" convention statepush uses.
func (s *Sink) Submit(entry Entry) {
	if s == nil {
		return
	}
	s.mu.Lock()
	s.buffer = append(s.buffer, entry)
	s.bufferSize++
	// Backpressure: drop oldest entries when the buffer overflows.
	// Losing the tail of a flood is better than OOMing the agent.
	if s.bufferSize > bufferCap {
		drop := s.bufferSize - bufferCap
		s.buffer = s.buffer[drop:]
		s.bufferSize -= drop
	}
	trigger := s.bufferSize >= flushBatchSize
	s.mu.Unlock()
	if trigger {
		select {
		case s.flushCh <- struct{}{}:
		default:
			// A flush is already pending; the goroutine will see the
			// fresh entries when it next reads the buffer.
		}
	}
}

// Close drains the buffer and stops the background goroutine. Honors
// the caller's context for the final flush — e.g., a SIGTERM handler
// with a short deadline.
func (s *Sink) Close(ctx context.Context) {
	if s == nil {
		return
	}
	close(s.doneCh)
	s.wg.Wait()
	s.flushOnce(ctx)
}

func (s *Sink) run() {
	defer s.wg.Done()
	ticker := time.NewTicker(flushInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.doneCh:
			return
		case <-ticker.C:
			s.flushOnce(context.Background())
		case <-s.flushCh:
			s.flushOnce(context.Background())
		}
	}
}

func (s *Sink) flushOnce(ctx context.Context) {
	s.mu.Lock()
	if len(s.buffer) == 0 {
		s.mu.Unlock()
		return
	}
	pending := s.buffer
	s.buffer = nil
	s.bufferSize = 0
	s.mu.Unlock()

	s.post(ctx, pending)
}

func (s *Sink) post(ctx context.Context, entries []Entry) {
	body, err := json.Marshal(struct {
		Messages []Entry `json:"messages"`
	}{Messages: entries})
	if err != nil {
		s.log.Debug("message sink marshal", "err", err, "count", len(entries))
		return
	}

	ctx, cancel := context.WithTimeout(ctx, postTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.endpoint, bytes.NewReader(body))
	if err != nil {
		s.log.Debug("message sink request build", "err", err)
		return
	}
	req.Header.Set("Authorization", "Bearer "+s.token)
	req.Header.Set("Content-Type", "application/json")

	resp, err := s.client.Do(req)
	if err != nil {
		// Best-effort; the receiving side is idempotent on msg_id so a
		// dropped batch is harmless from a correctness standpoint —
		// only loses-history-on-restart matters, and that's the whole
		// point of the durable sink we just dropped a batch towards.
		s.log.Debug("message sink POST", "err", err, "count", len(entries))
		return
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode >= 400 {
		s.log.Debug("message sink non-2xx",
			"status", resp.StatusCode,
			"count", len(entries),
		)
	}
}
