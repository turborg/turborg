package messages

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"time"

	"github.com/turborg/turborg/internal/messagesink"
)

// HTTPStore mirrors channel history to an external HTTP service that
// implements the wire contract documented below. Writes are batched
// through the existing messagesink.Sink (already tuned, retried, and
// backpressured); reads are a synchronous GET.
//
// Wire contract operators must implement at the configured URL:
//
//	POST <url>
//	  Authorization: Bearer <token>
//	  Content-Type: application/json
//	  Body: {"messages": [{"msg_id","channel","nick","text","ts","kind"}, ...]}
//	  Status: 2xx on success; non-2xx is logged at debug and entries
//	  are NOT retried (the receiving side is expected to dedupe on
//	  msg_id, so a dropped batch is recoverable from the next batch).
//
//	GET <url>?channel=<ch>&before=<ISO8601>&limit=<int>
//	  Authorization: Bearer <token>
//	  Status: 200 on success.
//	  Body: {"messages": [{"msg_id","channel","nick","text","ts"}, ...],
//	         "has_more": bool}
//	  - `before` empty means "no upper bound, return the most recent N".
//	  - Messages are returned newest-first.
//	  - has_more = true tells the caller to keep paginating.
type HTTPStore struct {
	endpoint string
	token    string
	client   *http.Client
	sink     *messagesink.Sink // owned: written by Submit, closed by Close.
	log      *slog.Logger
}

// NewHTTPStore returns nil when either endpoint or token is empty —
// matches the convention messagesink.New uses, so the caller can plug
// the result straight into a Store-typed field with a single non-nil
// check.
//
// sink is owned by the returned HTTPStore — Submit pushes through it
// (write half) and the caller passes it in so the same batched +
// retried + backpressured queue services Submit for both this Store
// and the existing gateway.MessageRecorder path during migration.
func NewHTTPStore(endpoint, token string, sink *messagesink.Sink, log *slog.Logger) *HTTPStore {
	if endpoint == "" || token == "" {
		return nil
	}
	if log == nil {
		log = slog.Default()
	}
	return &HTTPStore{
		endpoint: endpoint,
		token:    token,
		client:   &http.Client{Timeout: readTimeout},
		sink:     sink,
		log:      log,
	}
}

// readTimeout caps how long a Recent call can block. The gateway's
// `history` op + bouncer's CHATHISTORY both run on hot paths (a WS
// frame handler / an IRC client read loop) — letting either stall on a
// slow backend would visibly freeze the user's UI.
const readTimeout = 5 * time.Second

// Submit defers to the underlying sink. The HTTPStore intentionally
// owns no per-Submit logic — message ID minting + ISO 8601 timestamp
// formatting + batched POST are all messagesink concerns we don't
// duplicate here.
func (s *HTTPStore) Submit(_ context.Context, m Message) error {
	if s == nil || s.sink == nil {
		return nil
	}
	// Hand the entry off through messagesink's batched Submit. The
	// receiving service is expected to mint msg_id when the field is
	// empty (matching the legacy messagesink.Recorder contract); a
	// caller that has its own id can pass it through Message.ID.
	s.sink.Submit(messagesink.Entry{
		MsgID:   m.ID,
		Channel: m.Channel,
		Nick:    m.Nick,
		Text:    m.Text,
		Ts:      m.TS.UTC().Format("2006-01-02T15:04:05.000000Z"),
		Kind:    "message",
	})
	return nil
}

// Recent queries the configured endpoint for up to limit messages
// strictly older than before. Returns an empty slice (not an error) on
// network failure — callers treat history as best-effort, and an
// in-flight reconnect shouldn't trip a hard failure on an attach-time
// replay.
func (s *HTTPStore) Recent(ctx context.Context, channel string, before time.Time, limit int) ([]Message, error) {
	if s == nil || channel == "" || limit <= 0 {
		return nil, nil
	}
	ctx, cancel := context.WithTimeout(ctx, readTimeout)
	defer cancel()

	q := url.Values{}
	q.Set("channel", channel)
	q.Set("limit", strconv.Itoa(limit))
	if !before.IsZero() {
		q.Set("before", before.UTC().Format("2006-01-02T15:04:05.000000Z"))
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.endpoint+"?"+q.Encode(), nil)
	if err != nil {
		return nil, fmt.Errorf("messages: build history request: %w", err)
	}
	req.Header.Set("Authorization", "Bearer "+s.token)

	resp, err := s.client.Do(req)
	if err != nil {
		// Network/timeout failures are downgraded to debug + empty —
		// the replay path stays best-effort. A flaky read endpoint
		// must not cascade into a failed bouncer attach.
		s.log.Debug("messages: history fetch", "err", err, "channel", channel)
		return nil, nil
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		s.log.Debug("messages: history non-200",
			"status", resp.StatusCode,
			"channel", channel,
		)
		return nil, nil
	}

	var body struct {
		Messages []struct {
			MsgID   string `json:"msg_id"`
			Channel string `json:"channel"`
			Nick    string `json:"nick"`
			Text    string `json:"text"`
			Ts      string `json:"ts"`
		} `json:"messages"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		s.log.Debug("messages: history decode", "err", err)
		return nil, nil
	}

	out := make([]Message, 0, len(body.Messages))
	for _, e := range body.Messages {
		ts, err := time.Parse("2006-01-02T15:04:05.000000Z", e.Ts)
		if err != nil {
			// Fall back to RFC3339 / RFC3339Nano so a receiver that
			// emits a slightly different ISO precision still flows
			// through. Skip the entry on hard parse failure rather
			// than poison the whole reply with an error.
			if ts, err = time.Parse(time.RFC3339Nano, e.Ts); err != nil {
				continue
			}
		}
		out = append(out, Message{
			ID:      e.MsgID,
			Channel: e.Channel,
			Nick:    e.Nick,
			Text:    e.Text,
			TS:      ts,
		})
	}
	return out, nil
}
