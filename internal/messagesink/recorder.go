package messagesink

import (
	"math/rand"
	"sync"
	"time"

	"github.com/oklog/ulid/v2"
)

// Recorder adapts the messagesink.Sink to the narrow shape the
// gateway expects. The gateway hands off (channel, nick, text, ts)
// quadruples; the recorder mints a ULID (Crockford-base32, sortable)
// and formats the timestamp to ISO 8601 microsecond before pushing
// into the batched HTTP sink.
//
// ULID is preferred over UUIDv4 here because the dedupe key on the
// receiving side is (msg_id, ts) — a sortable id keeps the unique-
// index hot pages local to the partition the row lives in,
// avoiding random insert hot-spots.
type Recorder struct {
	sink *Sink

	entropyMu sync.Mutex
	entropy   *rand.Rand
}

// NewRecorder wraps a non-nil Sink. Returns nil when sink is nil so
// the caller can plug the result straight into an interface field
// without producing a typed-nil interface value that misfires the
// gateway's `if recorder != nil` guard.
func NewRecorder(sink *Sink) *Recorder {
	if sink == nil {
		return nil
	}
	return &Recorder{
		sink: sink,
		// math/rand for ULID entropy is fine — ULID uniqueness is a
		// (timestamp, 80-bit random) shape; collision needs both to
		// match within the same millisecond, and the receiving side's
		// (msg_id, ts) unique key catches the vanishing chance anyway.
		entropy: rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

// Submit mints a new message id and pushes the entry into the sink's
// outbound batch. Nil receiver is a no-op so the gateway can call
// unconditionally.
func (r *Recorder) Submit(channel, nick, text string, ts time.Time) {
	if r == nil || r.sink == nil {
		return
	}
	r.entropyMu.Lock()
	id := ulid.MustNew(ulid.Timestamp(ts), r.entropy).String()
	r.entropyMu.Unlock()

	r.sink.Submit(Entry{
		MsgID:   id,
		Channel: channel,
		Nick:    nick,
		Text:    text,
		// ISO 8601 with microsecond precision so MariaDB's DATETIME(6)
		// receives a round-trippable value. The receiving Carbon::parse
		// + the model's $dateFormat = 'Y-m-d H:i:s.u' line up cleanly.
		Ts:   ts.UTC().Format("2006-01-02T15:04:05.000000Z"),
		Kind: "message",
	})
}
