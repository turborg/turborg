// Package messages defines the durable channel-history seam turborg
// uses for replay (on bouncer / WS attach) and on-demand scrollback
// (CHATHISTORY for IRC clients, the WS `history` op for the bundled
// reference UI and any downstream client).
//
// Two implementations ship in this package:
//
//   - MemoryStore — bounded per-channel ring, the default.
//     History is lost on restart; cap is 200 lines per channel,
//     mirroring the prior in-memory rings the bouncer and gateway each
//     held independently.
//   - HTTPStore — durable mirror backed by an external HTTP service.
//     Submits batch through the existing messagesink.Sink (write side);
//     reads are a synchronous GET. Operators point
//     TURBORG_MESSAGE_STORE_URL at any service that speaks the wire
//     contract documented on HTTPStore.
//
// The Store interface is intentionally narrow — two methods. Adding
// more (TARGETS for CHATHISTORY's channel listing, msgid lookups for
// edit/delete, etc.) is a forward step we'll take once a real consumer
// needs them.
package messages

import (
	"context"
	"sync"
	"time"
)

// PerChannelCap is the default soft cap MemoryStore applies per
// channel. Sized to match the historical 200-msg rings the bouncer and
// gateway each maintained before the store seam landed. Bumping this
// is a deliberate operator choice — bigger rings mean bigger
// per-connector memory footprint on busy networks.
const PerChannelCap = 200

// Message is the in-process representation of a channel message —
// what the store accepts on Submit and yields from Recent. Mirrors the
// shape of messagesink.Entry on the wire, modulo time being a proper
// time.Time so downstream code (decorateReplayLine, the WS frame
// builder) doesn't reparse strings.
//
// ID is optional. MemoryStore tolerates an empty ID; HTTPStore
// requires the receiving service to mint one if missing (matches the
// existing recorder behavior).
type Message struct {
	ID      string
	Channel string
	Nick    string
	Text    string
	TS      time.Time
}

// Store is the read+write seam for channel history. Implementations
// must be safe for concurrent use — both Submit and Recent fire from
// multiple goroutines (every inbound IRC line; every WS history op;
// every bouncer attach).
//
// Recent returns messages strictly OLDER than `before`, newest-first,
// up to `limit` entries. Passing the zero time means "no upper bound,
// return the most recent `limit` messages". An empty channel string is
// treated as a no-op (no implementation looks up cross-channel
// history).
type Store interface {
	Submit(ctx context.Context, m Message) error
	Recent(ctx context.Context, channel string, before time.Time, limit int) ([]Message, error)
}

// MemoryStore is the goroutine-safe in-process Store implementation.
// Per-channel rings keep at most cap entries; oldest entries fall off
// when the cap is exceeded. cap == 0 means "use the package default".
type MemoryStore struct {
	cap int

	mu       sync.RWMutex
	channels map[string][]Message
}

// NewMemoryStore returns a fresh, empty MemoryStore. Pass cap = 0 to
// get the package default (PerChannelCap).
func NewMemoryStore(cap int) *MemoryStore {
	if cap <= 0 {
		cap = PerChannelCap
	}
	return &MemoryStore{
		cap:      cap,
		channels: map[string][]Message{},
	}
}

// Submit appends m to the per-channel ring. The ring is bounded; once
// it exceeds cap, the oldest entries are dropped. Context is honored
// only for cancellation parity with HTTPStore — MemoryStore itself
// never blocks long enough to need it.
func (s *MemoryStore) Submit(_ context.Context, m Message) error {
	if m.Channel == "" {
		return nil
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	bucket := append(s.channels[m.Channel], m)
	if len(bucket) > s.cap {
		bucket = bucket[len(bucket)-s.cap:]
	}
	s.channels[m.Channel] = bucket
	return nil
}

// Recent returns up to limit messages in the channel that are strictly
// older than before. before == zero time means "no upper bound, last
// limit messages". Returned slice is newest-first; nil when the
// channel has no history.
func (s *MemoryStore) Recent(_ context.Context, channel string, before time.Time, limit int) ([]Message, error) {
	if channel == "" || limit <= 0 {
		return nil, nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	bucket := s.channels[channel]
	if len(bucket) == 0 {
		return nil, nil
	}
	// Walk newest → oldest, filtering by `before` and accumulating up
	// to `limit`. Returned slice stays newest-first so the WS history
	// frame and the bouncer BATCH wrap the right order without an
	// extra reverse at every call site.
	out := make([]Message, 0, limit)
	for i := len(bucket) - 1; i >= 0; i-- {
		m := bucket[i]
		if !before.IsZero() && !m.TS.Before(before) {
			continue
		}
		out = append(out, m)
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

// Len reports the count of messages currently stored for channel.
// Useful for tests and for operators consuming the /metrics endpoint —
// not part of the Store interface.
func (s *MemoryStore) Len(channel string) int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.channels[channel])
}
