package irc

import (
	"sync"
	"time"
)

// pingLedger tracks outstanding client-initiated PING tokens awaiting
// matching PONGs. All methods are safe for concurrent use.
//
// The ledger is per-session — runSession creates a fresh one each
// iteration so a reconnect starts with a clean slate.
type pingLedger struct {
	mu      sync.Mutex
	pending map[string]time.Time
}

func newPingLedger() *pingLedger {
	return &pingLedger{pending: map[string]time.Time{}}
}

// Add records a sent PING token with the wall-clock send time. If the
// token is already pending the timestamp is overwritten — should never
// happen in practice (tokens come from a monotonic counter) but the
// behavior is defined for safety.
func (l *pingLedger) Add(token string, sentAt time.Time) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.pending[token] = sentAt
}

// Ack removes the token if present. Returns true when the ack matched
// an outstanding PING (i.e. we initiated this one) and false otherwise.
// Server-initiated PINGs prompt a PONG from us, but those PONGs travel
// out — never appear in our inbound stream — so we'll never see them
// here. An unknown token is therefore an unmatched server echo or an
// already-acked duplicate; either way, harmless.
func (l *pingLedger) Ack(token string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.pending[token]; !ok {
		return false
	}
	delete(l.pending, token)
	return true
}

// Oldest returns the send time of the oldest outstanding PING and
// whether the ledger has anything outstanding. The pong-watchdog uses
// this to compare against `now - PongTimeout`.
func (l *pingLedger) Oldest() (time.Time, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.pending) == 0 {
		return time.Time{}, false
	}
	var oldest time.Time
	first := true
	for _, t := range l.pending {
		if first || t.Before(oldest) {
			oldest = t
			first = false
		}
	}
	return oldest, true
}

// Len is the number of outstanding PINGs. Exposed for tests; no
// production caller needs it.
func (l *pingLedger) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.pending)
}
