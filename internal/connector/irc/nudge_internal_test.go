package irc

import (
	"strings"
	"sync"
	"testing"
	"time"
)

// TestOwnerNudgeResetsAtUTCMidnight injects a fake clock to walk the
// nudge across a UTC day boundary. Lives in the irc package (not
// irc_test) so it can poke the unexported `now` field without exporting
// a test-only setter.
func TestOwnerNudgeResetsAtUTCMidnight(t *testing.T) {
	// Literal valid args, so construction never returns nil — no guard
	// needed (and a nil-check-then-deref trips staticcheck SA5011).
	n := NewOwnerNudge("alice", 2)

	// Frozen clock pointing at 2026-05-15 23:50 UTC.
	clock := time.Date(2026, 5, 15, 23, 50, 0, 0, time.UTC)
	var mu sync.Mutex
	n.now = func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return clock
	}

	var (
		sentMu sync.Mutex
		sent   []string
	)
	send := func(line string) error {
		sentMu.Lock()
		defer sentMu.Unlock()
		sent = append(sent, line)
		return nil
	}

	// Day 1: three Notes with everyN=2 → fires at count=2 only.
	n.Note(send)
	n.Note(send)
	n.Note(send)

	sentMu.Lock()
	if len(sent) != 1 {
		t.Fatalf("day 1: expected 1 nudge, got %d (%v)", len(sent), sent)
	}
	if !strings.Contains(sent[0], "2 messages") {
		t.Fatalf("day 1: nudge should report count=2, got %q", sent[0])
	}
	sentMu.Unlock()

	// Cross UTC midnight.
	mu.Lock()
	clock = time.Date(2026, 5, 16, 0, 30, 0, 0, time.UTC)
	mu.Unlock()

	// Day 2: two Notes — the second must fire because the counter
	// restarts. The cumulative count of 5 (3 + 2) must not delay it.
	n.Note(send)
	n.Note(send)

	sentMu.Lock()
	defer sentMu.Unlock()
	if len(sent) != 2 {
		t.Fatalf("day 2: expected 2 total nudges, got %d (%v)", len(sent), sent)
	}
	if !strings.Contains(sent[1], "2 messages") {
		t.Fatalf("day 2: nudge should report count=2 (day-local), got %q", sent[1])
	}
}
