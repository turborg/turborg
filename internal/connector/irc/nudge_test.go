package irc_test

import (
	"errors"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/connector/irc"
)

// captureSender returns a sender func that records every line passed to
// it. Thread-safe so the test can assert from outside.
func captureSender() (func(string) error, func() []string) {
	var (
		mu   sync.Mutex
		seen []string
	)
	send := func(line string) error {
		mu.Lock()
		defer mu.Unlock()
		seen = append(seen, line)
		return nil
	}
	read := func() []string {
		mu.Lock()
		defer mu.Unlock()
		out := make([]string, len(seen))
		copy(out, seen)
		return out
	}
	return send, read
}

func TestNewOwnerNudgeReturnsNilForIncompleteConfig(t *testing.T) {
	assert.Nil(t, irc.NewOwnerNudge("", 100),
		"empty owner nick must yield nil")
	assert.Nil(t, irc.NewOwnerNudge("alice", 0),
		"zero everyN must yield nil")
	assert.Nil(t, irc.NewOwnerNudge("alice", -1),
		"negative everyN must yield nil")

	n := irc.NewOwnerNudge("alice", 100)
	require.NotNil(t, n, "fully-configured input must return a valid nudge")
}

func TestNoteFiresOnExactMultiple(t *testing.T) {
	n := irc.NewOwnerNudge("alice", 3)
	send, read := captureSender()

	for i := 0; i < 9; i++ {
		n.Note(send)
	}

	sent := read()
	// 9 calls with everyN=3 → DM fires on 3, 6, 9 — three notifications.
	require.Len(t, sent, 3)
	for _, line := range sent {
		assert.True(t, strings.HasPrefix(line, "PRIVMSG alice :"),
			"every nudge must DM the configured owner nick")
	}
	assert.Contains(t, sent[0], "3 messages")
	assert.Contains(t, sent[1], "6 messages")
	assert.Contains(t, sent[2], "9 messages")
}

func TestNoteDoesNotFireBelowThreshold(t *testing.T) {
	n := irc.NewOwnerNudge("alice", 100)
	send, read := captureSender()

	for i := 0; i < 99; i++ {
		n.Note(send)
	}

	assert.Empty(t, read(), "no DM until the count crosses a multiple of everyN")
}

func TestNoteIsSafeOnNilReceiver(t *testing.T) {
	// Operator opted out of nudges — Note on nil pointer must be a no-op,
	// never panic.
	var n *irc.OwnerNudge
	require.NotPanics(t, func() { n.Note(nil) })
}

func TestNoteIgnoresNilSender(t *testing.T) {
	// Even with a configured nudge, a nil sender callback should be a
	// no-op rather than a crash — the call site has already done its
	// counting work for the day.
	n := irc.NewOwnerNudge("alice", 1)
	require.NotPanics(t, func() { n.Note(nil) })
}

func TestNoteSenderErrorIsSilent(t *testing.T) {
	// A nudge DM is best-effort. The sender returning an error must not
	// surface anywhere — Note returns nothing and the next Note still
	// works.
	n := irc.NewOwnerNudge("alice", 1)
	send := func(string) error { return errors.New("link down") }
	require.NotPanics(t, func() { n.Note(send) })
}

func TestNoteConcurrent(t *testing.T) {
	// Race-test: many goroutines incrementing the counter must produce
	// exactly count / everyN nudges and never lose / double-fire.
	n := irc.NewOwnerNudge("alice", 10)
	send, read := captureSender()

	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			n.Note(send)
		}()
	}
	wg.Wait()

	assert.Len(t, read(), 20,
		"200 notes with everyN=10 must produce exactly 20 nudges")
}
