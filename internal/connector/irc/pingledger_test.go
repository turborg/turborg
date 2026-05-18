package irc

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestPingLedgerAddAck(t *testing.T) {
	l := newPingLedger()
	now := time.Now()

	l.Add("tb-1", now)
	assert.Equal(t, 1, l.Len())

	assert.True(t, l.Ack("tb-1"), "Ack on a pending token returns true")
	assert.Equal(t, 0, l.Len(), "Ack must remove the token from pending")
}

func TestPingLedgerAckUnknownReturnsFalse(t *testing.T) {
	l := newPingLedger()
	assert.False(t, l.Ack("never-sent"),
		"Ack on a token we never recorded returns false (server echo or duplicate)")
}

func TestPingLedgerAckIsIdempotent(t *testing.T) {
	l := newPingLedger()
	l.Add("tb-1", time.Now())

	assert.True(t, l.Ack("tb-1"))
	assert.False(t, l.Ack("tb-1"),
		"a second Ack for an already-removed token must report no-match")
}

func TestPingLedgerOldestReturnsEarliest(t *testing.T) {
	l := newPingLedger()
	t0 := time.Now()
	t1 := t0.Add(50 * time.Millisecond)
	t2 := t0.Add(100 * time.Millisecond)

	l.Add("late", t2)
	l.Add("first", t0)
	l.Add("middle", t1)

	oldest, ok := l.Oldest()
	assert.True(t, ok)
	assert.Equal(t, t0, oldest, "Oldest must surface the earliest sentAt regardless of insertion order")
}

func TestPingLedgerOldestEmpty(t *testing.T) {
	l := newPingLedger()
	_, ok := l.Oldest()
	assert.False(t, ok, "Oldest on an empty ledger returns ok=false")
}

func TestPingLedgerOldestAdvancesAfterAck(t *testing.T) {
	l := newPingLedger()
	t0 := time.Now()
	t1 := t0.Add(200 * time.Millisecond)

	l.Add("a", t0)
	l.Add("b", t1)

	l.Ack("a")
	oldest, ok := l.Oldest()
	assert.True(t, ok)
	assert.Equal(t, t1, oldest, "after acking the oldest, the next-oldest becomes Oldest()")
}

func TestPingLedgerConcurrentAddAckOldest(t *testing.T) {
	// Drive Add/Ack/Oldest from many goroutines so `go test -race`
	// flags any unguarded map access. The assertion is intentionally
	// weak — we only care that the ledger is internally consistent
	// (no panic, no data race) under contention.
	l := newPingLedger()
	const workers = 16
	const perWorker = 200

	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		go func(w int) {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				token := "w" + strconv.Itoa(w) + "-" + strconv.Itoa(i)
				l.Add(token, time.Now())
				_, _ = l.Oldest()
				l.Ack(token)
			}
		}(w)
	}
	wg.Wait()

	assert.Equal(t, 0, l.Len(),
		"every Add was paired with an Ack — ledger must end empty")
}
