package irc_test

import (
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/connector/irc"
)

// TestBackoffScheduleSequenceWithoutJitter walks the default cadence end
// to end and confirms it caps at the final value.
func TestBackoffScheduleSequenceWithoutJitter(t *testing.T) {
	t.Parallel()
	base := []time.Duration{1 * time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 60 * time.Second}
	b := irc.NewBackoffScheduleWith(base, 0, rand.New(rand.NewSource(1)))

	want := []time.Duration{
		1 * time.Second,
		2 * time.Second,
		4 * time.Second,
		8 * time.Second,
		60 * time.Second,
		60 * time.Second, // stays at the cap
		60 * time.Second,
	}
	for i, w := range want {
		assert.Equal(t, w, b.Next(), "Next call %d", i+1)
	}
}

// TestBackoffScheduleJitterStaysWithinBand confirms that with jitter=0.2
// every returned value falls in [0.8x, 1.2x] of the base — the contract
// the supervisor relies on to keep the schedule bounded.
func TestBackoffScheduleJitterStaysWithinBand(t *testing.T) {
	t.Parallel()
	base := []time.Duration{1 * time.Second}
	const jitter = 0.2
	const lower = float64(time.Second) * (1 - jitter)
	const upper = float64(time.Second) * (1 + jitter)

	b := irc.NewBackoffScheduleWith(base, jitter, rand.New(rand.NewSource(42)))
	for i := 0; i < 200; i++ {
		d := float64(b.Next())
		assert.GreaterOrEqual(t, d, lower, "iteration %d below band", i)
		assert.LessOrEqual(t, d, upper, "iteration %d above band", i)
	}
}

// TestBackoffScheduleResetRewinds confirms Reset returns the schedule
// to its head — invoked by the supervisor after a successful registration
// so the next outage starts at the short end again.
func TestBackoffScheduleResetRewinds(t *testing.T) {
	t.Parallel()
	base := []time.Duration{1 * time.Second, 2 * time.Second, 4 * time.Second}
	b := irc.NewBackoffScheduleWith(base, 0, rand.New(rand.NewSource(1)))
	require.Equal(t, 1*time.Second, b.Next())
	require.Equal(t, 2*time.Second, b.Next())
	b.Reset()
	assert.Equal(t, 1*time.Second, b.Next(), "Reset must rewind to the head")
}

func TestBackoffScheduleDefaultsExposeOneSecondHead(t *testing.T) {
	t.Parallel()
	b := irc.NewBackoffSchedule()
	d := b.Next()
	// With ±20% jitter, the first sleep is in [0.8s, 1.2s].
	assert.GreaterOrEqual(t, d, 800*time.Millisecond)
	assert.LessOrEqual(t, d, 1200*time.Millisecond)
}

// TestBackoffScheduleWithEdgeArgs exercises the defensive coercions in
// NewBackoffScheduleWith — empty base falls back to the default, negative
// jitter coerces to zero, nil rng seeds from time.
func TestBackoffScheduleWithEdgeArgs(t *testing.T) {
	t.Parallel()
	b := irc.NewBackoffScheduleWith(nil, -1, nil)
	// Default base means the first value is ~1s with default 20% jitter
	// — but jitter coerced to 0 means we get exactly 1s.
	assert.Equal(t, time.Second, b.Next())
}

func TestBackoffScheduleConcurrentNextIsSafe(t *testing.T) {
	t.Parallel()
	b := irc.NewBackoffSchedule()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 50; j++ {
				_ = b.Next()
			}
		}()
	}
	wg.Wait()
	// All 400 calls have advanced past the schedule head, so the index
	// must be clamped at the end.
	assert.GreaterOrEqual(t, b.Index(), 1)
}
