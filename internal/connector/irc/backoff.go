package irc

import (
	"math/rand"
	"sync"
	"time"
)

// defaultBackoffSchedule is the standard reconnect cadence: 1s, 2s, 4s,
// 8s, 16s, 30s, 60s, then 60s flat. Picked to put the bulk of recovery
// attempts in the first minute (most outages are momentary) and cap the
// per-attempt cost at one minute thereafter so server-side rate limits
// stay quiet during long outages.
var defaultBackoffSchedule = []time.Duration{
	1 * time.Second,
	2 * time.Second,
	4 * time.Second,
	8 * time.Second,
	16 * time.Second,
	30 * time.Second,
	60 * time.Second,
}

// defaultBackoffJitter is the symmetric multiplicative jitter applied to
// every returned duration. ±20% prevents many connectors emerging from a
// shared outage from synchronising their retry packets and thundering-
// herding a network's reconnect endpoints.
const defaultBackoffJitter = 0.2

// BackoffSchedule yields the next sleep duration for a reconnect loop.
// Successive Next calls walk through the base schedule, then return the
// final base value indefinitely. Each returned value is jittered.
//
// Reset returns to the head of the schedule and is called by the
// supervisor after a successful registration so the next outage starts
// from the short end again.
//
// Concurrency: Next + Reset are safe for parallel use.
type BackoffSchedule struct {
	base   []time.Duration
	jitter float64

	mu  sync.Mutex
	idx int
	rng *rand.Rand
}

// NewBackoffSchedule returns the schedule with the default cadence
// (1s,2s,4s,8s,16s,30s,60s,60s,…) and ±20% jitter, seeded from the
// current time.
func NewBackoffSchedule() *BackoffSchedule {
	return NewBackoffScheduleWith(defaultBackoffSchedule, defaultBackoffJitter, nil)
}

// NewBackoffScheduleWith lets callers (tests, custom supervisors) supply
// a different base schedule, jitter fraction, or RNG. A nil rng falls
// back to a time-seeded source. A non-nil rng makes the jitter
// deterministic — required by table tests that want to assert exact
// returned durations.
func NewBackoffScheduleWith(base []time.Duration, jitter float64, rng *rand.Rand) *BackoffSchedule {
	if len(base) == 0 {
		base = defaultBackoffSchedule
	}
	if jitter < 0 {
		jitter = 0
	}
	if rng == nil {
		rng = rand.New(rand.NewSource(time.Now().UnixNano())) //nolint:gosec // not cryptographic
	}
	owned := make([]time.Duration, len(base))
	copy(owned, base)
	return &BackoffSchedule{
		base:   owned,
		jitter: jitter,
		rng:    rng,
	}
}

// Next returns the next backoff duration and advances the schedule.
// After exhausting the base schedule, Next keeps returning the final
// base value (still jittered).
func (b *BackoffSchedule) Next() time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	i := b.idx
	if i >= len(b.base) {
		i = len(b.base) - 1
	} else {
		b.idx++
	}
	d := b.base[i]
	if b.jitter > 0 {
		// Multiplier in [1-jitter, 1+jitter]. Symmetric around the base
		// so the expected delay equals the base value.
		multiplier := 1.0 + b.jitter*(2*b.rng.Float64()-1)
		d = time.Duration(float64(d) * multiplier)
	}
	return d
}

// Reset rewinds the schedule to the head. Called by the supervisor after
// a successful registration so the next outage starts at the short end
// again.
func (b *BackoffSchedule) Reset() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.idx = 0
}

// Index returns the schedule's current position (the value Next will use
// on its next call, clamped to len(base)-1). Useful for tests; not part
// of the supervisor's hot path.
func (b *BackoffSchedule) Index() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.idx
}
