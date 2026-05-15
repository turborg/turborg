package irc_test

import (
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/connector/irc"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (f *fakeClock) Now() time.Time {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.now
}

func (f *fakeClock) Advance(d time.Duration) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.now = f.now.Add(d)
}

func newClock() *fakeClock {
	return &fakeClock{now: time.Unix(1_700_000_000, 0)}
}

// -- RateLimiter -------------------------------------------------------------

func TestRateLimiterRejectsBadConstruction(t *testing.T) {
	_, err := irc.NewRateLimiter(0, time.Second, time.Second, nil)
	require.Error(t, err)
	_, err = irc.NewRateLimiter(1, 0, time.Second, nil)
	require.Error(t, err)
	_, err = irc.NewRateLimiter(1, time.Second, 0, nil)
	require.Error(t, err)
}

func TestRateLimiterAcceptsValidConstruction(t *testing.T) {
	r, err := irc.NewRateLimiter(3, 10*time.Second, time.Minute, nil)
	require.NoError(t, err)
	require.NotNil(t, r)
	assert.False(t, r.IsLocked("ip"))
}

func TestRateLimiterTriggersLockoutAtThreshold(t *testing.T) {
	c := newClock()
	r, _ := irc.NewRateLimiter(3, 10*time.Second, 30*time.Second, c.Now)

	for i := 0; i < 2; i++ {
		res := r.RecordFailure("1.2.3.4")
		assert.False(t, res.Locked, "below threshold attempt %d should not lock", i+1)
	}
	res := r.RecordFailure("1.2.3.4")
	assert.True(t, res.Locked, "threshold attempt must lock")
	assert.Equal(t, 3, res.Count)
	assert.True(t, r.IsLocked("1.2.3.4"))
}

func TestRateLimiterLockoutExpires(t *testing.T) {
	c := newClock()
	r, _ := irc.NewRateLimiter(2, 5*time.Second, 10*time.Second, c.Now)

	r.RecordFailure("ip")
	r.RecordFailure("ip")
	require.True(t, r.IsLocked("ip"))

	c.Advance(11 * time.Second)
	assert.False(t, r.IsLocked("ip"))
	assert.Equal(t, time.Duration(0), r.TimeUntilUnlock("ip"))
}

func TestRateLimiterFailuresOutsideWindowDrop(t *testing.T) {
	c := newClock()
	r, _ := irc.NewRateLimiter(3, 5*time.Second, 30*time.Second, c.Now)

	r.RecordFailure("ip")
	r.RecordFailure("ip")
	c.Advance(6 * time.Second) // both prior fall outside the window
	res := r.RecordFailure("ip")
	assert.Equal(t, 1, res.Count, "stale entries must be evicted")
	assert.False(t, res.Locked)
}

func TestRateLimiterSuccessClearsState(t *testing.T) {
	c := newClock()
	r, _ := irc.NewRateLimiter(2, 10*time.Second, 10*time.Second, c.Now)
	r.RecordFailure("ip")
	r.RecordSuccess("ip")
	res := r.RecordFailure("ip")
	assert.Equal(t, 1, res.Count)
	assert.False(t, res.Locked)
}

func TestRateLimiterPerIPIsolation(t *testing.T) {
	c := newClock()
	r, _ := irc.NewRateLimiter(2, 10*time.Second, 10*time.Second, c.Now)

	r.RecordFailure("a")
	r.RecordFailure("a")
	assert.True(t, r.IsLocked("a"))
	assert.False(t, r.IsLocked("b"))
}

func TestRateLimiterDefaultClockIsRealTime(t *testing.T) {
	r, _ := irc.NewRateLimiter(2, 50*time.Millisecond, 50*time.Millisecond, nil)
	r.RecordFailure("ip")
	r.RecordFailure("ip")
	require.True(t, r.IsLocked("ip"))
	time.Sleep(60 * time.Millisecond)
	assert.False(t, r.IsLocked("ip"))
}

// -- Throttle ----------------------------------------------------------------

func TestThrottleRejectsBadConstruction(t *testing.T) {
	_, err := irc.NewThrottle(0, time.Second, nil)
	require.Error(t, err)
	_, err = irc.NewThrottle(1, 0, nil)
	require.Error(t, err)
}

func TestThrottleAllowsUnderQuota(t *testing.T) {
	c := newClock()
	tr, _ := irc.NewThrottle(3, 10*time.Second, c.Now)
	for i := 0; i < 3; i++ {
		assert.True(t, tr.Allow("alice"))
	}
	assert.False(t, tr.Allow("alice"), "4th call within window must be denied")
}

func TestThrottleWindowSlides(t *testing.T) {
	c := newClock()
	tr, _ := irc.NewThrottle(2, 5*time.Second, c.Now)

	tr.Allow("alice")
	tr.Allow("alice")
	assert.False(t, tr.Allow("alice"))
	c.Advance(6 * time.Second)
	assert.True(t, tr.Allow("alice"), "events outside window must be evicted")
}

func TestThrottlePerKeyIsolation(t *testing.T) {
	c := newClock()
	tr, _ := irc.NewThrottle(1, 10*time.Second, c.Now)
	assert.True(t, tr.Allow("alice"))
	assert.False(t, tr.Allow("alice"))
	assert.True(t, tr.Allow("bob"))
}

func TestThrottleConcurrent(t *testing.T) {
	tr, _ := irc.NewThrottle(50, time.Second, nil)
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_ = tr.Allow("key")
			}
		}()
	}
	wg.Wait()
}

func TestThrottleAllowWithReasonReturnsRetryAfter(t *testing.T) {
	clock := newClock()
	tr, err := irc.NewThrottle(3, 30*time.Second, clock.Now)
	require.NoError(t, err)

	// First 3 calls fill the bucket.
	for i := 0; i < 3; i++ {
		res := tr.AllowWithReason("#x")
		require.True(t, res.Allow, "call %d should be allowed", i+1)
		assert.Zero(t, res.RetryAfter, "allowed calls return zero RetryAfter")
		clock.Advance(time.Second)
	}

	// 4th call hits the cap. Oldest event is now 3s ago, so retry-after
	// is window (30s) - 3s = 27s.
	res := tr.AllowWithReason("#x")
	assert.False(t, res.Allow)
	assert.Equal(t, 27*time.Second, res.RetryAfter)
}

func TestThrottleAllowWithReasonRespectsPerKeyScope(t *testing.T) {
	// Per-target throttle: hammering one channel must not lock out
	// another. v3 plan: "free user sending 5 to #a + 5 to #b is fine".
	clock := newClock()
	tr, err := irc.NewThrottle(2, 30*time.Second, clock.Now)
	require.NoError(t, err)

	for i := 0; i < 2; i++ {
		assert.True(t, tr.AllowWithReason("#a").Allow)
	}
	assert.False(t, tr.AllowWithReason("#a").Allow, "#a should be at cap")

	// #b is its own bucket — fresh quota.
	assert.True(t, tr.AllowWithReason("#b").Allow)
	assert.True(t, tr.AllowWithReason("#b").Allow)
	assert.False(t, tr.AllowWithReason("#b").Allow, "#b reached its own cap independently")
}

func TestThrottleAllowWithReasonRecoversAfterWindow(t *testing.T) {
	clock := newClock()
	tr, _ := irc.NewThrottle(1, 30*time.Second, clock.Now)

	assert.True(t, tr.AllowWithReason("#x").Allow)
	assert.False(t, tr.AllowWithReason("#x").Allow)

	clock.Advance(31 * time.Second)
	assert.True(t, tr.AllowWithReason("#x").Allow, "bucket should free after window")
}
