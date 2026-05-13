package irc

import (
	"errors"
	"sync"
	"time"
)

// FailureResult is the outcome of recording one PASS failure.
type FailureResult struct {
	// Count is the number of failures within the active window after
	// this one was recorded.
	Count int
	// Locked is true if this failure pushed the IP over the threshold
	// and the IP is now in a lockout window.
	Locked bool
}

// Clock abstracts the time source so tests can drive lockouts and window
// rolls deterministically.
type Clock func() time.Time

// RateLimiter is a per-IP sliding-window failure counter with a lockout
// tail. Once an IP exceeds MaxFailures within any rolling Window, it is
// locked out for Lockout. RecordSuccess clears the counter for that IP.
//
// Distinct from Throttle — that one tracks successes per minute; this one
// tracks failures for bouncer PASS auth.
type RateLimiter struct {
	MaxFailures int
	Window      time.Duration
	Lockout     time.Duration
	now         Clock

	mu           sync.Mutex
	failures     map[string][]time.Time
	lockedUntil  map[string]time.Time
}

func NewRateLimiter(maxFailures int, window, lockout time.Duration, clock Clock) (*RateLimiter, error) {
	if maxFailures < 1 {
		return nil, errors.New("ratelimit: MaxFailures must be >= 1")
	}
	if window <= 0 {
		return nil, errors.New("ratelimit: Window must be > 0")
	}
	if lockout <= 0 {
		return nil, errors.New("ratelimit: Lockout must be > 0")
	}
	if clock == nil {
		clock = time.Now
	}
	return &RateLimiter{
		MaxFailures: maxFailures,
		Window:      window,
		Lockout:     lockout,
		now:         clock,
		failures:    map[string][]time.Time{},
		lockedUntil: map[string]time.Time{},
	}, nil
}

// TimeUntilUnlock returns the remaining lockout duration for ip, or 0 if
// it isn't locked. As a side effect, expired entries are cleared.
func (r *RateLimiter) TimeUntilUnlock(ip string) time.Duration {
	r.mu.Lock()
	defer r.mu.Unlock()
	until, ok := r.lockedUntil[ip]
	if !ok {
		return 0
	}
	remaining := until.Sub(r.now())
	if remaining <= 0 {
		delete(r.lockedUntil, ip)
		return 0
	}
	return remaining
}

func (r *RateLimiter) IsLocked(ip string) bool {
	return r.TimeUntilUnlock(ip) > 0
}

// RecordSuccess clears all state for ip.
func (r *RateLimiter) RecordSuccess(ip string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.failures, ip)
	delete(r.lockedUntil, ip)
}

// RecordFailure logs one failed attempt. Lock the IP out if it crosses
// MaxFailures within the active Window.
func (r *RateLimiter) RecordFailure(ip string) FailureResult {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := r.now()
	cutoff := now.Add(-r.Window)
	bucket := r.failures[ip]
	pruned := bucket[:0]
	for _, t := range bucket {
		if !t.Before(cutoff) {
			pruned = append(pruned, t)
		}
	}
	pruned = append(pruned, now)
	r.failures[ip] = pruned

	count := len(pruned)
	if count >= r.MaxFailures {
		r.lockedUntil[ip] = now.Add(r.Lockout)
		delete(r.failures, ip)
		return FailureResult{Count: count, Locked: true}
	}
	return FailureResult{Count: count, Locked: false}
}

// Throttle allows up to MaxPerWindow events per key within a sliding
// Window. Allow returns true and records the event if under quota, or
// false without recording. Used for command + CTCP throttling per sender
// — a chatty user can't flood the bot.
type Throttle struct {
	MaxPerWindow int
	Window       time.Duration
	now          Clock

	mu     sync.Mutex
	events map[string][]time.Time
}

func NewThrottle(maxPerWindow int, window time.Duration, clock Clock) (*Throttle, error) {
	if maxPerWindow < 1 {
		return nil, errors.New("throttle: MaxPerWindow must be >= 1")
	}
	if window <= 0 {
		return nil, errors.New("throttle: Window must be > 0")
	}
	if clock == nil {
		clock = time.Now
	}
	return &Throttle{
		MaxPerWindow: maxPerWindow,
		Window:       window,
		now:          clock,
		events:       map[string][]time.Time{},
	}, nil
}

func (t *Throttle) Allow(key string) bool {
	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.now()
	cutoff := now.Add(-t.Window)
	bucket := t.events[key]
	pruned := bucket[:0]
	for _, ts := range bucket {
		if !ts.Before(cutoff) {
			pruned = append(pruned, ts)
		}
	}
	if len(pruned) >= t.MaxPerWindow {
		t.events[key] = pruned
		return false
	}
	pruned = append(pruned, now)
	t.events[key] = pruned
	return true
}
