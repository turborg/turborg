package irc

import (
	"log/slog"
	"sync"
	"time"
)

const (
	// Reconnect-storm circuit-breaker defaults. A flapping upstream — a session
	// that drops before the 30s stable window, so the backoff never resets —
	// reconnects at the 60s backoff cap, ~1/min, indefinitely. After
	// defaultReconnectStormMax reconnects within the window, force the longer
	// cooldown so one broken tenant can't keep hammering the IRC network and,
	// in the pooled runtime, the shared egress IP (sustained reconnect floods
	// are a classic K-line trigger that would take down every tenant on that
	// IP). At ~1/min a genuine flapper trips in ~6 min; a healthy connector
	// never approaches it.
	defaultReconnectStormWindow   = 10 * time.Minute
	defaultReconnectStormMax      = 6
	defaultReconnectStormCooldown = 15 * time.Minute
)

// reconnectStorm is a sliding-window circuit breaker over the supervisor's
// reconnect loop. It complements the per-attempt backoff (which caps the
// delay) with a hard cooldown once the *rate* of reconnects over a window
// crosses a threshold — turning sustained flapping into long quiet periods.
type reconnectStorm struct {
	window      time.Duration
	maxAttempts int
	cooldown    time.Duration

	mu       sync.Mutex
	attempts []time.Time
}

func newReconnectStorm(window time.Duration, maxAttempts int, cooldown time.Duration) *reconnectStorm {
	return &reconnectStorm{window: window, maxAttempts: maxAttempts, cooldown: cooldown}
}

// record logs a reconnect attempt at now, prunes attempts older than the
// window, and reports whether the count within the window now exceeds the
// threshold. A non-positive maxAttempts disables the breaker.
func (r *reconnectStorm) record(now time.Time) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.maxAttempts <= 0 {
		return false
	}
	cutoff := now.Add(-r.window)
	kept := r.attempts[:0]
	for _, t := range r.attempts {
		if t.After(cutoff) {
			kept = append(kept, t)
		}
	}
	r.attempts = append(kept, now)
	return len(r.attempts) > r.maxAttempts
}

// nextDelay records a reconnect attempt and returns how long to wait before
// it: the normal backoff delay, or the longer cooldown when the reconnect rate
// has crossed the storm threshold. Logs a warning on the storm path.
func (r *reconnectStorm) nextDelay(now time.Time, backoff time.Duration, log *slog.Logger) time.Duration {
	if r.record(now) {
		if log != nil {
			log.Warn("irc reconnect storm; cooling down to protect the upstream + egress IP",
				"window", r.window, "max_attempts", r.maxAttempts, "cooldown", r.cooldown)
		}
		return r.cooldown
	}
	return backoff
}
