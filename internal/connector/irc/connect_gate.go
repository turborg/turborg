package irc

import (
	"context"
	"os"
	"strconv"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

const (
	// DefaultConnectGateInterval is the production per-(egress-IP, host)
	// sustained connect spacing — the steady-state refill rate of each
	// bucket. ~1 connect / 10s keeps any single egress IP well under IRC
	// networks' connection-flood thresholds.
	DefaultConnectGateInterval = 10 * time.Second
	// DefaultConnectGateBurst is how many connects a single (egress-IP, host)
	// bucket admits back-to-back before throttling to one per interval. A
	// mass reconnect — a pool redeploy bringing N tenants up at once —
	// admits this many immediately per key, then drains at the sustained
	// rate. The burst smooths the redeploy spike without lifting the flood
	// ceiling; raise it (or lower the interval) when many tenants share one
	// egress IP and the network tolerates a faster recovery.
	DefaultConnectGateBurst = 10
	// DefaultReconnectFloor is the production minimum reconnect delay —
	// above a typical server's ghost-nick reap window so a fast reconnect
	// after a drop never collides with our own lingering session (433).
	DefaultReconnectFloor = 20 * time.Second
)

// connectGate bounds the rate of new upstream connections per
// (egress-IP, host) across the whole process, with one token bucket per
// key. The egress IP — shared by every tenant a pooled process round-robins
// across it — is the unit IRC networks rate-limit and K-line, so per-
// connector backoff alone cannot protect it: N tenants each individually
// "behaving" still flood the shared IP. This gate caps the aggregate
// new-connection rate on each egress IP regardless of tenant count or
// reconnect flapping. Distinct egress IPs get distinct buckets and never
// wait on each other, so the gate is per-IP, not per-process: a redeploy of
// a pool fanned across K egress IPs drains K times faster than one crammed
// onto a single IP.
//
// Each key is a leaky token bucket (golang.org/x/time/rate): it starts full
// (burst tokens), so the first burst connects after an idle period fire
// immediately, then refills at one token per interval. Wait honours the
// caller's context so shutdown unblocks promptly.
//
// Crucially the bucket is leak-free under contention. rate.Limiter.Wait
// reserves a token and, if the context is cancelled before the token is
// granted, *returns the token* (cancels the reservation). An earlier hand-
// rolled scheduler instead advanced the per-key schedule on every Wait —
// including the ones a caller abandoned at its dial deadline. Under a herd
// (a pool redeploy, a flapping tenant) those abandoned reservations pushed
// the schedule past wall-clock faster than it drained, so the tail of the
// queue starved forever: connections never admitted, retried, re-reserved,
// and starved harder. The token bucket cannot run away — a cancelled wait
// costs nothing.
//
// interval <= 0 disables the gate (Wait is a no-op) — the default, so unit
// tests that construct connectors directly are unaffected. The production
// main() enables it via EnableConnectGate / EnableConnectGateFromEnv.
type connectGate struct {
	mu       sync.Mutex
	interval time.Duration
	burst    int
	limiters map[string]*rate.Limiter
}

func newConnectGate(interval time.Duration) *connectGate {
	return newConnectGateWithBurst(interval, DefaultConnectGateBurst)
}

func newConnectGateWithBurst(interval time.Duration, burst int) *connectGate {
	if burst < 1 {
		burst = 1
	}
	return &connectGate{
		interval: interval,
		burst:    burst,
		limiters: map[string]*rate.Limiter{},
	}
}

// sharedConnectGate is the process-wide gate every connect routes through.
// Disabled (interval 0) until EnableConnectGate is called by main().
var sharedConnectGate = newConnectGate(0)

// EnableConnectGate sets the process-wide per-(egress-IP, host) connect
// interval at the default burst. Called once at startup by the production
// binaries; left at 0 (disabled) in tests. A non-positive interval disables
// the gate.
func EnableConnectGate(interval time.Duration) {
	sharedConnectGate.configure(interval, DefaultConnectGateBurst)
}

// EnableConnectGateWithBurst sets both the sustained interval and the
// per-key burst. A non-positive interval disables the gate; a burst < 1 is
// clamped to 1.
func EnableConnectGateWithBurst(interval time.Duration, burst int) {
	sharedConnectGate.configure(interval, burst)
}

// EnableConnectGateFromEnv enables the gate from the environment:
// TURBORG_IRC_CONNECT_GATE_INTERVAL (a Go duration, e.g. "10s"; "0" disables)
// and TURBORG_IRC_CONNECT_GATE_BURST (a positive integer). Unset or invalid
// values fall back to the defaults. Called once from each production main().
func EnableConnectGateFromEnv() {
	d := DefaultConnectGateInterval
	if v := os.Getenv("TURBORG_IRC_CONNECT_GATE_INTERVAL"); v != "" {
		if p, err := time.ParseDuration(v); err == nil && p >= 0 {
			d = p
		}
	}
	b := DefaultConnectGateBurst
	if v := os.Getenv("TURBORG_IRC_CONNECT_GATE_BURST"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n >= 1 {
			b = n
		}
	}
	EnableConnectGateWithBurst(d, b)
}

// configure swaps the interval/burst and drops the existing buckets so they
// are recreated lazily under the new rate. Called once at startup; the map
// reset is harmless if any connect is mid-Wait (its reservation lives on its
// own *rate.Limiter, which it already holds a reference to).
func (g *connectGate) configure(interval time.Duration, burst int) {
	if burst < 1 {
		burst = 1
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	g.interval = interval
	g.burst = burst
	g.limiters = map[string]*rate.Limiter{}
}

// limiterFor returns the token bucket for key, creating it on first use, or
// nil when the gate is disabled (interval <= 0).
func (g *connectGate) limiterFor(key string) *rate.Limiter {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.interval <= 0 {
		return nil
	}
	lim := g.limiters[key]
	if lim == nil {
		lim = rate.NewLimiter(rate.Every(g.interval), g.burst)
		g.limiters[key] = lim
	}
	return lim
}

// Wait blocks until the gate admits a new connection for key, or ctx is done
// (returning ctx.Err()). A cancelled wait returns its token, so abandoning
// the wait never advances the key's schedule. A no-op when the gate is
// disabled.
func (g *connectGate) Wait(ctx context.Context, key string) error {
	lim := g.limiterFor(key)
	if lim == nil {
		return nil
	}
	return lim.Wait(ctx)
}
