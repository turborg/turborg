package irc

import (
	"context"
	"os"
	"sync"
	"time"
)

const (
	// DefaultConnectGateInterval is the production per-(egress-IP, host)
	// connect spacing. ~1 connect / 10s keeps any single egress IP well
	// under IRC networks' connection-flood thresholds even with many
	// tenants flapping at once.
	DefaultConnectGateInterval = 10 * time.Second
	// DefaultReconnectFloor is the production minimum reconnect delay —
	// above a typical server's ghost-nick reap window so a fast reconnect
	// after a drop never collides with our own lingering session (433).
	DefaultReconnectFloor = 20 * time.Second
)

// connectGate bounds the rate of new upstream connections per
// (sourceIP, host) across the whole process. The egress IP — shared by
// every tenant a pooled process routes through it — is the unit IRC
// networks rate-limit and G-line, so per-connector backoff alone cannot
// protect it: N tenants each individually "behaving" still flood the
// shared IP, which is what escalates a momentary throttle into a
// network-wide ban. This gate serialises connects to a safe aggregate
// rate regardless of tenant count or reconnect flapping.
//
// It is a per-key minimum-interval scheduler: a lone connect after a
// quiet period fires immediately; rapid successive connects to the same
// key queue at one per interval. Acquire honours the caller's context so
// shutdown unblocks promptly.
//
// interval <= 0 disables the gate (Wait is a no-op) — the default, so
// unit tests that construct connectors directly are unaffected. The
// production main() enables it via EnableConnectGate.
type connectGate struct {
	mu       sync.Mutex
	interval time.Duration
	nextFree map[string]time.Time
}

func newConnectGate(interval time.Duration) *connectGate {
	return &connectGate{interval: interval, nextFree: map[string]time.Time{}}
}

// sharedConnectGate is the process-wide gate every Dial routes through.
// Disabled (interval 0) until EnableConnectGate is called by main().
var sharedConnectGate = newConnectGate(0)

// EnableConnectGate sets the process-wide per-(egress-IP, host) connect
// interval. Called once at startup by the production binaries; left at 0
// (disabled) in tests. A non-positive interval disables the gate.
func EnableConnectGate(interval time.Duration) {
	sharedConnectGate.setInterval(interval)
}

// EnableConnectGateFromEnv enables the gate using
// TURBORG_IRC_CONNECT_GATE_INTERVAL (a Go duration, e.g. "10s"), falling
// back to DefaultConnectGateInterval. Set the env to "0" to disable.
// Called once from each production main().
func EnableConnectGateFromEnv() {
	d := DefaultConnectGateInterval
	if v := os.Getenv("TURBORG_IRC_CONNECT_GATE_INTERVAL"); v != "" {
		if p, err := time.ParseDuration(v); err == nil && p >= 0 {
			d = p
		}
	}
	EnableConnectGate(d)
}

func (g *connectGate) setInterval(d time.Duration) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.interval = d
}

// reserve returns how long the caller must wait before connecting under
// key, and advances the key's schedule by one interval. A connect after
// the key has been idle returns 0; bursts queue at one per interval.
func (g *connectGate) reserve(key string, now time.Time) time.Duration {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.interval <= 0 {
		return 0
	}
	nf := g.nextFree[key]
	if nf.Before(now) {
		nf = now
	}
	wait := nf.Sub(now)
	g.nextFree[key] = nf.Add(g.interval)
	return wait
}

// Wait blocks until the gate admits a new connection for key, or ctx is
// done (returning ctx.Err()).
func (g *connectGate) Wait(ctx context.Context, key string) error {
	d := g.reserve(key, time.Now())
	if d <= 0 {
		return nil
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
