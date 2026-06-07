package irc

import (
	"context"
	"testing"
	"time"
)

func TestConnectGate_DisabledIsNoOp(t *testing.T) {
	g := newConnectGate(0)
	now := time.Now()
	// Even a burst of reserves on the same key all return 0 when disabled.
	for i := 0; i < 5; i++ {
		if d := g.reserve("ip|host", now); d != 0 {
			t.Fatalf("disabled gate must return 0, got %v on attempt %d", d, i)
		}
	}
	if err := g.Wait(context.Background(), "ip|host"); err != nil {
		t.Fatalf("disabled gate Wait must not block/err: %v", err)
	}
}

func TestConnectGate_SerializesBurstPerKey(t *testing.T) {
	const interval = 10 * time.Second
	g := newConnectGate(interval)
	now := time.Now()

	// A lone connect after idle fires immediately; the next four queue at
	// one interval apart — bounding the per-key (egress-IP, host) rate.
	for i := 0; i < 5; i++ {
		got := g.reserve("ip|host", now)
		want := time.Duration(i) * interval
		if got != want {
			t.Fatalf("attempt %d: got wait %v, want %v", i, got, want)
		}
	}
}

func TestConnectGate_KeysAreIndependent(t *testing.T) {
	g := newConnectGate(10 * time.Second)
	now := time.Now()
	// First reserve on each distinct key is immediate — one egress IP's
	// backlog never delays another's.
	if d := g.reserve("ipA|host", now); d != 0 {
		t.Fatalf("key A first reserve must be 0, got %v", d)
	}
	if d := g.reserve("ipB|host", now); d != 0 {
		t.Fatalf("key B first reserve must be 0, got %v", d)
	}
}

func TestConnectGate_SetIntervalTakesEffect(t *testing.T) {
	g := newConnectGate(0)
	now := time.Now()
	if d := g.reserve("ip|host", now); d != 0 {
		t.Fatalf("disabled gate must return 0, got %v", d)
	}
	g.setInterval(5 * time.Second)
	// First reserve after enabling is immediate; the second queues.
	_ = g.reserve("ip|host", now)
	if d := g.reserve("ip|host", now); d != 5*time.Second {
		t.Fatalf("after setInterval, burst must queue at the interval, got %v", d)
	}
}

func TestConnectGate_EnableHelpers(t *testing.T) {
	// These mutate the process-wide gate; restore it disabled afterwards so
	// other tests (which expect the gate inert) are unaffected.
	t.Cleanup(func() { EnableConnectGate(0) })

	// Use a fixed `now` so the queued delay is exact (real time advancing
	// between the two reserve calls would shave microseconds off).
	now := time.Now()
	EnableConnectGate(3 * time.Second)
	_ = sharedConnectGate.reserve("k|h", now)
	if d := sharedConnectGate.reserve("k|h", now); d != 3*time.Second {
		t.Fatalf("EnableConnectGate did not set the interval, got %v", d)
	}

	t.Setenv("TURBORG_IRC_CONNECT_GATE_INTERVAL", "7s")
	EnableConnectGateFromEnv()
	_ = sharedConnectGate.reserve("k2|h", now)
	if d := sharedConnectGate.reserve("k2|h", now); d != 7*time.Second {
		t.Fatalf("EnableConnectGateFromEnv did not honor env, got %v", d)
	}

	// Invalid env falls back to the default.
	t.Setenv("TURBORG_IRC_CONNECT_GATE_INTERVAL", "not-a-duration")
	EnableConnectGateFromEnv()
	_ = sharedConnectGate.reserve("k3|h", now)
	if d := sharedConnectGate.reserve("k3|h", now); d != DefaultConnectGateInterval {
		t.Fatalf("invalid env must fall back to default, got %v", d)
	}
}

func TestConnectGate_WaitAdmitsAfterInterval(t *testing.T) {
	g := newConnectGate(30 * time.Millisecond)
	g.reserve("ip|host", time.Now()) // burn the immediate slot
	start := time.Now()
	if err := g.Wait(context.Background(), "ip|host"); err != nil {
		t.Fatalf("Wait should admit after the interval, got %v", err)
	}
	if elapsed := time.Since(start); elapsed < 15*time.Millisecond {
		t.Fatalf("Wait returned too early (%v) — it should have blocked ~1 interval", elapsed)
	}
}

func TestConnectGate_WaitHonorsContext(t *testing.T) {
	g := newConnectGate(time.Hour) // long enough that only ctx unblocks
	// Burn the immediate slot so the next Wait must block.
	g.reserve("ip|host", time.Now())

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := g.Wait(ctx, "ip|host"); err == nil {
		t.Fatal("expected ctx deadline error while gate is blocking")
	}
	if time.Since(start) > time.Second {
		t.Fatal("Wait did not unblock promptly on ctx cancellation")
	}
}
