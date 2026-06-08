package irc

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestConnectGate_DisabledIsNoOp(t *testing.T) {
	g := newConnectGate(0)
	// A burst of waits on the same key all admit immediately when disabled.
	for i := 0; i < 5; i++ {
		if err := g.Wait(context.Background(), "ip|host"); err != nil {
			t.Fatalf("disabled gate Wait must not block/err, got %v on attempt %d", err, i)
		}
	}
}

func TestConnectGate_BurstThenThrottlesPerKey(t *testing.T) {
	const (
		interval = 40 * time.Millisecond
		burst    = 3
	)
	g := newConnectGateWithBurst(interval, burst)

	// The bucket starts full: the first `burst` connects after an idle period
	// fire immediately.
	start := time.Now()
	for i := 0; i < burst; i++ {
		if err := g.Wait(context.Background(), "ip|host"); err != nil {
			t.Fatalf("burst slot %d should admit immediately, got %v", i, err)
		}
	}
	if elapsed := time.Since(start); elapsed > interval/2 {
		t.Fatalf("burst of %d should be near-instant, took %v", burst, elapsed)
	}

	// The next connect must wait ~one interval for a token to refill.
	waitStart := time.Now()
	if err := g.Wait(context.Background(), "ip|host"); err != nil {
		t.Fatalf("post-burst Wait should admit after the interval, got %v", err)
	}
	if elapsed := time.Since(waitStart); elapsed < interval/2 {
		t.Fatalf("post-burst Wait returned too early (%v) — it should have throttled ~1 interval", elapsed)
	}
}

func TestConnectGate_KeysAreIndependent(t *testing.T) {
	g := newConnectGateWithBurst(time.Hour, 1) // burst 1: second same-key wait blocks
	// Burn key A's only token.
	if err := g.Wait(context.Background(), "ipA|host"); err != nil {
		t.Fatalf("key A first wait must admit, got %v", err)
	}
	// Key B's bucket is independent and still full — admits immediately even
	// though key A is now saturated for an hour.
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := g.Wait(ctx, "ipB|host"); err != nil {
		t.Fatalf("key B must not wait on key A's backlog, got %v", err)
	}
}

func TestConnectGate_WaitHonorsContext(t *testing.T) {
	g := newConnectGateWithBurst(time.Hour, 1) // long enough that only ctx unblocks
	// Burn the only token so the next Wait must block.
	if err := g.Wait(context.Background(), "ip|host"); err != nil {
		t.Fatalf("first wait must admit, got %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	if err := g.Wait(ctx, "ip|host"); err == nil {
		t.Fatal("expected ctx deadline error while the bucket is empty")
	}
	if time.Since(start) > time.Second {
		t.Fatal("Wait did not unblock promptly on ctx cancellation")
	}
}

// TestConnectGate_CancelledWaitsDoNotRunAway is the regression guard for the
// production incident: a herd of connects that all abandon their wait (each
// killed by its dial deadline, then retried) must NOT push the key's schedule
// past wall-clock. With the old strict-interval scheduler every abandoned
// Wait still consumed a slot, so the tail starved forever. With a token
// bucket a cancelled wait returns its token, so after the storm a fresh Wait
// still admits within ~one interval.
func TestConnectGate_CancelledWaitsDoNotRunAway(t *testing.T) {
	const interval = 30 * time.Millisecond
	g := newConnectGateWithBurst(interval, 1)

	// Drain the initial token so subsequent waits must block on the refill.
	if err := g.Wait(context.Background(), "ip|host"); err != nil {
		t.Fatalf("priming wait must admit, got %v", err)
	}

	// Simulate 50 connects that each give up almost immediately (a flapping
	// herd retrying under a short deadline). None should consume a real slot.
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
			defer cancel()
			_ = g.Wait(ctx, "ip|host")
		}()
	}
	wg.Wait()

	// A patient connect must now be admitted within a small multiple of the
	// interval — proving the schedule did not run away. Under the old leak
	// this would have blocked for ~50 intervals.
	ctx, cancel := context.WithTimeout(context.Background(), 4*interval)
	defer cancel()
	start := time.Now()
	if err := g.Wait(ctx, "ip|host"); err != nil {
		t.Fatalf("after a storm of cancelled waits, a patient connect must admit promptly, got %v after %v", err, time.Since(start))
	}
}

func TestConnectGate_EnableHelpers(t *testing.T) {
	// These mutate the process-wide gate; restore it disabled afterwards so
	// other tests (which expect the gate inert) are unaffected.
	t.Cleanup(func() { EnableConnectGate(0) })

	// Interval + burst from env are honored: burst 2 admits two immediately,
	// the third throttles on the 1h interval (so a tight ctx times out).
	t.Setenv("TURBORG_IRC_CONNECT_GATE_INTERVAL", "1h")
	t.Setenv("TURBORG_IRC_CONNECT_GATE_BURST", "2")
	EnableConnectGateFromEnv()
	for i := 0; i < 2; i++ {
		if err := sharedConnectGate.Wait(context.Background(), "k|h"); err != nil {
			t.Fatalf("env burst slot %d should admit, got %v", i, err)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := sharedConnectGate.Wait(ctx, "k|h"); err == nil {
		t.Fatal("third connect past the env burst must throttle on the interval")
	}

	// Invalid interval falls back to the default (which is far shorter than an
	// hour), so after burning the default burst a connect still admits well
	// within a second.
	t.Setenv("TURBORG_IRC_CONNECT_GATE_INTERVAL", "not-a-duration")
	t.Setenv("TURBORG_IRC_CONNECT_GATE_BURST", "1")
	EnableConnectGateFromEnv()
	if err := sharedConnectGate.Wait(context.Background(), "k2|h"); err != nil {
		t.Fatalf("first wait under default interval must admit, got %v", err)
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), DefaultConnectGateInterval+time.Second)
	defer cancel2()
	if err := sharedConnectGate.Wait(ctx2, "k2|h"); err != nil {
		t.Fatalf("invalid env must fall back to the default interval, got %v", err)
	}
}

func TestConnectGate_ConfigureTakesEffect(t *testing.T) {
	g := newConnectGate(0)
	// Disabled: never blocks.
	if err := g.Wait(context.Background(), "ip|host"); err != nil {
		t.Fatalf("disabled gate must admit, got %v", err)
	}
	// Enable with burst 1 and a long interval; the second same-key wait blocks.
	g.configure(time.Hour, 1)
	if err := g.Wait(context.Background(), "ip|host"); err != nil {
		t.Fatalf("first wait after configure must admit, got %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	if err := g.Wait(ctx, "ip|host"); err == nil {
		t.Fatal("after configure(1h,1) the second same-key wait must throttle")
	}
}
