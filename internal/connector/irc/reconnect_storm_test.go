package irc

import (
	"io"
	"log/slog"
	"testing"
	"time"
)

func TestReconnectStormTripsOverThreshold(t *testing.T) {
	s := newReconnectStorm(10*time.Minute, 3, 15*time.Minute)
	base := time.Now()
	// Up to the threshold (3) within the window: no storm yet.
	if s.record(base) || s.record(base.Add(time.Second)) || s.record(base.Add(2*time.Second)) {
		t.Fatal("must not trip at or below the threshold")
	}
	// The one that pushes the window count past the threshold trips it.
	if !s.record(base.Add(3 * time.Second)) {
		t.Fatal("a reconnect that pushes the count over the threshold must trip the storm")
	}
}

func TestReconnectStormPrunesOldAttempts(t *testing.T) {
	s := newReconnectStorm(5*time.Minute, 2, time.Hour)
	base := time.Now()
	s.record(base)
	s.record(base.Add(time.Minute))
	// Well past the window: the two old attempts age out, so a lone reconnect
	// is not a storm — the breaker only fires on a sustained *rate*.
	if s.record(base.Add(6 * time.Minute)) {
		t.Fatal("attempts older than the window must prune, not accumulate into a storm")
	}
}

func TestReconnectStormDisabled(t *testing.T) {
	s := newReconnectStorm(time.Minute, 0, time.Hour) // maxAttempts <= 0 disables
	now := time.Now()
	for i := 0; i < 50; i++ {
		if s.record(now.Add(time.Duration(i) * time.Millisecond)) {
			t.Fatal("a non-positive maxAttempts must disable the breaker")
		}
	}
}

func TestReconnectStormNextDelay(t *testing.T) {
	s := newReconnectStorm(10*time.Minute, 2, 15*time.Minute)
	now := time.Now()
	backoff := 5 * time.Second
	log := slog.New(slog.NewTextHandler(io.Discard, nil))

	if d := s.nextDelay(now, backoff, log); d != backoff {
		t.Fatalf("under threshold: want backoff %v, got %v", backoff, d)
	}
	if d := s.nextDelay(now.Add(time.Second), backoff, log); d != backoff {
		t.Fatalf("under threshold: want backoff %v, got %v", backoff, d)
	}
	// Over threshold (> 2) → the long cooldown, regardless of the backoff.
	if d := s.nextDelay(now.Add(2*time.Second), backoff, log); d != 15*time.Minute {
		t.Fatalf("over threshold: want cooldown 15m, got %v", d)
	}
	// nil logger on the storm path must not panic.
	if d := s.nextDelay(now.Add(3*time.Second), backoff, nil); d != 15*time.Minute {
		t.Fatalf("over threshold (nil log): want cooldown 15m, got %v", d)
	}
}
