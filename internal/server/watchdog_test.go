package server

import (
	"context"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWatchdogLevel(t *testing.T) {
	assert.Equal(t, slog.LevelInfo, watchdogLevel(100, 0), "no threshold → never warn")
	assert.Equal(t, slog.LevelInfo, watchdogLevel(50, 100), "under threshold → info")
	assert.Equal(t, slog.LevelWarn, watchdogLevel(100, 100), "at threshold → warn")
	assert.Equal(t, slog.LevelWarn, watchdogLevel(150, 100), "over threshold → warn")
}

// countingHandler is a minimal slog.Handler that counts "pool watchdog"
// records so the test can confirm the watchdog actually sampled.
type countingHandler struct{ n *atomic.Int64 }

func (h countingHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h countingHandler) Handle(_ context.Context, r slog.Record) error {
	if r.Message == "pool watchdog" {
		h.n.Add(1)
	}
	return nil
}
func (h countingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h countingHandler) WithGroup(string) slog.Handler      { return h }

func TestRunWatchdogSamplesAndStops(t *testing.T) {
	var n atomic.Int64
	s := New(nil, slog.New(countingHandler{n: &n}))

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { s.RunWatchdog(ctx, 10*time.Millisecond, 0); close(done) }()

	require.Eventually(t, func() bool { return n.Load() >= 1 }, 2*time.Second, 5*time.Millisecond,
		"watchdog must emit at least one sample")

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("watchdog did not stop on context cancel")
	}
}

func TestRunWatchdogDisabledWithZeroInterval(t *testing.T) {
	s := New(nil, slog.Default())
	// Returns immediately (no ticker) — a non-positive interval disables it.
	s.RunWatchdog(context.Background(), 0, 0)
}
