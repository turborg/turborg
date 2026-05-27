package server

import (
	"context"
	"log/slog"
	"runtime"
	"time"
)

// watchdogLevel decides the log level for a resource sample: Warn once heap
// crosses the configured threshold (>0), else Info. Split out so the decision
// is unit-tested without driving a real heap to the limit.
func watchdogLevel(heapAlloc, heapWarnBytes uint64) slog.Level {
	if heapWarnBytes > 0 && heapAlloc >= heapWarnBytes {
		return slog.LevelWarn
	}
	return slog.LevelInfo
}

// RunWatchdog periodically samples pool-wide resource usage — heap, goroutine
// count, attached tenants — and logs it, escalating to Warn when heap crosses
// heapWarnBytes. This is observability + early warning; the hard memory defense
// is GOMEMLIMIT (GC backpressure) and per-tenant quarantine. A non-positive
// interval disables it. Blocks until ctx is cancelled.
func (s *Server) RunWatchdog(ctx context.Context, interval time.Duration, heapWarnBytes uint64) {
	if interval <= 0 {
		return
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.sampleResources(heapWarnBytes)
		}
	}
}

// sampleResources reads one resource snapshot and logs it at the
// threshold-appropriate level.
func (s *Server) sampleResources(heapWarnBytes uint64) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	s.log.Log(context.Background(), watchdogLevel(m.HeapAlloc, heapWarnBytes), "pool watchdog",
		"heap_bytes", m.HeapAlloc,
		"goroutines", runtime.NumGoroutine(),
		"tenants", s.Count(),
		"heap_warn_bytes", heapWarnBytes,
	)
}
