// Command loadgen measures the pooled turborg runtime's footprint under load.
// It boots N synthetic tenants against an in-process fake IRC sink, waits for
// them all to register upstream, then reports RAM + goroutine cost — the
// numbers that drive pool host sizing.
//
//	go run ./cmd/loadgen -tenants 10000 -channels 2 -duration 30s
//
// The 10k numbers must be taken on representative hardware; small runs work
// anywhere and validate the harness.
package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"time"

	"github.com/turborg/turborg/internal/server"
)

func main() {
	tenants := flag.Int("tenants", 100, "number of synthetic tenants")
	channels := flag.Int("channels", 2, "channels per tenant")
	duration := flag.Duration("duration", 10*time.Second, "steady-state sampling duration")
	settle := flag.Duration("settle", 60*time.Second, "max wait for all tenants to register upstream")
	flag.Parse()

	if err := run(*tenants, *channels, *settle, *duration); err != nil {
		fmt.Fprintln(os.Stderr, "loadgen:", err)
		os.Exit(1)
	}
}

// run boots the load, waits for registration, samples, and prints a report.
// Returns an error if not all tenants register within settle.
func run(tenants, channels int, settle, duration time.Duration) error {
	fake, err := startFakeIRC()
	if err != nil {
		return err
	}
	defer fake.close()

	// Error-level logging only: per-tenant attach logs at Info would be
	// N lines of noise and would themselves skew the measurement.
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError}))
	srv := server.New(&server.StaticSource{Tenants: buildSpecs(tenants, channels, fake.addr())}, log)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()

	start := time.Now()
	if !waitFor(func() bool { return fake.registered() >= tenants }, settle) {
		return fmt.Errorf("only %d/%d tenants registered within %s", fake.registered(), tenants, settle)
	}
	attachDur := time.Since(start)

	// Steady-state: hold for duration, tracking peak goroutine count.
	peakGoroutines := runtime.NumGoroutine()
	deadline := time.Now().Add(duration)
	for time.Now().Before(deadline) {
		if g := runtime.NumGoroutine(); g > peakGoroutines {
			peakGoroutines = g
		}
		time.Sleep(200 * time.Millisecond)
	}

	runtime.GC()
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	report(tenants, srv.Count(), attachDur, m, peakGoroutines)

	cancel()
	<-done
	return nil
}

func buildSpecs(n, channels int, addr string) []server.TenantSpec {
	specs := make([]server.TenantSpec, 0, n)
	for i := 0; i < n; i++ {
		chans := make([]any, 0, channels)
		for c := 0; c < channels; c++ {
			chans = append(chans, fmt.Sprintf("#load%d", c))
		}
		specs = append(specs, server.TenantSpec{
			TurborgID:   fmt.Sprintf("load-%d", i),
			RuntimeMode: "pooled",
			Connectors: []server.ConnectorSpec{{
				Type: "irc",
				Config: map[string]any{
					"network":   addr,
					"use_tls":   false,
					"nick":      fmt.Sprintf("ld%d", i),
					"username":  fmt.Sprintf("ld%d", i),
					"real_name": "loadgen",
					"channels":  chans,
				},
				Secrets: map[string]any{},
			}},
		})
	}
	return specs
}

func waitFor(pred func() bool, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if pred() {
			return true
		}
		time.Sleep(25 * time.Millisecond)
	}
	return pred()
}

func report(requested, attached int, attachDur time.Duration, m runtime.MemStats, goroutines int) {
	const mb = 1024 * 1024
	perTenantKB := 0.0
	goroutinesPer := 0.0
	if attached > 0 {
		perTenantKB = float64(m.HeapAlloc) / float64(attached) / 1024
		goroutinesPer = float64(goroutines) / float64(attached)
	}
	fmt.Printf("\n=== turborg-server pooled load report ===\n")
	fmt.Printf("tenants requested   : %d\n", requested)
	fmt.Printf("tenants attached    : %d\n", attached)
	fmt.Printf("time to register all: %s\n", attachDur.Round(time.Millisecond))
	fmt.Printf("heap in use         : %.1f MB\n", float64(m.HeapAlloc)/mb)
	fmt.Printf("sys reserved        : %.1f MB\n", float64(m.Sys)/mb)
	fmt.Printf("per-tenant heap     : %.1f KB\n", perTenantKB)
	fmt.Printf("goroutines          : %d\n", goroutines)
	fmt.Printf("goroutines/tenant   : %.1f\n", goroutinesPer)
	fmt.Printf("=========================================\n")
}
