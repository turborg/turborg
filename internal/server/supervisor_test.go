package server

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// callCounter is a tiny concurrency-safe per-id counter for the panic tests.
type callCounter struct {
	mu sync.Mutex
	m  map[string]int
}

func newCallCounter() *callCounter { return &callCounter{m: map[string]int{}} }

func (c *callCounter) Inc(id string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.m[id]++
	return c.m[id]
}

func (c *callCounter) Get(id string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.m[id]
}

// TestPanicQuarantinesOnlyOffendingTenant is the core M3 promise: a panic in
// one tenant's work loop quarantines ONLY that tenant — the process survives
// (this test completing proves it) and sibling tenants keep running — and the
// quarantined tenant auto-revives after backoff.
func TestPanicQuarantinesOnlyOffendingTenant(t *testing.T) {
	var boomCalls atomic.Int32

	src := &StaticSource{Tenants: []TenantSpec{spec("boom"), spec("ok")}}
	srv := New(src, testLogger())
	srv.quarantineBase = 20 * time.Millisecond
	srv.workFactory = func(tn *Tenant) func(context.Context) error {
		return func(ctx context.Context) error {
			if tn.ID == "boom" && boomCalls.Add(1) == 1 {
				panic("boom: first run only")
			}
			<-ctx.Done()
			return ctx.Err()
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()

	// boom panics, is quarantined, then revived — proven by a second call.
	require.Eventually(t, func() bool { return boomCalls.Load() >= 2 }, 2*time.Second, 5*time.Millisecond,
		"quarantined tenant was not revived")
	require.Eventually(t, func() bool {
		st, ok := srv.Status("boom")
		return ok && st == StatusRunning
	}, 2*time.Second, 5*time.Millisecond, "revived tenant should be running again")

	boom := srv.tenants["boom"]
	require.GreaterOrEqual(t, boom.Failures(), 1, "panic should have been recorded")

	// The sibling must never have been quarantined.
	require.Never(t, func() bool {
		st, _ := srv.Status("ok")
		return st != StatusRunning
	}, 200*time.Millisecond, 20*time.Millisecond, "sibling tenant must be unaffected by another tenant's panic")

	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

// TestManyPanicsIsolated stress-checks isolation: of N tenants, a subset
// panic once each; the rest must stay running and the process must survive.
func TestManyPanicsIsolated(t *testing.T) {
	const total = 20
	panicSet := map[string]bool{"t-1": true, "t-4": true, "t-9": true, "t-14": true, "t-17": true}
	calls := newCallCounter()

	specs := make([]TenantSpec, 0, total)
	for i := 0; i < total; i++ {
		specs = append(specs, spec(fmt.Sprintf("t-%d", i)))
	}

	srv := New(&StaticSource{Tenants: specs}, testLogger())
	srv.quarantineBase = 10 * time.Millisecond
	srv.workFactory = func(tn *Tenant) func(context.Context) error {
		return func(ctx context.Context) error {
			if panicSet[tn.ID] && calls.Inc(tn.ID) == 1 {
				panic("isolated panic")
			}
			<-ctx.Done()
			return ctx.Err()
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()

	// Every panicking tenant revives (second call); the rest never panic.
	require.Eventually(t, func() bool {
		for id := range panicSet {
			if calls.Get(id) < 2 {
				return false
			}
		}
		return true
	}, 3*time.Second, 10*time.Millisecond, "all panicking tenants should revive")

	require.Equal(t, total, srv.Count(), "no tenant should be lost to a panic")

	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}
