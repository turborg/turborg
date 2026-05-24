package server

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func specWithChannel(id, channel string) TenantSpec {
	return TenantSpec{
		TurborgID:   id,
		RuntimeMode: "pooled",
		Connectors: []ConnectorSpec{{
			Type:    "irc",
			Config:  map[string]any{"channels": []any{channel}},
			Secrets: map[string]any{},
		}},
	}
}

// TestUpdateRestartsTenantWithNewSpec is the M7 deliverable: changing a
// running tenant's spec restarts its work with the new config, without losing
// the tenant or disturbing the process.
func TestUpdateRestartsTenantWithNewSpec(t *testing.T) {
	var runs atomic.Int32
	seen := make(chan string, 8)

	events := make(chan TenantEvent)
	src := &StaticSource{Tenants: []TenantSpec{specWithChannel("x", "#one")}, Events: events}
	srv := New(src, testLogger())
	srv.workFactory = func(tn *Tenant) func(context.Context) error {
		return func(ctx context.Context) error {
			runs.Add(1)
			tn.mu.Lock()
			ch := fmt.Sprint(tn.spec.Connectors[0].Config["channels"])
			tn.mu.Unlock()
			seen <- ch
			<-ctx.Done()
			return ctx.Err()
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()

	require.Equal(t, "[#one]", waitStr(t, seen), "first run should see the initial channel")

	// Change the channel — the tenant must restart and observe the new spec.
	events <- TenantEvent{Kind: TenantUpserted, Spec: specWithChannel("x", "#two")}
	require.Equal(t, "[#two]", waitStr(t, seen), "restart should see the updated channel")
	require.GreaterOrEqual(t, int(runs.Load()), 2)
	require.Equal(t, 1, srv.Count(), "update must not duplicate the tenant")

	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestIdenticalUpsertDoesNotRestart(t *testing.T) {
	var runs atomic.Int32

	events := make(chan TenantEvent)
	src := &StaticSource{Tenants: []TenantSpec{specWithChannel("x", "#one")}, Events: events}
	srv := New(src, testLogger())
	srv.workFactory = func(_ *Tenant) func(context.Context) error {
		return func(ctx context.Context) error {
			runs.Add(1)
			<-ctx.Done()
			return ctx.Err()
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()

	require.Eventually(t, func() bool { return runs.Load() == 1 }, time.Second, 5*time.Millisecond)

	// Re-upsert the identical spec: must be a no-op (no restart).
	events <- TenantEvent{Kind: TenantUpserted, Spec: specWithChannel("x", "#one")}
	require.Never(t, func() bool { return runs.Load() != 1 }, 150*time.Millisecond, 15*time.Millisecond,
		"identical spec must not restart the tenant")

	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func waitStr(t *testing.T, ch <-chan string) string {
	t.Helper()
	select {
	case s := <-ch:
		return s
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for run observation")
		return ""
	}
}
