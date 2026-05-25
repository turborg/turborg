package server

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/tests/fixtures/fakeirc"
)

func ircTenantSpec(id string, port int, nick, channel string) TenantSpec {
	return TenantSpec{
		TurborgID:   id,
		RuntimeMode: "pooled",
		Connectors: []ConnectorSpec{{
			Type: "irc",
			Config: map[string]any{
				"network":   fmt.Sprintf("127.0.0.1:%d", port),
				"use_tls":   false,
				"nick":      nick,
				"username":  nick,
				"real_name": "pooled tenant",
				"channels":  []any{channel},
			},
			Secrets: map[string]any{},
		}},
	}
}

func lineContaining(sub string) func([]string) bool {
	return func(lines []string) bool {
		for _, l := range lines {
			if strings.Contains(l, sub) {
				return true
			}
		}
		return false
	}
}

// TestTwoTenantsConnectIndependently is the M2 deliverable: two pooled
// tenants in one Server, each pointed at its own upstream, both register and
// JOIN — and neither tenant's traffic leaks onto the other's server.
func TestTwoTenantsConnectIndependently(t *testing.T) {
	fsA := fakeirc.New(t)
	defer fsA.Close()
	fsB := fakeirc.New(t)
	defer fsB.Close()

	src := &StaticSource{Tenants: []TenantSpec{
		ircTenantSpec("tenant-a", fsA.Port(), "nicka", "#aaa"),
		ircTenantSpec("tenant-b", fsB.Port(), "nickb", "#bbb"),
	}}
	srv := New(src, testLogger())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()

	require.True(t, fsA.WaitFor(lineContaining("JOIN #aaa"), 3*time.Second),
		"tenant A did not JOIN its channel; received: %v", fsA.Received())
	require.True(t, fsB.WaitFor(lineContaining("JOIN #bbb"), 3*time.Second),
		"tenant B did not JOIN its channel; received: %v", fsB.Received())

	// No cross-talk: each upstream sees only its own tenant's identity and
	// channel. (A short wait that must NOT become true.)
	require.False(t, fsA.WaitFor(lineContaining("#bbb"), 250*time.Millisecond),
		"tenant B's channel leaked onto tenant A's server: %v", fsA.Received())
	require.False(t, fsA.WaitFor(lineContaining("nickb"), 250*time.Millisecond),
		"tenant B's nick leaked onto tenant A's server: %v", fsA.Received())
	require.False(t, fsB.WaitFor(lineContaining("nicka"), 250*time.Millisecond),
		"tenant A's nick leaked onto tenant B's server: %v", fsB.Received())

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("server did not drain within 3s of cancel")
	}
}
