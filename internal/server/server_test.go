package server

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

// goleak guards the core M1 promise: attach/detach and shutdown drain every
// tenant goroutine — no leaks.
func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func spec(id string, connectors ...string) TenantSpec {
	cs := make([]ConnectorSpec, 0, len(connectors))
	for _, c := range connectors {
		cs = append(cs, ConnectorSpec{Type: c, Config: map[string]any{}, Secrets: map[string]any{}})
	}
	return TenantSpec{TurborgID: id, RuntimeMode: "pooled", Connectors: cs}
}

func TestServerBootsInitialTenants(t *testing.T) {
	src := &StaticSource{Tenants: []TenantSpec{spec("a", "irc"), spec("b")}}
	srv := New(src, testLogger())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()

	require.Eventually(t, func() bool { return srv.Count() == 2 }, time.Second, 5*time.Millisecond)
	require.True(t, srv.Has("a"))
	require.True(t, srv.Has("b"))
	require.Equal(t, []string{"a", "b"}, srv.TenantIDs())

	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
	require.Equal(t, 0, srv.Count(), "shutdown must drain every tenant")
}

func TestServerAttachAndDetachViaEvents(t *testing.T) {
	events := make(chan TenantEvent)
	src := &StaticSource{Events: events}
	srv := New(src, testLogger())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()

	events <- TenantEvent{Kind: TenantUpserted, Spec: spec("x", "irc")}
	require.Eventually(t, func() bool { return srv.Has("x") }, time.Second, 5*time.Millisecond)

	events <- TenantEvent{Kind: TenantRemoved, TurborgID: "x"}
	require.Eventually(t, func() bool { return !srv.Has("x") }, time.Second, 5*time.Millisecond)

	cancel()
	<-done
}

func TestServerUpsertUpdatesInPlace(t *testing.T) {
	events := make(chan TenantEvent)
	src := &StaticSource{Tenants: []TenantSpec{spec("x", "irc")}, Events: events}
	srv := New(src, testLogger())

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()

	require.Eventually(t, func() bool { return srv.Has("x") }, time.Second, 5*time.Millisecond)

	// Re-upserting the same id must update in place, not add a tenant.
	events <- TenantEvent{Kind: TenantUpserted, Spec: spec("x", "irc", "discord")}
	require.Never(t, func() bool { return srv.Count() != 1 }, 100*time.Millisecond, 10*time.Millisecond)

	cancel()
	<-done
}

func TestServerIgnoresEmptyTurborgID(t *testing.T) {
	src := &StaticSource{Tenants: []TenantSpec{spec(""), spec("ok")}}
	srv := New(src, testLogger())

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()

	require.Eventually(t, func() bool { return srv.Has("ok") }, time.Second, 5*time.Millisecond)
	require.Equal(t, 1, srv.Count(), "empty-id spec must be ignored")

	cancel()
	<-done
}

func TestFileSourceInitial(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "tenants.json")
	require.NoError(t, os.WriteFile(path, []byte(`{"tenants":[
		{"turborg_id":"f1","runtime_mode":"pooled","connectors":[{"type":"irc","config":{},"secrets":{}}]},
		{"turborg_id":"f2","runtime_mode":"pooled","connectors":[]}
	]}`), 0o600))

	specs, err := (&FileSource{Path: path}).Initial(context.Background())
	require.NoError(t, err)
	require.Len(t, specs, 2)
	require.Equal(t, "f1", specs[0].TurborgID)
	require.Equal(t, "irc", specs[0].Connectors[0].Type)
}

func TestFileSourceMissingFile(t *testing.T) {
	_, err := (&FileSource{Path: filepath.Join(t.TempDir(), "nope.json")}).Initial(context.Background())
	require.Error(t, err)
}
