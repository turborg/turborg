package server

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestTenantStatusString(t *testing.T) {
	require.Equal(t, "running", StatusRunning.String())
	require.Equal(t, "quarantined", StatusQuarantined.String())
	require.Equal(t, "unknown", TenantStatus(99).String())
}

func TestFileSourceWatchClosesOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	ch, err := (&FileSource{Path: "/does/not/matter"}).Watch(ctx)
	require.NoError(t, err)
	cancel()
	select {
	case _, ok := <-ch:
		require.False(t, ok, "channel should close on cancel")
	case <-time.After(time.Second):
		t.Fatal("Watch channel did not close on cancel")
	}
}

// errSource drives Server.Run's error and stream-closed branches.
type errSource struct {
	initialErr error
	watchErr   error
	closed     bool
}

func (e *errSource) Initial(context.Context) ([]TenantSpec, error) { return nil, e.initialErr }

func (e *errSource) Watch(context.Context) (<-chan TenantEvent, error) {
	if e.watchErr != nil {
		return nil, e.watchErr
	}
	ch := make(chan TenantEvent)
	if e.closed {
		close(ch)
	}
	return ch, nil
}

func TestServerRunInitialError(t *testing.T) {
	boom := errors.New("initial boom")
	err := New(&errSource{initialErr: boom}, testLogger()).Run(context.Background())
	require.ErrorIs(t, err, boom)
}

func TestServerRunWatchError(t *testing.T) {
	boom := errors.New("watch boom")
	err := New(&errSource{watchErr: boom}, testLogger()).Run(context.Background())
	require.ErrorIs(t, err, boom)
}

func TestServerRunStreamClosed(t *testing.T) {
	err := New(&errSource{closed: true}, testLogger()).Run(context.Background())
	require.ErrorContains(t, err, "stream closed")
}

func TestWorkErrorIsLoggedNotQuarantined(t *testing.T) {
	src := &StaticSource{Tenants: []TenantSpec{spec("e")}}
	srv := New(src, testLogger())
	srv.workFactory = func(_ *Tenant) func(context.Context) error {
		return func(context.Context) error { return errors.New("work failure, not a panic") }
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- srv.Run(ctx) }()

	require.Eventually(t, func() bool {
		st, ok := srv.Status("e")
		return ok && st == StatusRunning // an error (vs panic) must not quarantine
	}, time.Second, 5*time.Millisecond)

	cancel()
	require.ErrorIs(t, <-done, context.Canceled)
}

func TestHTTPSourceDefaultClientAndLogger(t *testing.T) {
	src, feed := newHTTPSourceTest(t)
	feed.set(spec("a"))
	// Exercise the default-client branch (Client left nil).
	src.Client = nil
	specs, err := src.Initial(context.Background())
	require.NoError(t, err)
	require.Len(t, specs, 1)

	// Exercise logger() default + the poll fetch-error branch: bad URL, no Log.
	bad := &HTTPSource{BaseURL: "http://127.0.0.1:1/v1/internal", HostID: "x", Interval: 10 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	ch, err := bad.Watch(ctx)
	require.NoError(t, err)
	time.Sleep(40 * time.Millisecond) // let at least one failed poll log
	cancel()
	<-ch // drains/closes
}
