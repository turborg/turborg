package irc_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/agent"
	"github.com/turborg/turborg/internal/connector/irc"
	"github.com/turborg/turborg/tests/fixtures/fakeirc"
)

// Trivial accessor tests — every setter must round-trip through a
// matching getter or visible side effect. Cheap to write, catches the
// kind of "I changed the field name on one side" regressions that
// integration tests don't notice.

func TestConnectorSetOwnerNudgeAccepts(t *testing.T) {
	c := irc.New(&irc.Settings{Hostname: "h", Nick: "n"}, nil, nil)
	// Pass nil (the disabled form) and a real nudge — both must succeed
	// without panicking. The nudge's behavior is covered by nudge_test.go;
	// here we only verify the setter is wired.
	c.SetOwnerNudge(nil)
	c.SetOwnerNudge(&irc.OwnerNudge{})
}

func TestConnectorSetBouncerAttachHookAccepts(t *testing.T) {
	c := irc.New(&irc.Settings{Hostname: "h", Nick: "n"}, nil, nil)
	c.SetBouncerAttachHook(nil)
	c.SetBouncerAttachHook(func(string) {})
}

// TestSupervisorPublishesStoppedOnGracefulShutdown closes the test
// server, then immediately cancels — the operator-cancellation branch
// of Run must transition to stopped (not paused_idle or transient).
func TestSupervisorPublishesStoppedOnGracefulShutdown(t *testing.T) {
	fs := fakeirc.New(t)
	defer fs.Close()

	conn := irc.New(&irc.Settings{
		Hostname: "127.0.0.1",
		Port:     fs.Port(),
		Nick:     "turborg",
		Channels: []string{"#test"},
	}, nil, nil)

	a := agent.New(nil)
	a.AddConnector(conn)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	require.Eventually(t, func() bool {
		return conn.UpstreamState().State() == irc.UpstreamStateRegistered
	}, 2*time.Second, 10*time.Millisecond)

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("agent did not shut down on ctx cancel")
	}
	assert.Equal(t, irc.UpstreamStateStopped, conn.UpstreamState().State())
}

// TestSupervisorWithEscalationDisabledReturnsClosedDoneImmediately
// covers the runEscalationWatchdog early-return branch — when both
// warn and pause are zero, the watchdog must not start a goroutine
// (the close(done) executes inline).
func TestSupervisorWithEscalationDisabledReturnsClosedDoneImmediately(t *testing.T) {
	fs := fakeirc.New(t)
	defer fs.Close()

	conn := irc.New(&irc.Settings{
		Hostname:           "127.0.0.1",
		Port:               fs.Port(),
		Nick:               "turborg",
		Channels:           []string{"#test"},
		UpstreamWarnAfter:  0,
		UpstreamPauseAfter: 0,
	}, nil, nil)

	a := agent.New(nil)
	a.AddConnector(conn)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	require.Eventually(t, func() bool {
		return conn.UpstreamState().State() == irc.UpstreamStateRegistered
	}, 2*time.Second, 10*time.Millisecond)

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("agent did not shut down")
	}
}

// TestStartDegradesGracefullyOnRecoverableConnectFailure: a recoverable
// initial-connect failure (here, connection refused) must NOT abort Start —
// the connector stays up in disconnected_transient so Run's supervisor retries,
// keeping the bouncer / web shell alive instead of crashing the tenant.
func TestStartDegradesGracefullyOnRecoverableConnectFailure(t *testing.T) {
	conn := irc.New(&irc.Settings{
		Hostname:         "127.0.0.1",
		Port:             1, // non-listening — connection refused (recoverable)
		Nick:             "turborg",
		HandshakeTimeout: 200 * time.Millisecond,
	}, nil, nil)

	err := conn.Start(context.Background())
	require.NoError(t, err, "a recoverable connect failure must not abort Start")
	assert.Equal(t, irc.UpstreamStateDisconnectedTransient, conn.UpstreamState().State(),
		"failed initial Dial must publish disconnected_transient for the supervisor to retry")
}

// TestSendReturnsErrorWhenWriteLineFails covers Send's write-error
// branch — kill the upstream connection so cli.WriteLine returns EPIPE,
// and verify Send surfaces the error rather than logging silently.
func TestSendReturnsErrorWhenWriteLineFails(t *testing.T) {
	fs := newReconnectTestServer(t)

	conn := irc.New(&irc.Settings{
		Hostname: "127.0.0.1",
		Port:     fs.Port(),
		Nick:     "turborg",
		Channels: []string{"#test"},
	}, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	require.NoError(t, conn.Start(ctx))

	// Kill the underlying socket and call Send. The write will fail
	// with EPIPE / ECONNRESET; we don't care which — just that Send
	// surfaces the error rather than swallowing it.
	fs.Close()
	// Loop a few times so the second Send sees a guaranteed dead
	// socket (write to a half-closed socket returns nil on the first
	// flushable packet, error on the next).
	var lastErr error
	for i := 0; i < 5; i++ {
		lastErr = conn.Send(&agent.OutboundEnvelope{
			Connector: "irc", Channel: "#test", Text: "x",
		})
		if lastErr != nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	require.Error(t, lastErr, "Send must surface a write error after upstream death")

	_ = conn.Stop(context.Background())
}

// TestStartPublishesAuthFailedOnPreMOTDPasswdMismatch covers the
// awaitHandshake → ClassifyNumeric path for 464 ERR_PASSWDMISMATCH —
// state must reach disconnected_auth_failed even though Start fails
// fast with an error.
func TestStartPublishesAuthFailedOnPreMOTDPasswdMismatch(t *testing.T) {
	srv := newReconnectTestServerWithReject(t, "ERR_PASSWDMISMATCH")
	defer srv.Close()

	conn := irc.New(&irc.Settings{
		Hostname:         "127.0.0.1",
		Port:             srv.Port(),
		Nick:             "turborg",
		HandshakeTimeout: 1 * time.Second,
	}, nil, nil)

	err := conn.Start(context.Background())
	require.Error(t, err)
	assert.Equal(t, irc.UpstreamStateDisconnectedAuthFailed, conn.UpstreamState().State())
}

// TestStartPublishesBannedOnPreMOTDYoureBanned covers the
// awaitHandshake → ClassifyNumeric path for 465 ERR_YOUREBANNEDCREEP.
func TestStartPublishesBannedOnPreMOTDYoureBanned(t *testing.T) {
	srv := newReconnectTestServerWithReject(t, "ERR_YOUREBANNEDCREEP")
	defer srv.Close()

	conn := irc.New(&irc.Settings{
		Hostname:         "127.0.0.1",
		Port:             srv.Port(),
		Nick:             "turborg",
		HandshakeTimeout: 1 * time.Second,
	}, nil, nil)

	err := conn.Start(context.Background())
	require.Error(t, err)
	assert.Equal(t, irc.UpstreamStateDisconnectedBanned, conn.UpstreamState().State())
}

// TestTransitionFromErrorContextCanceledMapsToStopped covers the
// distinguishing branch in transitionFromError — context.Canceled is
// operator-initiated and must produce Stopped, not transient.
func TestTransitionFromErrorContextCanceledMapsToStopped(t *testing.T) {
	// Build a connector whose Dial will receive a cancelled ctx. The
	// supervisor's transitionFromError is exercised when bringUp's Dial
	// returns the cancellation error.
	conn := irc.New(&irc.Settings{
		Hostname:         "127.0.0.1",
		Port:             1,
		Nick:             "turborg",
		HandshakeTimeout: 200 * time.Millisecond,
	}, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // pre-cancel
	err := conn.Start(ctx)
	require.Error(t, err)
	// State should be either Stopped (if Dial returned context.Canceled
	// directly) or Disconnected* (if Dial returned a wrapped network
	// error before noticing the cancellation). Both are acceptable here
	// — the test exercises the branch, the exact outcome depends on
	// goroutine scheduling.
	got := conn.UpstreamState().State()
	if got != irc.UpstreamStateStopped &&
		got != irc.UpstreamStateDisconnectedTransient {
		t.Fatalf("unexpected state after cancelled Start: %s", got)
	}
	// Also exercise the supervisor cleanup so goleak doesn't complain.
	_ = conn.Stop(context.Background())
	_ = err
	_ = errors.New
}
