package irc_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/agent"
	"github.com/turborg/turborg/internal/connector/irc"
)

// TestSuspendParksWithoutReconnect covers the Disconnect button's core
// contract: Suspend() drops the upstream link (QUIT) and parks the supervisor
// in disconnected_by_user WITHOUT tearing the connector down or entering the
// reconnect loop — a parked connector must never dial. Resume() then brings it
// straight back, proving the bouncer/web gateway survived the whole time.
func TestSuspendParksWithoutReconnect(t *testing.T) {
	fs := newReconnectTestServer(t)

	conn := irc.New(&irc.Settings{
		Hostname: "127.0.0.1",
		Port:     fs.Port(),
		Nick:     "turborg",
		Channels: []string{"#test"},
	}, nil, nil)

	a := agent.New(nil)
	a.AddConnector(conn)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	require.Eventually(t, func() bool {
		return conn.UpstreamState().State() == irc.UpstreamStateRegistered &&
			fs.Sessions() == 1
	}, 2*time.Second, 10*time.Millisecond, "first registration must complete")

	conn.Suspend()

	require.Eventually(t, func() bool {
		return conn.UpstreamState().State() == irc.UpstreamStateDisconnectedByUser
	}, time.Second, 10*time.Millisecond, "Suspend must park in disconnected_by_user")

	require.Eventually(t, func() bool {
		return containsPrefix("QUIT")(fs.Received())
	}, time.Second, 25*time.Millisecond,
		"Suspend must send a QUIT; received: %v", fs.Received())

	// While parked the supervisor must NOT dial — no second session opens even
	// though the backoff floor would otherwise have fired by now.
	require.Never(t, func() bool {
		return fs.Sessions() > 1
	}, 1500*time.Millisecond, 50*time.Millisecond,
		"a parked connector must not reconnect")

	conn.Resume()

	require.Eventually(t, func() bool {
		return conn.UpstreamState().State() == irc.UpstreamStateRegistered &&
			fs.Sessions() == 2
	}, 3*time.Second, 25*time.Millisecond,
		"Resume must reconnect; sessions=%d state=%s", fs.Sessions(), conn.UpstreamState().State())

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("agent did not shut down")
	}
}

// TestSuspendDoesNotEscalateToPausedIdle guards the watchdog fix: a parked
// (user-suspended) link is intentional, not an outage, so the escalation
// watchdog must NOT promote it to the terminal paused_idle state — that would
// tear down a connector the user merely disconnected. We pick warn/pause
// windows short enough that an un-fixed watchdog would have escalated.
func TestSuspendDoesNotEscalateToPausedIdle(t *testing.T) {
	fs := newReconnectTestServer(t)

	conn := irc.New(&irc.Settings{
		Hostname:           "127.0.0.1",
		Port:               fs.Port(),
		Nick:               "turborg",
		UpstreamWarnAfter:  100 * time.Millisecond,
		UpstreamPauseAfter: 300 * time.Millisecond,
	}, nil, nil)

	a := agent.New(nil)
	a.AddConnector(conn)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	require.Eventually(t, func() bool {
		return conn.UpstreamState().State() == irc.UpstreamStateRegistered
	}, 2*time.Second, 10*time.Millisecond, "first registration must complete")

	conn.Suspend()
	require.Eventually(t, func() bool {
		return conn.UpstreamState().State() == irc.UpstreamStateDisconnectedByUser
	}, time.Second, 10*time.Millisecond, "Suspend must park")

	// Well past UpstreamPauseAfter: the state must stay parked, never escalate.
	require.Never(t, func() bool {
		return conn.UpstreamState().State() == irc.UpstreamStatePausedIdle
	}, time.Second, 50*time.Millisecond,
		"a parked connector must not escalate to paused_idle")
	assert.Equal(t, irc.UpstreamStateDisconnectedByUser, conn.UpstreamState().State())

	// And it's still a live, resumable connector — the run loop didn't unwind.
	conn.Resume()
	require.Eventually(t, func() bool {
		return conn.UpstreamState().State() == irc.UpstreamStateRegistered
	}, 3*time.Second, 25*time.Millisecond, "Resume must reconnect after the pause window")

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("agent did not shut down")
	}
}

// TestInvalidNickParksAndResumesOnNewNick covers the invalid-nick recovery
// path: a 432 on the chosen nick parks the supervisor in
// disconnected_nick_invalid (no reconnect storm), and applying a usable nick
// resumes it in place — re-registering under the new nick with no operator
// restart. The server 432s "admin" and welcomes only "guest".
func TestInvalidNickParksAndResumesOnNewNick(t *testing.T) {
	fs := newReconnectTestServerWithReject(t, "ERR_ERRONEOUSNICKNAME_UNTIL_GOOD")
	fs.goodNick = "guest"

	conn := irc.New(&irc.Settings{
		Hostname: "127.0.0.1",
		Port:     fs.Port(),
		Nick:     "admin",
	}, nil, nil)

	a := agent.New(nil)
	a.AddConnector(conn)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	require.Eventually(t, func() bool {
		return conn.UpstreamState().State() == irc.UpstreamStateDisconnectedNickInvalid
	}, 3*time.Second, 10*time.Millisecond, "the invalid nick must park in disconnected_nick_invalid")

	// Parked, not looping: no unbounded reconnect attempts on an unusable nick.
	require.Never(t, func() bool {
		return fs.Sessions() > 2
	}, time.Second, 50*time.Millisecond, "an invalid-nick park must not storm reconnects")

	// Pick a usable nick — this is the resume trigger.
	conn.ApplyNick("guest")

	require.Eventually(t, func() bool {
		return conn.UpstreamState().State() == irc.UpstreamStateRegistered &&
			conn.CurrentNick() == "guest"
	}, 3*time.Second, 25*time.Millisecond,
		"applying a usable nick must resume and re-register; state=%s nick=%s",
		conn.UpstreamState().State(), conn.CurrentNick())

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("agent did not shut down")
	}
}

// TestStartSuspendedDoesNotConnect covers the boot-while-suspended path: a
// connector marked suspended before Start must come up parked rather than
// flapping connect→quit (a pooled tenant built from a suspended spec, or a
// restarted container the user had disconnected). Resume connects it.
func TestStartSuspendedDoesNotConnect(t *testing.T) {
	fs := newReconnectTestServer(t)

	conn := irc.New(&irc.Settings{
		Hostname: "127.0.0.1",
		Port:     fs.Port(),
		Nick:     "turborg",
	}, nil, nil)
	conn.SetInitialSuspended(true)

	a := agent.New(nil)
	a.AddConnector(conn)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	require.Eventually(t, func() bool {
		return conn.UpstreamState().State() == irc.UpstreamStateDisconnectedByUser
	}, time.Second, 10*time.Millisecond, "a suspended boot must park in disconnected_by_user")

	// No upstream connection must have been opened at all.
	require.Never(t, func() bool {
		return fs.Sessions() > 0
	}, time.Second, 50*time.Millisecond, "a suspended boot must not dial upstream")

	conn.Resume()
	require.Eventually(t, func() bool {
		return conn.UpstreamState().State() == irc.UpstreamStateRegistered &&
			fs.Sessions() == 1
	}, 3*time.Second, 25*time.Millisecond, "Resume must perform the first connect")

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("agent did not shut down")
	}
}

// TestSuspendResumeIdempotent verifies the guards: a second Suspend while
// already suspended is a no-op, and a Resume while connected (never suspended)
// doesn't disturb the live session.
func TestSuspendResumeIdempotent(t *testing.T) {
	fs := newReconnectTestServer(t)

	conn := irc.New(&irc.Settings{
		Hostname: "127.0.0.1",
		Port:     fs.Port(),
		Nick:     "turborg",
	}, nil, nil)

	a := agent.New(nil)
	a.AddConnector(conn)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	require.Eventually(t, func() bool {
		return conn.UpstreamState().State() == irc.UpstreamStateRegistered
	}, 2*time.Second, 10*time.Millisecond, "first registration must complete")

	// Resume while connected (not suspended) is a no-op — the session survives.
	conn.Resume()
	require.Never(t, func() bool {
		return conn.UpstreamState().State() != irc.UpstreamStateRegistered
	}, 500*time.Millisecond, 50*time.Millisecond,
		"Resume on a live, non-suspended connector must not disturb it")

	conn.Suspend()
	require.Eventually(t, func() bool {
		return conn.UpstreamState().State() == irc.UpstreamStateDisconnectedByUser
	}, time.Second, 10*time.Millisecond, "Suspend must park")

	// A second Suspend is a no-op (still parked, no extra QUIT churn).
	conn.Suspend()
	assert.Equal(t, irc.UpstreamStateDisconnectedByUser, conn.UpstreamState().State())

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("agent did not shut down")
	}
}
