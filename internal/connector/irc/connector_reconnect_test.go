package irc_test

import (
	"bufio"
	"context"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/agent"
	"github.com/turborg/turborg/internal/connector/irc"
	"github.com/turborg/turborg/tests/fixtures/fakeirc"
)

// TestSupervisorPublishesStateProgressionOnStart verifies the happy
// path: idle → connecting → registering → registered. Subscribers
// (bouncer + web gateway) rely on this ordering to surface "currently
// connecting…" state to freshly-attaching clients.
func TestSupervisorPublishesStateProgressionOnStart(t *testing.T) {
	fs := fakeirc.New(t)
	defer fs.Close()

	conn := irc.New(&irc.Settings{
		Hostname: "127.0.0.1",
		Port:     fs.Port(),
		Nick:     "turborg",
		Channels: []string{"#test"},
	}, nil, nil)

	var mu sync.Mutex
	var seen []irc.UpstreamState
	sub := conn.UpstreamState().Subscribe(func(c irc.UpstreamStateChange) {
		mu.Lock()
		defer mu.Unlock()
		seen = append(seen, c.To)
	})
	defer sub.Unsubscribe()

	a := agent.New(nil)
	a.AddConnector(conn)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	require.True(t,
		fs.WaitFor(containsPrefix("JOIN #test"), 2*time.Second),
		"connector did not register; received: %v", fs.Received(),
	)

	require.Eventually(t, func() bool {
		return conn.UpstreamState().State() == irc.UpstreamStateRegistered
	}, time.Second, 10*time.Millisecond)

	mu.Lock()
	got := append([]irc.UpstreamState(nil), seen...)
	mu.Unlock()
	assert.Equal(t,
		[]irc.UpstreamState{
			irc.UpstreamStateConnecting,
			irc.UpstreamStateRegistering,
			irc.UpstreamStateRegistered,
		},
		got,
		"state transitions during Start must follow connecting→registering→registered",
	)

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("agent did not shut down")
	}
	assert.Equal(t, irc.UpstreamStateStopped, conn.UpstreamState().State())
}

// TestSupervisorTerminalOnAuthFailedHaltsReconnect verifies that the
// SASL auth-fail path classifies the upstream as terminal — the
// supervisor must return an error rather than retry indefinitely, and
// state must reach disconnected_auth_failed so consumers can render the
// "fix your credentials" message.
func TestSupervisorTerminalOnAuthFailedHaltsReconnect(t *testing.T) {
	fs := fakeirc.New(t, fakeirc.WithSASL(fakeirc.SASLFail))
	defer fs.Close()

	conn := irc.New(&irc.Settings{
		Hostname:     "127.0.0.1",
		Port:         fs.Port(),
		Nick:         "turborg",
		Channels:     []string{"#test"},
		SASLUser:     "alice",
		SASLPassword: "wrong",
	}, nil, nil)

	a := agent.New(nil)
	a.AddConnector(conn)

	done := make(chan error, 1)
	go func() { done <- a.Run(context.Background()) }()

	select {
	case err := <-done:
		require.Error(t, err, "auth-failed must surface as a non-nil Run error")
		assert.Contains(t, err.Error(), "SASL failed")
	case <-time.After(3 * time.Second):
		t.Fatal("supervisor did not unwind on SASL auth-fail")
	}
	assert.Equal(t, irc.UpstreamStateDisconnectedAuthFailed, conn.UpstreamState().State())
}

// reconnectTestServer is a minimal in-process IRC server that runs an
// accept loop, completes the standard handshake per session, and lets
// tests Kill() the current session to simulate an upstream drop. Each
// fresh client connection counts as a new session in Sessions().
// writerLike is the narrow slice of net.Conn the joinHook callback
// uses. Defined as an interface so the hook signature doesn't leak
// net.Conn into edge-test code that just needs to write a reply.
type writerLike interface {
	Write(p []byte) (n int, err error)
}

type reconnectTestServer struct {
	t        *testing.T
	listener net.Listener

	mu        sync.Mutex
	conn      net.Conn
	sessions  int32
	received  []string
	closeOnce sync.Once
	closed    chan struct{}

	// rejectAfter, when non-empty, names a pre-MOTD numeric the server
	// replies with instead of 001/376 — exercises the classifier's
	// nick-unavailable / auth-failed / banned branches. The special value
	// "ERR_NICKNAMEINUSE_THEN_OK" rejects the first NICK with 433 then
	// welcomes the suffixed fallback — exercises the 433 fallback path.
	rejectAfter string

	// reg433Sent tracks whether the one-shot 433 has been emitted (for the
	// ERR_NICKNAMEINUSE_THEN_OK fallback mode).
	reg433Sent bool

	// joinHook lets edge tests intercept the JOIN reply. Returning
	// true means "I handled this JOIN" and suppresses the default
	// self-JOIN echo; returning false falls through to the standard
	// behavior. Used by the rejoin-failure tests to surface 474/475/
	// 471/473/476 numerics on specific channels.
	joinHook func(conn writerLike, line string) bool
}

// firstNickForTest is the test-fixture accessor for the cached NICK
// the server saw during this session. Exported (camelCase) for use
// from sibling _test files in the same package.
func (s *reconnectTestServer) firstNickForTest() string {
	return s.firstNickLocked()
}

func newReconnectTestServer(t *testing.T) *reconnectTestServer {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	s := &reconnectTestServer{t: t, listener: l, closed: make(chan struct{})}
	go s.acceptLoop()
	t.Cleanup(s.Close)
	return s
}

// newReconnectTestServerWithReject builds a server that responds to
// USER with a pre-MOTD rejection numeric instead of the usual 001+376
// success path. The rejection drives the supervisor's classifier
// branches for nick-unavailable / auth-failed / banned during
// registration.
func newReconnectTestServerWithReject(t *testing.T, reject string) *reconnectTestServer {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	s := &reconnectTestServer{
		t:           t,
		listener:    l,
		closed:      make(chan struct{}),
		rejectAfter: reject,
	}
	go s.acceptLoop()
	t.Cleanup(s.Close)
	return s
}

func (s *reconnectTestServer) Port() int { return s.listener.Addr().(*net.TCPAddr).Port }
func (s *reconnectTestServer) Sessions() int32 {
	return atomic.LoadInt32(&s.sessions)
}

func (s *reconnectTestServer) Close() {
	s.closeOnce.Do(func() {
		close(s.closed)
		_ = s.listener.Close()
		s.mu.Lock()
		if s.conn != nil {
			_ = s.conn.Close()
		}
		s.mu.Unlock()
	})
}

// Kill closes the currently-active client connection, simulating an
// upstream drop. The next Accept will surface a fresh session.
func (s *reconnectTestServer) Kill() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn != nil {
		_ = s.conn.Close()
		s.conn = nil
	}
}

func (s *reconnectTestServer) Received() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.received))
	copy(out, s.received)
	return out
}

func (s *reconnectTestServer) acceptLoop() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		atomic.AddInt32(&s.sessions, 1)
		s.mu.Lock()
		s.conn = conn
		s.mu.Unlock()
		s.runSession(conn)
		select {
		case <-s.closed:
			return
		default:
		}
	}
}

func (s *reconnectTestServer) runSession(conn net.Conn) {
	reader := bufio.NewReader(conn)
	for {
		select {
		case <-s.closed:
			return
		default:
		}
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimRight(line, "\r\n")
		s.mu.Lock()
		s.received = append(s.received, line)
		s.mu.Unlock()
		s.handleLine(conn, line)
	}
}

func (s *reconnectTestServer) handleLine(conn net.Conn, line string) {
	switch {
	case strings.HasPrefix(line, "CAP REQ"):
		idx := strings.Index(line, " :")
		if idx < 0 {
			return
		}
		caps := strings.Fields(line[idx+2:])
		_, _ = conn.Write([]byte(":fake CAP * ACK :" + strings.Join(caps, " ") + "\r\n"))
	case strings.HasPrefix(line, "NICK "):
		// Fallback mode: 433 the first NICK, welcome the suffixed retry.
		if s.rejectAfter == "ERR_NICKNAMEINUSE_THEN_OK" {
			nick := strings.TrimSpace(strings.TrimPrefix(line, "NICK "))
			s.mu.Lock()
			first := !s.reg433Sent
			s.reg433Sent = true
			s.mu.Unlock()
			if first {
				_, _ = conn.Write([]byte(":fake 433 * " + nick + " :Nickname is already in use\r\n"))
			} else {
				_, _ = conn.Write([]byte(":fake 001 " + nick + " :Welcome\r\n"))
				_, _ = conn.Write([]byte(":fake 376 " + nick + " :End of MOTD\r\n"))
			}
		}
	case strings.HasPrefix(line, "USER "):
		nick := s.firstNickLocked()
		switch s.rejectAfter {
		case "ERR_NICKNAMEINUSE_THEN_OK":
			// Handled on the NICK line(s), not here.
		case "ERR_NICKNAMEINUSE":
			_, _ = conn.Write([]byte(":fake 433 * " + nick + " :Nickname is already in use\r\n"))
		case "ERR_ERRONEOUSNICKNAME":
			_, _ = conn.Write([]byte(":fake 432 * " + nick + " :Erroneous Nickname\r\n"))
		case "ERR_PASSWDMISMATCH":
			_, _ = conn.Write([]byte(":fake 464 " + nick + " :Password incorrect\r\n"))
		case "ERR_YOUREBANNEDCREEP":
			_, _ = conn.Write([]byte(":fake 465 " + nick + " :You are banned\r\n"))
		default:
			_, _ = conn.Write([]byte(":fake 001 " + nick + " :Welcome\r\n"))
			_, _ = conn.Write([]byte(":fake 376 " + nick + " :End of MOTD\r\n"))
		}
	case strings.HasPrefix(line, "JOIN "):
		if s.joinHook != nil && s.joinHook(conn, line) {
			return
		}
		target := strings.TrimSpace(strings.TrimPrefix(line, "JOIN "))
		nick := s.firstNickLocked()
		_, _ = conn.Write([]byte(":" + nick + "!~u@host JOIN " + target + "\r\n"))
	}
}

func (s *reconnectTestServer) firstNickLocked() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, l := range s.received {
		if strings.HasPrefix(l, "NICK ") {
			return strings.TrimSpace(strings.TrimPrefix(l, "NICK "))
		}
	}
	return "*"
}

// TestSupervisorReconnectsAfterUpstreamKill covers the central
// recoverable path: server drops the client mid-session, the
// supervisor's classifier marks transient, the backoff fires, a fresh
// dial succeeds, and the second registration completes. Sessions()
// proves we actually opened a second TCP connection rather than
// silently no-op-ing.
func TestSupervisorReconnectsAfterUpstreamKill(t *testing.T) {
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
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	require.Eventually(t, func() bool {
		return conn.UpstreamState().State() == irc.UpstreamStateRegistered &&
			fs.Sessions() == 1
	}, 2*time.Second, 10*time.Millisecond, "first registration must complete")

	// Kill the current session. The supervisor's read loop sees EOF,
	// classifies transient, sleeps backoff (1s ±20% on the first
	// attempt), and reconnects.
	fs.Kill()

	require.Eventually(t, func() bool {
		return conn.UpstreamState().State() == irc.UpstreamStateRegistered &&
			fs.Sessions() == 2
	}, 5*time.Second, 50*time.Millisecond,
		"supervisor must reconnect; sessions=%d state=%s", fs.Sessions(), conn.UpstreamState().State())

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("agent did not shut down")
	}
}

// TestRegisterFallsBackOnNickInUse covers the 433 fallback: the server
// rejects the first NICK as in-use, and instead of aborting (which would
// drop into the reconnect loop and race our own ghost), the connector
// suffixes "_" and completes registration under the fallback nick. The
// desired nick is preserved as the reclaim target.
func TestRegisterFallsBackOnNickInUse(t *testing.T) {
	fs := newReconnectTestServerWithReject(t, "ERR_NICKNAMEINUSE_THEN_OK")

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
	}, 3*time.Second, 10*time.Millisecond, "must register on the suffixed fallback nick")

	require.Equal(t, "turborg_", conn.CurrentNick(), "live nick is the fallback")
	require.Equal(t, "turborg", conn.DesiredNick(), "desired (reclaim target) stays the original")

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("agent did not shut down")
	}
}

// TestRegisterDoesNotFallBackOnErroneousNick: a 432 (erroneous/reserved
// nick — e.g. a services-restricted name) must NOT trigger the "_" fallback,
// because appending "_" can't make an invalid nick valid. It surfaces as
// nick-unavailable and reconnects (under the throttle) with the same nick.
func TestRegisterDoesNotFallBackOnErroneousNick(t *testing.T) {
	fs := newReconnectTestServerWithReject(t, "ERR_ERRONEOUSNICKNAME")

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
		return conn.UpstreamState().State() == irc.UpstreamStateDisconnectedNickUnavailable
	}, 3*time.Second, 10*time.Millisecond, "432 must surface as nick-unavailable")

	for _, l := range fs.Received() {
		require.NotContains(t, l, "NICK admin_",
			"a 432 erroneous nick must NOT fall back to a _-suffixed nick")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("agent did not shut down")
	}
}

// TestSupervisorEscalatesTransientToPausedIdle confirms the long-outage
// escalation: when the upstream stays unreachable past UpstreamPauseAfter,
// the watchdog transitions the state machine to paused_idle and the
// supervisor's Run unwinds with a terminal error.
//
// Pattern: complete the initial registration so Start succeeds and the
// supervisor takes over, then Close() the fixture so subsequent dials
// get connection-refused. The supervisor's reconnect sleeps + retries
// keep it in disconnected_transient long enough for the watchdog to
// escalate.
func TestSupervisorEscalatesTransientToPausedIdle(t *testing.T) {
	fs := newReconnectTestServer(t)

	conn := irc.New(&irc.Settings{
		Hostname:           "127.0.0.1",
		Port:               fs.Port(),
		Nick:               "turborg",
		Channels:           []string{"#test"},
		UpstreamPauseAfter: 800 * time.Millisecond,
	}, nil, nil)

	// Subscribe up front — agent.Stop transitions the machine to
	// stopped immediately after Run unwinds, so the final State() read
	// would lose the paused_idle observation. Capturing transitions as
	// they happen gives the test a reliable proof.
	sawPaused := make(chan struct{}, 1)
	sub := conn.UpstreamState().Subscribe(func(c irc.UpstreamStateChange) {
		if c.To == irc.UpstreamStatePausedIdle {
			select {
			case sawPaused <- struct{}{}:
			default:
			}
		}
	})
	defer sub.Unsubscribe()

	a := agent.New(nil)
	a.AddConnector(conn)
	done := make(chan error, 1)
	go func() { done <- a.Run(context.Background()) }()

	require.Eventually(t, func() bool {
		return conn.UpstreamState().State() == irc.UpstreamStateRegistered
	}, 2*time.Second, 10*time.Millisecond, "initial registration must succeed")

	// Take the upstream offline. Current session dies on next read;
	// every retry hereafter gets connection-refused.
	fs.Close()

	select {
	case <-sawPaused:
	case <-time.After(8 * time.Second):
		t.Fatalf("supervisor never escalated to paused_idle; state=%s",
			conn.UpstreamState().State())
	}

	select {
	case err := <-done:
		require.Error(t, err, "paused_idle must surface as a Run error")
		assert.Contains(t, err.Error(), "paused_idle")
	case <-time.After(2 * time.Second):
		t.Fatalf("Run did not return after paused_idle escalation; current state=%s",
			conn.UpstreamState().State())
	}
}

// TestSupervisorWarnHookFiresInTransient confirms the long-outage
// warn callback fires once per transient-outage window — the bouncer
// (or any other subscriber) will use this to broadcast a "still
// retrying" NOTICE. Here we just verify the supervisor invokes it
// with the right shape.
func TestSupervisorWarnHookFiresInTransient(t *testing.T) {
	fs := newReconnectTestServer(t)

	var warns int32
	var lastReason string
	var hookMu sync.Mutex
	conn := irc.New(&irc.Settings{
		Hostname:           "127.0.0.1",
		Port:               fs.Port(),
		Nick:               "turborg",
		Channels:           []string{"#test"},
		UpstreamWarnAfter:  200 * time.Millisecond,
		UpstreamPauseAfter: 10 * time.Second, // keep escalation out of scope here
	}, nil, nil)
	conn.SetUpstreamWarnHook(func(reason string, _ time.Duration) {
		hookMu.Lock()
		lastReason = reason
		hookMu.Unlock()
		atomic.AddInt32(&warns, 1)
	})

	a := agent.New(nil)
	a.AddConnector(conn)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	require.Eventually(t, func() bool {
		return conn.UpstreamState().State() == irc.UpstreamStateRegistered
	}, 2*time.Second, 10*time.Millisecond, "initial registration must succeed")

	// Take the upstream offline so the supervisor stays in transient.
	fs.Close()

	require.Eventually(t, func() bool {
		return atomic.LoadInt32(&warns) >= 1
	}, 4*time.Second, 25*time.Millisecond,
		"warn hook must fire after %s in transient", 200*time.Millisecond)

	hookMu.Lock()
	gotReason := lastReason
	hookMu.Unlock()
	// Server reason is the wrapped Go error from the failing read or
	// the failing reconnect dial — neither is empty.
	assert.NotEmpty(t, gotReason,
		"warn hook should receive the propagated server/transport reason")

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("agent did not shut down")
	}
	// The hook is "once per transient-dwell window" — not strict-once
	// across all dwells, since each reconnect-failure re-enters
	// transient. Lower bound is what matters: at least one fire.
	assert.GreaterOrEqual(t, atomic.LoadInt32(&warns), int32(1))
}

// TestSupervisorRespectsCtxCancelDuringBackoffSleep confirms that
// cancellation during the inter-attempt sleep unwinds Run cleanly
// rather than waiting out the full backoff.
func TestSupervisorRespectsCtxCancelDuringBackoffSleep(t *testing.T) {
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
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	// Complete initial registration so Start hands off to the supervisor.
	require.Eventually(t, func() bool {
		return conn.UpstreamState().State() == irc.UpstreamStateRegistered
	}, 2*time.Second, 10*time.Millisecond)

	// Bounce the connection so the supervisor enters its backoff sleep.
	fs.Close()
	require.Eventually(t, func() bool {
		return conn.UpstreamState().State() == irc.UpstreamStateDisconnectedTransient
	}, 2*time.Second, 10*time.Millisecond, "supervisor must classify the drop as transient")

	startCancel := time.Now()
	cancel()
	select {
	case err := <-done:
		assert.NoError(t, err, "ctx-cancel must produce a nil Run return")
		assert.Less(t, time.Since(startCancel), 500*time.Millisecond,
			"cancel during backoff sleep must unwind well under the 1s base sleep")
	case <-time.After(2 * time.Second):
		t.Fatal("supervisor did not honour ctx cancel during backoff sleep")
	}
	assert.Equal(t, irc.UpstreamStateStopped, conn.UpstreamState().State())
}
