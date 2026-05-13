package irc_test

import (
	"bufio"
	"context"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/connector/irc"
)

// freshBouncer builds a bouncer listening on a random port. tearDown
// stops it and waits for goroutines so goleak stays happy.
func freshBouncer(t *testing.T, password string, opts ...func(*irc.Bouncer)) (*irc.Bouncer, string) {
	t.Helper()
	b, err := irc.NewBouncer(password, "127.0.0.1", 0, nil, nil)
	require.NoError(t, err)
	for _, o := range opts {
		o(b)
	}
	require.NoError(t, b.Start(context.Background()))
	t.Cleanup(func() { _ = b.Stop() })
	return b, b.Addr()
}

func bouncerClient(t *testing.T, addr string) (net.Conn, *bufio.Reader) {
	t.Helper()
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn, bufio.NewReader(conn)
}

func writeLine(t *testing.T, conn net.Conn, line string) {
	t.Helper()
	_, err := conn.Write([]byte(line + "\r\n"))
	require.NoError(t, err)
}

func readUntil(t *testing.T, r *bufio.Reader, pred func(string) bool, timeout time.Duration) string {
	t.Helper()
	deadline := time.Now().Add(timeout)
	_ = r.Buffered() // touch
	for time.Now().Before(deadline) {
		_ = r // keep ref
		line, err := readLineWithDeadline(r, time.Until(deadline))
		if err != nil {
			t.Fatalf("read failed waiting for predicate: %v", err)
		}
		if pred(line) {
			return line
		}
	}
	t.Fatalf("timed out waiting for predicate")
	return ""
}

func readLineWithDeadline(r *bufio.Reader, _ time.Duration) (string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimRight(line, "\r\n"), nil
}

// --- Construction ----------------------------------------------------------

func TestBouncerRejectsEmptyPassword(t *testing.T) {
	_, err := irc.NewBouncer("", "127.0.0.1", 0, nil, nil)
	require.Error(t, err)
}

// --- Pre-auth NOTICE -------------------------------------------------------

func TestBouncerSendsPreAuthNotice(t *testing.T) {
	_, addr := freshBouncer(t, "hunter2")
	conn, r := bouncerClient(t, addr)
	_ = conn
	line, err := r.ReadString('\n')
	require.NoError(t, err)
	assert.Contains(t, line, "NOTICE AUTH",
		"clients need an auth hint before they send PASS")
}

// --- CAP negotiation -------------------------------------------------------

func TestBouncerCapLS(t *testing.T) {
	_, addr := freshBouncer(t, "hunter2")
	conn, r := bouncerClient(t, addr)
	_, _ = r.ReadString('\n') // notice
	writeLine(t, conn, "CAP LS")
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	line, err := r.ReadString('\n')
	require.NoError(t, err)
	assert.Contains(t, line, "CAP * LS :", "LS returns empty capability list")
}

func TestBouncerCapReqNak(t *testing.T) {
	_, addr := freshBouncer(t, "hunter2")
	conn, r := bouncerClient(t, addr)
	_, _ = r.ReadString('\n')
	writeLine(t, conn, "CAP REQ :sasl multi-prefix")
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	line, err := r.ReadString('\n')
	require.NoError(t, err)
	assert.Contains(t, line, "CAP * NAK :sasl multi-prefix")
}

// --- PASS auth ------------------------------------------------------------

func TestBouncerPassSuccess(t *testing.T) {
	b, addr := freshBouncer(t, "hunter2")
	conn, r := bouncerClient(t, addr)
	_, _ = r.ReadString('\n') // notice
	writeLine(t, conn, "PASS hunter2")

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	line, err := r.ReadString('\n')
	require.NoError(t, err)
	assert.Contains(t, line, " 001 ", "successful PASS returns 001 welcome")

	// Allow handler goroutine to register the client in the map.
	require.Eventually(t, func() bool {
		for _, c := range b.Clients() {
			if c.Authenticated() {
				return true
			}
		}
		return false
	}, time.Second, 10*time.Millisecond)
}

func TestBouncerPassFailureClosesConnection(t *testing.T) {
	_, addr := freshBouncer(t, "hunter2")
	conn, r := bouncerClient(t, addr)
	_, _ = r.ReadString('\n')
	writeLine(t, conn, "PASS wrong")

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	got, err := r.ReadString('\n')
	require.NoError(t, err)
	assert.Contains(t, got, "ERROR :Closing Link")

	// Server should close the conn after sending ERROR.
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err = r.ReadString('\n')
	require.Error(t, err)
}

// --- Rate limit -----------------------------------------------------------

func TestBouncerRateLimitsRepeatedFailures(t *testing.T) {
	rl, err := irc.NewRateLimiter(2, time.Second, 10*time.Second, nil)
	require.NoError(t, err)

	b, err := irc.NewBouncer("hunter2", "127.0.0.1", 0, rl, nil)
	require.NoError(t, err)
	require.NoError(t, b.Start(context.Background()))
	t.Cleanup(func() { _ = b.Stop() })

	for i := 0; i < 3; i++ {
		conn, r := bouncerClient(t, b.Addr())
		_, _ = r.ReadString('\n')
		writeLine(t, conn, "PASS wrong")
		conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		got, err := r.ReadString('\n')
		require.NoError(t, err)
		if i < 2 {
			assert.Contains(t, got, "Bad password",
				"first two failures return the bad-password reason")
		} else {
			assert.Contains(t, got, "Too many attempts",
				"third attempt must return lockout reason")
		}
	}
}

// --- Pre-auth gating -----------------------------------------------------

func TestBouncerDropsPreAuthPrivmsg(t *testing.T) {
	b, addr := freshBouncer(t, "hunter2")
	var upstream []string
	var mu sync.Mutex
	b.AttachUpstream(func(line string) error {
		mu.Lock()
		defer mu.Unlock()
		upstream = append(upstream, line)
		return nil
	})

	conn, r := bouncerClient(t, addr)
	_, _ = r.ReadString('\n')
	writeLine(t, conn, "PRIVMSG #ch :should be silently dropped")
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	got := append([]string{}, upstream...)
	mu.Unlock()
	assert.Empty(t, got, "pre-auth messages must not be forwarded upstream")
}

// --- Upstream forwarding -------------------------------------------------

func TestBouncerForwardsPostAuth(t *testing.T) {
	b, addr := freshBouncer(t, "hunter2")
	var upstream []string
	var mu sync.Mutex
	b.AttachUpstream(func(line string) error {
		mu.Lock()
		defer mu.Unlock()
		upstream = append(upstream, line)
		return nil
	})

	state := irc.NewChannelState()
	state.OnSelfJoin("#test")
	b.AttachState(state, "turborg", "ident", "host")

	conn, r := bouncerClient(t, addr)
	_, _ = r.ReadString('\n')
	writeLine(t, conn, "PASS hunter2")
	// drain welcome + state replay until we've seen the 366 marker.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		line, err := r.ReadString('\n')
		if err != nil {
			break
		}
		if strings.Contains(line, "366") {
			break
		}
	}

	writeLine(t, conn, "PRIVMSG #test :hi")
	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		for _, l := range upstream {
			if strings.Contains(l, "PRIVMSG #test :hi") {
				return true
			}
		}
		return false
	}, time.Second, 10*time.Millisecond, "post-auth message should be forwarded")
}

func TestBouncerCallsObserverOnForwardedPrivmsg(t *testing.T) {
	b, addr := freshBouncer(t, "hunter2")
	b.AttachUpstream(func(string) error { return nil })

	state := irc.NewChannelState()
	state.OnSelfJoin("#test")
	b.AttachState(state, "turborg", "ident", "host")

	type observed struct{ channel, sender, text, kind string }
	var got observed
	var done = make(chan struct{}, 1)
	b.AttachOutboundObserver(func(channel, sender, text, kind string) {
		got = observed{channel, sender, text, kind}
		select {
		case done <- struct{}{}:
		default:
		}
	})

	conn, r := bouncerClient(t, addr)
	_, _ = r.ReadString('\n')
	writeLine(t, conn, "PASS hunter2")
	for {
		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		line, err := r.ReadString('\n')
		if err != nil || strings.Contains(line, "366") {
			break
		}
	}
	writeLine(t, conn, "PRIVMSG #test :hello via bouncer")

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("observer was never called")
	}
	assert.Equal(t, "#test", got.channel)
	assert.Equal(t, "turborg", got.sender)
	assert.Equal(t, "hello via bouncer", got.text)
	assert.Equal(t, "PRIVMSG", got.kind)
}

// --- Broadcasting -------------------------------------------------------

func TestBouncerBroadcastFansOutToOtherClients(t *testing.T) {
	b, addr := freshBouncer(t, "hunter2")
	state := irc.NewChannelState()
	b.AttachState(state, "turborg", "ident", "host")

	connA, rA := bouncerClient(t, addr)
	_, _ = rA.ReadString('\n')
	writeLine(t, connA, "PASS hunter2")
	for {
		connA.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		line, err := rA.ReadString('\n')
		if err != nil || strings.Contains(line, "001") {
			break
		}
	}

	connB, rB := bouncerClient(t, addr)
	_, _ = rB.ReadString('\n')
	writeLine(t, connB, "PASS hunter2")
	for {
		connB.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		line, err := rB.ReadString('\n')
		if err != nil || strings.Contains(line, "001") {
			break
		}
	}

	// Allow handlers to mark both as authenticated.
	require.Eventually(t, func() bool {
		auth := 0
		for _, c := range b.Clients() {
			if c.Authenticated() {
				auth++
			}
		}
		return auth == 2
	}, time.Second, 10*time.Millisecond)

	b.Broadcast(":server 372 turborg :hello fellow clients", nil)

	for _, r := range []*bufio.Reader{rA, rB} {
		r := r
		done := make(chan string, 1)
		go func() {
			for {
				line, err := r.ReadString('\n')
				if err != nil {
					return
				}
				if strings.Contains(line, "hello fellow clients") {
					done <- line
					return
				}
			}
		}()
		select {
		case <-done:
		case <-time.After(time.Second):
			t.Fatal("client missed broadcast")
		}
	}
}

func TestBouncerBroadcastAsSelfPrependsPrefix(t *testing.T) {
	b, addr := freshBouncer(t, "hunter2")
	state := irc.NewChannelState()
	state.OnSelfJoin("#test")
	b.AttachState(state, "turborg", "ident", "host")

	conn, r := bouncerClient(t, addr)
	_, _ = r.ReadString('\n')
	writeLine(t, conn, "PASS hunter2")
	for {
		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		line, err := r.ReadString('\n')
		if err != nil || strings.Contains(line, "366") {
			break
		}
	}

	b.BroadcastAsSelf("PRIVMSG #test :echo", nil)

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if strings.Contains(line, "PRIVMSG #test :echo") {
			assert.Contains(t, line, ":turborg!ident@host",
				"BroadcastAsSelf must prepend the upstream prefix")
			return
		}
	}
}

// --- State replay -------------------------------------------------------

func TestBouncerReplaysStateOnAuth(t *testing.T) {
	b, addr := freshBouncer(t, "hunter2")
	state := irc.NewChannelState()
	state.OnSelfJoin("#test")
	state.SetTopic("#test", "hello world")
	state.SetTopicMeta("#test", "alice", 1700000000)
	state.OnNamesReply("#test", []string{"@turborg", "+alice", "bob"})
	state.OnNamesEnd("#test")
	b.AttachState(state, "turborg", "ident", "host")

	conn, r := bouncerClient(t, addr)
	_, _ = r.ReadString('\n')
	writeLine(t, conn, "PASS hunter2")

	var lines []string
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			break
		}
		lines = append(lines, strings.TrimRight(line, "\r\n"))
		if strings.Contains(line, "End of /NAMES list") {
			break
		}
	}

	var sawJoin, sawTopic, sawTopicWhoTime, sawNames, sawNamesEnd bool
	for _, l := range lines {
		switch {
		case strings.Contains(l, "JOIN #test") && strings.Contains(l, "turborg!ident@host"):
			sawJoin = true
		case strings.Contains(l, " 332 ") && strings.Contains(l, ":hello world"):
			sawTopic = true
		case strings.Contains(l, " 333 ") && strings.Contains(l, "alice 1700000000"):
			sawTopicWhoTime = true
		case strings.Contains(l, " 353 "):
			sawNames = true
		case strings.Contains(l, " 366 "):
			sawNamesEnd = true
		}
	}
	assert.True(t, sawJoin, "synthetic JOIN missing")
	assert.True(t, sawTopic, "RPL_TOPIC missing")
	assert.True(t, sawTopicWhoTime, "RPL_TOPICWHOTIME missing")
	assert.True(t, sawNames, "RPL_NAMREPLY missing")
	assert.True(t, sawNamesEnd, "RPL_ENDOFNAMES missing")
}

// --- Channel log replay -------------------------------------------------

func TestBouncerReplaysChannelLog(t *testing.T) {
	b, addr := freshBouncer(t, "hunter2")
	state := irc.NewChannelState()
	state.OnSelfJoin("#test")
	b.AttachState(state, "turborg", "ident", "host")

	// Record some traffic on the channel before any client connects.
	b.Broadcast(":alice!u@h PRIVMSG #test :hello from before", nil)
	b.Broadcast(":bob!u@h PRIVMSG #test :and from before too", nil)

	conn, r := bouncerClient(t, addr)
	_, _ = r.ReadString('\n')
	writeLine(t, conn, "PASS hunter2")

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	var sawBufferStart, sawHello, sawBufferEnd bool
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			break
		}
		switch {
		case strings.Contains(line, "buffer playback for #test"):
			sawBufferStart = true
		case strings.Contains(line, "hello from before"):
			sawHello = true
		case strings.Contains(line, "end of buffer"):
			sawBufferEnd = true
		}
		if sawBufferEnd {
			break
		}
	}
	assert.True(t, sawBufferStart, "buffer start marker missing")
	assert.True(t, sawHello, "buffered message missing")
	assert.True(t, sawBufferEnd, "buffer end marker missing")
}

// --- PING / PONG --------------------------------------------------------

func TestBouncerPingReplyDoesNotForward(t *testing.T) {
	b, addr := freshBouncer(t, "hunter2")
	var forwarded []string
	var mu sync.Mutex
	b.AttachUpstream(func(line string) error {
		mu.Lock()
		defer mu.Unlock()
		forwarded = append(forwarded, line)
		return nil
	})

	conn, r := bouncerClient(t, addr)
	_, _ = r.ReadString('\n')
	writeLine(t, conn, "PING :cookie")

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	line, err := r.ReadString('\n')
	require.NoError(t, err)
	assert.Contains(t, line, "PONG turborg-bouncer :cookie")

	time.Sleep(50 * time.Millisecond)
	mu.Lock()
	assert.Empty(t, forwarded, "PING must be answered locally, never forwarded")
	mu.Unlock()
}

// --- Malformed forwards dropped --------------------------------------

func TestBouncerUpdateUpstreamNickReflectsInBroadcast(t *testing.T) {
	b, addr := freshBouncer(t, "hunter2")
	state := irc.NewChannelState()
	state.OnSelfJoin("#test")
	b.AttachState(state, "old-nick", "ident", "host")

	conn, r := bouncerClient(t, addr)
	_, _ = r.ReadString('\n')
	writeLine(t, conn, "PASS hunter2")
	for {
		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		line, err := r.ReadString('\n')
		if err != nil || strings.Contains(line, "366") {
			break
		}
	}

	b.UpdateUpstreamNick("new-nick")
	b.UpdateUpstreamIdentity("new-ident", "new-host")
	b.BroadcastAsSelf("PRIVMSG #test :reborn", nil)

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			t.Fatalf("read: %v", err)
		}
		if strings.Contains(line, "reborn") {
			assert.Contains(t, line, ":new-nick!new-ident@new-host",
				"updates to upstream identity must take effect")
			return
		}
	}
}

func TestBouncerPassWithNoParam(t *testing.T) {
	_, addr := freshBouncer(t, "hunter2")
	conn, r := bouncerClient(t, addr)
	_, _ = r.ReadString('\n')
	writeLine(t, conn, "PASS")
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	got, err := r.ReadString('\n')
	require.NoError(t, err)
	assert.Contains(t, got, "Bad password", "empty PASS must be rejected")
}

func TestBouncerCapWithoutSubcommand(t *testing.T) {
	_, addr := freshBouncer(t, "hunter2")
	conn, r := bouncerClient(t, addr)
	_, _ = r.ReadString('\n')
	writeLine(t, conn, "CAP")
	// No reply expected — but the bouncer should keep running.
	time.Sleep(50 * time.Millisecond)
	writeLine(t, conn, "PASS hunter2")
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			t.Fatalf("read after PASS: %v", err)
		}
		if strings.Contains(line, " 001 ") {
			return
		}
	}
}

func TestBouncerDropsEmptyJoin(t *testing.T) {
	b, addr := freshBouncer(t, "hunter2")
	var forwarded []string
	var mu sync.Mutex
	b.AttachUpstream(func(line string) error {
		mu.Lock()
		defer mu.Unlock()
		forwarded = append(forwarded, line)
		return nil
	})

	state := irc.NewChannelState()
	b.AttachState(state, "turborg", "ident", "host")

	conn, r := bouncerClient(t, addr)
	_, _ = r.ReadString('\n')
	writeLine(t, conn, "PASS hunter2")
	for {
		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		line, err := r.ReadString('\n')
		if err != nil || strings.Contains(line, "001") {
			break
		}
	}

	writeLine(t, conn, "JOIN :")
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	for _, l := range forwarded {
		assert.NotContains(t, l, "JOIN :", "malformed JOIN must be dropped")
	}
}
