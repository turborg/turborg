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

func TestBouncerCapLSAdvertisesEchoMessage(t *testing.T) {
	_, addr := freshBouncer(t, "hunter2")
	conn, r := bouncerClient(t, addr)
	_, _ = r.ReadString('\n')
	writeLine(t, conn, "CAP LS")
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	line, err := r.ReadString('\n')
	require.NoError(t, err)
	assert.Contains(t, line, "echo-message",
		"LS must advertise echo-message so attached clients can negotiate self-echo")
}

func TestBouncerCapReqEchoMessageAcked(t *testing.T) {
	_, addr := freshBouncer(t, "hunter2")
	conn, r := bouncerClient(t, addr)
	_, _ = r.ReadString('\n')
	writeLine(t, conn, "CAP REQ :echo-message")
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	line, err := r.ReadString('\n')
	require.NoError(t, err)
	assert.Contains(t, line, "CAP * ACK :echo-message")
}

func TestBouncerWelcomeDeferredUntilCapEnd(t *testing.T) {
	// IRCv3: when a client enters CAP negotiation (CAP LS or CAP REQ),
	// registration is suspended until CAP END. The bouncer must NOT
	// emit the 001 welcome immediately after PASS — it must hold it
	// until CAP END so the cap set is active at registration time.
	_, addr := freshBouncer(t, "hunter2")
	conn, r := bouncerClient(t, addr)
	_, _ = r.ReadString('\n') // pre-auth NOTICE

	writeLine(t, conn, "CAP LS")
	conn.SetReadDeadline(time.Now().Add(time.Second))
	line, err := r.ReadString('\n')
	require.NoError(t, err)
	require.Contains(t, line, "CAP * LS")

	writeLine(t, conn, "CAP REQ :echo-message")
	conn.SetReadDeadline(time.Now().Add(time.Second))
	line, err = r.ReadString('\n')
	require.NoError(t, err)
	require.Contains(t, line, "CAP * ACK :echo-message")

	writeLine(t, conn, "PASS hunter2")

	// Before CAP END, 001 must NOT have arrived — read with a short
	// deadline and expect a timeout.
	conn.SetReadDeadline(time.Now().Add(150 * time.Millisecond))
	got, err := r.ReadString('\n')
	if err == nil {
		assert.NotContains(t, got, " 001 ",
			"001 welcome leaked before CAP END; got %q", got)
	}

	writeLine(t, conn, "CAP END")

	// Now 001 must arrive (the deferred welcome flushes).
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	found001 := false
	for !found001 {
		l, err := r.ReadString('\n')
		if err != nil {
			t.Fatalf("expected 001 after CAP END; got error %v", err)
		}
		if strings.Contains(l, " 001 ") {
			found001 = true
		}
	}
}

func TestBouncerWelcomeImmediateWithoutCap(t *testing.T) {
	// Client that never sends CAP LS gets normal registration —
	// 001 fires immediately after PASS success. Regression for the
	// non-IRCv3 client path.
	_, addr := freshBouncer(t, "hunter2")
	conn, r := bouncerClient(t, addr)
	_, _ = r.ReadString('\n')

	writeLine(t, conn, "PASS hunter2")
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	line, err := r.ReadString('\n')
	require.NoError(t, err)
	assert.Contains(t, line, " 001 ", "without CAP negotiation, 001 is immediate")
}

func TestBouncerCapReqMixedAckAndNak(t *testing.T) {
	_, addr := freshBouncer(t, "hunter2")
	conn, r := bouncerClient(t, addr)
	_, _ = r.ReadString('\n')
	writeLine(t, conn, "CAP REQ :echo-message multi-prefix")
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	// Two replies: one ACK + one NAK; order isn't strictly defined.
	first, err := r.ReadString('\n')
	require.NoError(t, err)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	second, err := r.ReadString('\n')
	require.NoError(t, err)
	combined := first + second
	assert.Contains(t, combined, "ACK :echo-message")
	assert.Contains(t, combined, "NAK :multi-prefix")
}

func TestBouncerEchoMessageFansBackToOriginator(t *testing.T) {
	b, addr := freshBouncer(t, "hunter2")
	b.AttachUpstream(func(string) error { return nil })
	state := irc.NewChannelState()
	state.OnSelfJoin("#test")
	b.AttachState(state, "bot", "user", "host")

	conn, r := bouncerClient(t, addr)
	_, _ = r.ReadString('\n')
	// Negotiate echo-message before PASS so we have it active at PRIVMSG time.
	writeLine(t, conn, "CAP REQ :echo-message")
	conn.SetReadDeadline(time.Now().Add(time.Second))
	_, _ = r.ReadString('\n') // ACK line
	writeLine(t, conn, "PASS hunter2")
	// Drain welcome + replay until 001.
	for {
		conn.SetReadDeadline(time.Now().Add(time.Second))
		line, err := r.ReadString('\n')
		if err != nil || strings.Contains(line, "001") {
			break
		}
	}

	writeLine(t, conn, "PRIVMSG #test :hello to myself")

	// With echo-message active, the originator should receive a copy
	// of its own PRIVMSG back. Without echo-message it would not.
	conn.SetReadDeadline(time.Now().Add(time.Second))
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			t.Fatalf("expected echoed PRIVMSG but got error: %v", err)
		}
		if strings.Contains(line, "PRIVMSG #test :hello to myself") &&
			strings.Contains(line, ":bot!user@host") {
			return
		}
	}
}

func TestBouncerZNCSelfMessageTagsBroadcast(t *testing.T) {
	// HexChat 2.12-2.15 supports znc.in/self-message but not the
	// standardized echo-message cap. The bouncer should advertise both
	// and prefix fan-out lines with the @znc.in/self-message tag for
	// clients that negotiated the legacy cap.
	b, addr := freshBouncer(t, "hunter2")
	state := irc.NewChannelState()
	state.OnSelfJoin("#test")
	b.AttachState(state, "bot", "user", "host")

	// Connect a SECOND bouncer client (so the originator and observer
	// roles are clearly separated). The observer negotiates the legacy
	// cap; we then have the bouncer fan a self-message to it.
	conn, r := bouncerClient(t, addr)
	_, _ = r.ReadString('\n')
	writeLine(t, conn, "CAP REQ :znc.in/self-message")
	conn.SetReadDeadline(time.Now().Add(time.Second))
	_, _ = r.ReadString('\n') // ACK
	writeLine(t, conn, "PASS hunter2")
	writeLine(t, conn, "CAP END")
	for {
		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		line, err := r.ReadString('\n')
		if err != nil || strings.Contains(line, "366") {
			break
		}
	}

	b.BroadcastAsSelf("PRIVMSG StephenS :from somewhere else", nil)

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	var sawTagged bool
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			break
		}
		if strings.Contains(line, "PRIVMSG StephenS :from somewhere else") {
			assert.True(t, strings.HasPrefix(line, "@znc.in/self-message "),
				"client with znc.in/self-message cap must receive the tag prefix; got %q", line)
			sawTagged = true
			break
		}
	}
	assert.True(t, sawTagged, "broadcast never reached the cap-aware client")
}

func TestBouncerEchoMessageOnlyNoSelfMessageTag(t *testing.T) {
	// Conversely, a client that negotiated echo-message (but NOT
	// znc.in/self-message) must NOT see the legacy tag prepended —
	// modern clients reject lines with unknown tags they didn't
	// negotiate to receive.
	b, addr := freshBouncer(t, "hunter2")
	state := irc.NewChannelState()
	state.OnSelfJoin("#test")
	b.AttachState(state, "bot", "user", "host")

	conn, r := bouncerClient(t, addr)
	_, _ = r.ReadString('\n')
	writeLine(t, conn, "CAP REQ :echo-message")
	conn.SetReadDeadline(time.Now().Add(time.Second))
	_, _ = r.ReadString('\n') // ACK
	writeLine(t, conn, "PASS hunter2")
	writeLine(t, conn, "CAP END")
	for {
		conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
		line, err := r.ReadString('\n')
		if err != nil || strings.Contains(line, "366") {
			break
		}
	}

	b.BroadcastAsSelf("PRIVMSG StephenS :no tag here", nil)

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			break
		}
		if strings.Contains(line, "PRIVMSG StephenS :no tag here") {
			assert.False(t, strings.HasPrefix(line, "@znc.in/self-message "),
				"echo-message-only client must NOT see the legacy tag; got %q", line)
			return
		}
	}
	t.Fatal("broadcast never reached the echo-message client")
}

func TestBouncerFansSelfDMToClientWithoutCap(t *testing.T) {
	// The bouncer fans self-PRIVMSGs to every authenticated client
	// regardless of cap state. We tried gating on echo-message /
	// znc.in/self-message earlier, but HexChat 2.16's auto-cap-
	// request silently skips both caps — gating made web-originated
	// DMs invisible in HexChat entirely. Operators preferred "wrong
	// tab name in HexChat" over "no message in HexChat", so the
	// fan-out is unconditional now. Clients that DO negotiate
	// znc.in/self-message additionally get the @-tag for correct
	// routing (TestBouncerZNCSelfMessageTagsBroadcast covers that).
	b, addr := freshBouncer(t, "hunter2")
	state := irc.NewChannelState()
	b.AttachState(state, "bot", "user", "host")

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

	b.BroadcastAsSelf("PRIVMSG StephenS :from web", nil)

	conn.SetReadDeadline(time.Now().Add(time.Second))
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			t.Fatalf("expected DM fan to reach the cap-less client, got error: %v", err)
		}
		if strings.Contains(line, "PRIVMSG StephenS :from web") {
			return // pass
		}
	}
}

func TestBouncerFansSelfChannelMessageToClientWithoutCap(t *testing.T) {
	// Channel-targeted self-PRIVMSG must still fan to every attached
	// client, cap or no cap — without the fan, HexChat in the channel
	// wouldn't see traffic the web client originated, and conversation
	// context would be broken across surfaces.
	b, addr := freshBouncer(t, "hunter2")
	state := irc.NewChannelState()
	state.OnSelfJoin("#test")
	b.AttachState(state, "bot", "user", "host")

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

	b.BroadcastAsSelf("PRIVMSG #test :greeting from web", nil)

	conn.SetReadDeadline(time.Now().Add(time.Second))
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			t.Fatalf("expected channel fan to a cap-less client, got error: %v", err)
		}
		if strings.Contains(line, "PRIVMSG #test :greeting from web") {
			return // pass
		}
	}
}

func TestBouncerWithoutEchoMessageExcludesOriginator(t *testing.T) {
	b, addr := freshBouncer(t, "hunter2")
	b.AttachUpstream(func(string) error { return nil })
	state := irc.NewChannelState()
	state.OnSelfJoin("#test")
	b.AttachState(state, "bot", "user", "host")

	conn, r := bouncerClient(t, addr)
	_, _ = r.ReadString('\n')
	// No CAP negotiation — echo-message not requested.
	writeLine(t, conn, "PASS hunter2")
	for {
		conn.SetReadDeadline(time.Now().Add(time.Second))
		line, err := r.ReadString('\n')
		if err != nil || strings.Contains(line, "001") {
			break
		}
	}

	writeLine(t, conn, "PRIVMSG #test :hello local-only")

	// 200ms should be more than enough — the originator must NOT
	// see an echo of its own PRIVMSG when echo-message wasn't negotiated.
	conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return // timeout = success: no echo received
		}
		if strings.Contains(line, "PRIVMSG #test :hello local-only") {
			t.Fatalf("originator received its own message without echo-message cap: %s", line)
		}
	}
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

// negotiateAndPass walks the IRCv3 CAP LS → CAP REQ <caps> → PASS →
// CAP END handshake against the bouncer and returns once auth+welcome
// have completed. Used by the replay-decoration tests to put the
// client into a known cap state before it consumes replay output.
func negotiateAndPass(t *testing.T, conn net.Conn, r *bufio.Reader, caps, password string) {
	t.Helper()
	_, _ = r.ReadString('\n') // pre-auth NOTICE

	writeLine(t, conn, "CAP LS")
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err := r.ReadString('\n')
	require.NoError(t, err)

	writeLine(t, conn, "CAP REQ :"+caps)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	ack, err := r.ReadString('\n')
	require.NoError(t, err)
	require.Contains(t, ack, "CAP * ACK :", "expected ACK for negotiated caps, got %q", ack)

	writeLine(t, conn, "PASS "+password)
	writeLine(t, conn, "CAP END")
}

func TestBouncerReplayCarriesServerTimeTag(t *testing.T) {
	// Clients that negotiated server-time must receive replayed lines
	// with an `@time=<ISO8601 UTC>` prefix so HexChat / mIRC / irssi
	// render them as historical and don't highlight the channel tab
	// as fresh activity. This is the user-visible fix for the "buffer
	// playback looks like new messages" bug.
	b, addr := freshBouncer(t, "hunter2")
	state := irc.NewChannelState()
	state.OnSelfJoin("#test")
	b.AttachState(state, "turborg", "ident", "host")

	b.Broadcast(":alice!u@h PRIVMSG #test :hello from before", nil)

	conn, r := bouncerClient(t, addr)
	negotiateAndPass(t, conn, r, "server-time message-tags", "hunter2")

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	var sawTaggedReplay bool
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			break
		}
		if strings.Contains(line, "PRIVMSG #test") &&
			strings.Contains(line, "hello from before") {
			// Tag block must precede the prefix and contain time= in
			// the expected ISO8601 format (RFC3339-ish with millis).
			assert.True(t, strings.HasPrefix(line, "@"),
				"server-time replay must start with @tags, got %q", line)
			assert.Contains(t, line, "time=",
				"server-time replay must carry time= tag, got %q", line)
			sawTaggedReplay = true
			break
		}
	}
	assert.True(t, sawTaggedReplay, "expected tagged replay line for #test")
}

func TestBouncerReplayWrappedInChathistoryBatch(t *testing.T) {
	// Clients that negotiated the `batch` cap get the per-channel
	// replay block wrapped in `BATCH +<id> chathistory <channel>` /
	// `BATCH -<id>` with every replayed line tagged `batch=<id>`. The
	// legacy NOTICE markers are dropped in this case — capable clients
	// would render them as out-of-band server NOTICEs, which is wrong.
	b, addr := freshBouncer(t, "hunter2")
	state := irc.NewChannelState()
	state.OnSelfJoin("#test")
	b.AttachState(state, "turborg", "ident", "host")

	b.Broadcast(":alice!u@h PRIVMSG #test :hello from before", nil)

	conn, r := bouncerClient(t, addr)
	negotiateAndPass(t, conn, r, "batch message-tags", "hunter2")

	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	var batchStart, batchEnd, batchedLine, sawLegacyMarker bool
	var batchID string
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			break
		}
		line = strings.TrimRight(line, "\r\n")
		switch {
		case strings.HasPrefix(line, "BATCH +"):
			// Format: "BATCH +<id> chathistory <channel>"
			rest := strings.TrimPrefix(line, "BATCH +")
			parts := strings.Fields(rest)
			require.GreaterOrEqual(t, len(parts), 3, "BATCH start malformed: %q", line)
			batchID = parts[0]
			assert.Equal(t, "chathistory", parts[1])
			assert.Equal(t, "#test", parts[2])
			batchStart = true
		case strings.HasPrefix(line, "BATCH -") && batchID != "":
			assert.Equal(t, "BATCH -"+batchID, line)
			batchEnd = true
		case strings.Contains(line, "PRIVMSG #test") &&
			strings.Contains(line, "hello from before"):
			assert.Contains(t, line, "batch="+batchID,
				"replay line must carry the batch tag: %q", line)
			batchedLine = true
		case strings.Contains(line, "buffer playback for #test"),
			strings.Contains(line, "end of buffer"):
			sawLegacyMarker = true
		}
		if batchEnd {
			break
		}
	}
	assert.True(t, batchStart, "missing BATCH start")
	assert.True(t, batchEnd, "missing BATCH end")
	assert.True(t, batchedLine, "missing batched replay line")
	assert.False(t, sawLegacyMarker, "legacy NOTICE markers must be suppressed for batch-cap clients")
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

// --- Operator-policy gating ------------------------------------------------

// trackForwarded wires AttachUpstream so the test can assert which lines
// (if any) the bouncer forwarded to the upstream IRC connection. Returns
// the slice + its mutex; the caller locks while reading.
func trackForwarded(b *irc.Bouncer) (*[]string, *sync.Mutex) {
	var lines []string
	var mu sync.Mutex
	b.AttachUpstream(func(line string) error {
		mu.Lock()
		defer mu.Unlock()
		lines = append(lines, line)
		return nil
	})
	return &lines, &mu
}

// authBouncerClient connects, sends PASS, drains the welcome burst and
// returns the live conn + reader at the post-001 prompt.
func authBouncerClient(t *testing.T, addr string) (net.Conn, *bufio.Reader) {
	t.Helper()
	conn, r := bouncerClient(t, addr)
	_, _ = r.ReadString('\n') // pre-auth notice
	writeLine(t, conn, "PASS hunter2")
	for {
		conn.SetReadDeadline(time.Now().Add(time.Second))
		line, err := r.ReadString('\n')
		if err != nil || strings.Contains(line, " 001 ") {
			break
		}
	}
	return conn, r
}

// readUntilContains keeps reading until the deadline hits, looking for a
// substring the test expects. Returns the matching line or empty string
// on timeout.
func readUntilContains(r *bufio.Reader, conn net.Conn, substr string, deadline time.Duration) string {
	conn.SetReadDeadline(time.Now().Add(deadline))
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return ""
		}
		if strings.Contains(line, substr) {
			return line
		}
	}
}

func TestBouncerNickLockedDropsNickUpstream(t *testing.T) {
	b, addr := freshBouncer(t, "hunter2")
	forwarded, mu := trackForwarded(b)
	b.AttachState(irc.NewChannelState(), "turborg", "ident", "host")
	b.AttachClientLimits(irc.ClientLimits{NickLocked: true})

	conn, r := authBouncerClient(t, addr)
	writeLine(t, conn, "NICK newnick")

	// Policy denials now flow as PRIVMSG from the *turborg virtual
	// service nick — IRC clients open a dedicated query buffer for it
	// rather than spamming real channels.
	notice := readUntilContains(r, conn, "Nick change", 500*time.Millisecond)
	assert.NotEmpty(t, notice, "client must receive a service PRIVMSG explaining the rejection")
	assert.Contains(t, notice, "*turborg",
		"service messages must be sourced from the *turborg virtual nick")
	assert.Contains(t, notice, "PRIVMSG",
		"service messages route as PRIVMSG so clients open a query buffer")

	mu.Lock()
	defer mu.Unlock()
	for _, l := range *forwarded {
		assert.NotContains(t, l, "NICK newnick",
			"NICK must not flow upstream when ClientLimits.NickLocked is set")
	}
}

func TestBouncerNickLockedAllowsOtherCommands(t *testing.T) {
	// Make sure NickLocked doesn't accidentally cordon off PRIVMSG / JOIN.
	b, addr := freshBouncer(t, "hunter2")
	forwarded, mu := trackForwarded(b)
	b.AttachState(irc.NewChannelState(), "turborg", "ident", "host")
	b.AttachClientLimits(irc.ClientLimits{NickLocked: true})

	conn, _ := authBouncerClient(t, addr)
	writeLine(t, conn, "PRIVMSG #room :hello")
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	var found bool
	for _, l := range *forwarded {
		if strings.Contains(l, "PRIVMSG #room :hello") {
			found = true
			break
		}
	}
	assert.True(t, found, "PRIVMSG must still flow upstream under NickLocked")
}

func TestBouncerMaxChannelsRejectsJoinAtCap(t *testing.T) {
	b, addr := freshBouncer(t, "hunter2")
	forwarded, mu := trackForwarded(b)

	// State has 5 channels already joined; cap is 5 → next JOIN is rejected.
	state := irc.NewChannelState()
	state.OnSelfJoin("#a")
	state.OnSelfJoin("#b")
	state.OnSelfJoin("#c")
	state.OnSelfJoin("#d")
	state.OnSelfJoin("#e")
	b.AttachState(state, "turborg", "ident", "host")
	b.AttachClientLimits(irc.ClientLimits{MaxChannels: 5})

	conn, r := authBouncerClient(t, addr)
	writeLine(t, conn, "JOIN #f")

	notice := readUntilContains(r, conn, "Channel cap reached", 500*time.Millisecond)
	assert.NotEmpty(t, notice, "client must receive a service PRIVMSG when JOIN exceeds the channel cap")
	assert.Contains(t, notice, "#f",
		"body must name the channel the user tried to join")
	assert.Contains(t, notice, "*turborg",
		"channel-cap denials now flow through the *turborg service buffer")

	mu.Lock()
	defer mu.Unlock()
	for _, l := range *forwarded {
		assert.NotContains(t, l, "JOIN #f",
			"JOIN must not flow upstream when MaxChannels is reached")
	}
}

func TestBouncerMaxChannelsAllowsJoinBelowCap(t *testing.T) {
	b, addr := freshBouncer(t, "hunter2")
	forwarded, mu := trackForwarded(b)

	state := irc.NewChannelState()
	state.OnSelfJoin("#a")
	state.OnSelfJoin("#b")
	b.AttachState(state, "turborg", "ident", "host")
	b.AttachClientLimits(irc.ClientLimits{MaxChannels: 5})

	conn, _ := authBouncerClient(t, addr)
	writeLine(t, conn, "JOIN #c")
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	var found bool
	for _, l := range *forwarded {
		if strings.Contains(l, "JOIN #c") {
			found = true
			break
		}
	}
	assert.True(t, found, "JOIN below the cap must flow upstream")
}

func TestBouncerPerTargetOutboundThrottleRejectsAfterCap(t *testing.T) {
	throttle, err := irc.NewThrottle(2, 30*time.Second, nil)
	require.NoError(t, err)

	b, addr := freshBouncer(t, "hunter2")
	forwarded, mu := trackForwarded(b)
	b.AttachState(irc.NewChannelState(), "turborg", "ident", "host")
	b.AttachOutboundThrottle(throttle)

	conn, r := authBouncerClient(t, addr)
	for i := 0; i < 2; i++ {
		writeLine(t, conn, "PRIVMSG #x :one")
	}
	time.Sleep(50 * time.Millisecond)

	// Third PRIVMSG hits the cap → NOTICE with retry-after, line dropped.
	writeLine(t, conn, "PRIVMSG #x :three")
	notice := readUntilContains(r, conn, "rate-limited", 500*time.Millisecond)
	assert.NotEmpty(t, notice, "client must receive a NOTICE when outbound throttle fires")
	assert.Contains(t, notice, "NOT sent")

	mu.Lock()
	defer mu.Unlock()
	thirdSeen := false
	twoSeen := 0
	for _, l := range *forwarded {
		if strings.Contains(l, ":three") {
			thirdSeen = true
		}
		if strings.Contains(l, ":one") {
			twoSeen++
		}
	}
	assert.False(t, thirdSeen, "third PRIVMSG must be dropped at the throttle")
	assert.Equal(t, 2, twoSeen, "first two PRIVMSGs forwarded")
}

func TestBouncerPerTargetOutboundThrottleScopesIndependently(t *testing.T) {
	// One bucket per target — a chatty user in #a must not lock out #b.
	throttle, err := irc.NewThrottle(1, 30*time.Second, nil)
	require.NoError(t, err)

	b, addr := freshBouncer(t, "hunter2")
	forwarded, mu := trackForwarded(b)
	b.AttachState(irc.NewChannelState(), "turborg", "ident", "host")
	b.AttachOutboundThrottle(throttle)

	conn, _ := authBouncerClient(t, addr)
	writeLine(t, conn, "PRIVMSG #a :hi")
	writeLine(t, conn, "PRIVMSG #b :hi")
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	var sawA, sawB bool
	for _, l := range *forwarded {
		if strings.Contains(l, "PRIVMSG #a") {
			sawA = true
		}
		if strings.Contains(l, "PRIVMSG #b") {
			sawB = true
		}
	}
	assert.True(t, sawA, "#a should be forwarded")
	assert.True(t, sawB, "#b is a separate bucket — must still be forwarded")
}

func TestBouncerActivityHookFiresOnAuthSuccess(t *testing.T) {
	b, addr := freshBouncer(t, "hunter2")
	var (
		mu      sync.Mutex
		reasons []string
	)
	b.AttachActivityHook(func(reason string) {
		mu.Lock()
		reasons = append(reasons, reason)
		mu.Unlock()
	})

	conn, r := bouncerClient(t, addr)
	_, _ = r.ReadString('\n')
	writeLine(t, conn, "PASS hunter2")
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err := r.ReadString('\n')
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(reasons) == 1 && reasons[0] == "bouncer_attach"
	}, time.Second, 10*time.Millisecond, "hook must fire exactly once with reason=bouncer_attach")
}

func TestBouncerActivityHookSilentOnAuthFailure(t *testing.T) {
	b, addr := freshBouncer(t, "hunter2")
	var fired bool
	var mu sync.Mutex
	b.AttachActivityHook(func(_ string) {
		mu.Lock()
		fired = true
		mu.Unlock()
	})

	conn, r := bouncerClient(t, addr)
	_, _ = r.ReadString('\n')
	writeLine(t, conn, "PASS wrong")
	// Wait long enough that any hook fired during handlePass would land.
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	assert.False(t, fired, "auth failure must not fire the activity hook")
}

func TestBouncerZeroClientLimitsIsUnrestricted(t *testing.T) {
	// Default (zero) ClientLimits should pass everything through — guards
	// the operator who doesn't configure any policy.
	b, addr := freshBouncer(t, "hunter2")
	forwarded, mu := trackForwarded(b)
	b.AttachState(irc.NewChannelState(), "turborg", "ident", "host")
	// No AttachClientLimits call — limits stay at the zero value.

	conn, _ := authBouncerClient(t, addr)
	writeLine(t, conn, "NICK whatever")
	writeLine(t, conn, "JOIN #x")
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	var sawNick, sawJoin bool
	for _, l := range *forwarded {
		if strings.Contains(l, "NICK whatever") {
			sawNick = true
		}
		if strings.Contains(l, "JOIN #x") {
			sawJoin = true
		}
	}
	assert.True(t, sawNick, "unrestricted limits must let NICK through")
	assert.True(t, sawJoin, "unrestricted limits must let JOIN through")
}
