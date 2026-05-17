package irc_test

import (
	"bufio"
	"context"
	"errors"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/agent"
	"github.com/turborg/turborg/internal/connector/irc"
)

// TestBouncerNotifyJoinFailureDropsFromWantedAndBroadcasts covers
// Edge 1 at the bouncer level: a per-channel rejoin-failure NOTICE
// fans to every joined channel × every attached client buffer with
// the server-supplied reason. The wanted-set drop happens in the
// connector's handler (covered separately end-to-end) — here we
// verify the NOTICE shape on its own.
func TestBouncerNotifyJoinFailureNotices(t *testing.T) {
	b, addr, machine := freshBouncerWithState(t, "Libera Chat")
	machine.Transition(irc.UpstreamStateRegistered)

	conn, r := authBouncerClient(t, addr)
	b.NotifyJoinFailure("#archlinux", "banned from channel")

	notice := readUntilContains(r, conn, "Could not rejoin", time.Second)
	require.NotEmpty(t, notice)
	assert.Equal(t, "#archlinux", noticeTarget(notice),
		"join-failure NOTICE must target the channel that failed to rejoin")
	assert.Contains(t, notice, "banned from channel",
		"server-supplied reason must thread into the body")
	assert.Contains(t, notice, "/join #archlinux",
		"body must tell the user how to retry")
}

// TestSupervisorObservesRejoinFailuresAndDropsFromWanted is the end-
// to-end half of Edge 1: the supervisor reconnects, the server replies
// to JOIN with 474 ERR_BANNEDFROMCHAN for one channel and accepts
// another, and the failed channel ends up dropped from the wanted-set
// while the accepted one stays.
func TestSupervisorObservesRejoinFailuresAndDropsFromWanted(t *testing.T) {
	fs := newRejectingTestServer(t, map[string]string{
		"#bannedchan": "474",
	})

	conn := irc.New(&irc.Settings{
		Hostname: "127.0.0.1",
		Port:     fs.Port(),
		Nick:     "turborg",
		Channels: []string{"#ok", "#bannedchan"},
	}, nil, nil)

	a := agent.New(nil)
	a.AddConnector(conn)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	require.Eventually(t, func() bool {
		_, banned := conn.WantedChannels().Get("#bannedchan")
		_, ok := conn.WantedChannels().Get("#ok")
		return ok && !banned
	}, 2*time.Second, 20*time.Millisecond,
		"after 474, #bannedchan must be dropped from wanted-set; #ok must survive")

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("agent did not shut down")
	}
}

// TestJoinFailureEventPublished verifies the EventJoinFailed signal
// (consumed by the web gateway) carries channel + code + reason.
func TestJoinFailureEventPublished(t *testing.T) {
	fs := newRejectingTestServer(t, map[string]string{
		"#keyed": "475",
	})

	bus := agent.NewEventBus(nil)
	var (
		mu     sync.Mutex
		events []agent.Event
	)
	bus.Subscribe(agent.EventJoinFailed, func(_ context.Context, ev *agent.Event) {
		mu.Lock()
		defer mu.Unlock()
		events = append(events, *ev)
	})

	conn := irc.New(&irc.Settings{
		Hostname: "127.0.0.1",
		Port:     fs.Port(),
		Nick:     "turborg",
		Channels: []string{"#keyed"},
	}, nil, bus)

	a := agent.New(nil)
	a.AddConnector(conn)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(events) >= 1
	}, 2*time.Second, 20*time.Millisecond)

	mu.Lock()
	got := events[0]
	mu.Unlock()
	assert.Equal(t, "#keyed", got.Fields["channel"])
	assert.Equal(t, "475", got.Fields["code"])
	assert.NotEmpty(t, got.Fields["reason"])

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("agent did not shut down")
	}
}

// TestBouncerPostSendErrorRejectsToClient covers Edge 2: when the
// state machine still reads `registered` but the upstream write fails
// (race window — upstream just died, machine hasn't caught up),
// the failing PRIVMSG must produce a channel-targeted "NOT sent"
// NOTICE rather than silent-dropping.
func TestBouncerPostSendErrorRejectsToClient(t *testing.T) {
	b, addr, machine := freshBouncerWithState(t, "Libera Chat", "#archlinux")
	machine.Transition(irc.UpstreamStateRegistered)
	// Wire AttachUpstream with a callback that always fails — every
	// forwarded line trips the post-send error path. The state stays
	// at `registered` so the gate passes; the failure surfaces from
	// the write side only.
	b.AttachUpstream(func(string) error {
		return errors.New("simulated upstream write failure")
	})

	conn, r := authBouncerClient(t, addr)
	writeLine(t, conn, "PRIVMSG #archlinux :hello")
	notice := readUntilContains(r, conn, "NOT sent", time.Second)
	require.NotEmpty(t, notice,
		"upstream write failure mid-burst must produce a per-message NOTICE")
	assert.Equal(t, "#archlinux", noticeTarget(notice),
		"the failing PRIVMSG's NOTICE must target its original channel")
}

// TestBouncerMultiClientBroadcastReachesEveryAttachee covers Edge 5:
// a single state-change broadcast must fan to every attached client,
// not just the most recent one. Two simultaneously-attached clients
// both see the per-channel NOTICE when upstream transitions to
// transient.
func TestBouncerMultiClientBroadcastReachesEveryAttachee(t *testing.T) {
	_, addr, machine := freshBouncerWithState(t, "Libera Chat", "#archlinux")
	machine.Transition(irc.UpstreamStateRegistered)

	c1, r1 := authBouncerClient(t, addr)
	c2, r2 := authBouncerClient(t, addr)

	// Drain everything the welcome flow buffered up to each client
	// BEFORE flipping state. sendWelcome runs async on the bouncer's
	// per-client handleClient goroutine; without an explicit drain
	// surfaceStateToClient could race the upcoming Transition and
	// fire its own NOTICE * with "Currently disconnected" body,
	// which the readUntilContains scan below would then pick up
	// instead of the broadcast NOTICE this test cares about.
	drainBuffered(t, c1, r1)
	drainBuffered(t, c2, r2)

	machine.Transition(irc.UpstreamStateDisconnectedTransient,
		irc.WithServerReason("EOF"))

	n1 := readUntilContains(r1, c1, "Currently disconnected", time.Second)
	n2 := readUntilContains(r2, c2, "Currently disconnected", time.Second)
	require.NotEmpty(t, n1, "client 1 must receive the broadcast")
	require.NotEmpty(t, n2, "client 2 must receive the broadcast")
	assert.Equal(t, "#archlinux", noticeTarget(n1))
	assert.Equal(t, "#archlinux", noticeTarget(n2))
}

// drainBuffered consumes everything currently readable on the
// connection within a short deadline. Used by tests that need
// sendWelcome (which the bouncer runs in a goroutine after auth) to
// have fully completed before they assert about subsequent lines.
func drainBuffered(t *testing.T, conn net.Conn, r *bufio.Reader) {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
	for {
		if _, err := r.ReadString('\n'); err != nil {
			return
		}
	}
}

// newRejectingTestServer extends reconnectTestServer with a map of
// channels that get rejected on JOIN. Used by Edge 1 integration
// tests to drive specific failure numerics through the connector's
// dispatchLine handler.
type rejectingTestServer struct {
	*reconnectTestServer
	rejectChannels map[string]string // channel → numeric code
}

func newRejectingTestServer(t *testing.T, rejects map[string]string) *rejectingTestServer {
	t.Helper()
	inner := newReconnectTestServer(t)
	s := &rejectingTestServer{reconnectTestServer: inner, rejectChannels: rejects}
	inner.joinHook = s.handleJoinWithRejection
	return s
}

func (s *rejectingTestServer) handleJoinWithRejection(conn writerLike, line string) bool {
	target := strings.TrimSpace(strings.TrimPrefix(line, "JOIN "))
	if idx := strings.Index(target, " "); idx >= 0 {
		target = target[:idx]
	}
	if code, ok := s.rejectChannels[target]; ok {
		nick := s.firstNickForTest()
		_, _ = conn.Write([]byte(":fake " + code + " " + nick + " " + target +
			" :Channel rejected by network\r\n"))
		return true // handled (don't echo a fake self-JOIN)
	}
	return false // not in the reject set — fall through to the default echo
}
