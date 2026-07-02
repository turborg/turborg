package irc_test

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/agent"
	"github.com/turborg/turborg/internal/connector/irc"
	"github.com/turborg/turborg/tests/fixtures/fakeirc"
)

// TestPongTimeoutTransitionsTransient: fake server completes the
// handshake but silently swallows every PING. Within
// ~ClientPingInterval + PongTimeout the connector must classify the
// upstream as silently dead and emit the transient transition. The
// ReadIdleTimeout is held large enough that it cannot be the cause.
func TestPongTimeoutTransitionsTransient(t *testing.T) {
	fs := fakeirc.New(t) // no WithPongResponses → server drops PINGs
	defer fs.Close()

	conn := irc.New(&irc.Settings{
		Hostname:           "127.0.0.1",
		Port:               fs.Port(),
		Nick:               "turborg",
		Channels:           []string{"#test"},
		ClientPingInterval: 80 * time.Millisecond,
		PongTimeout:        40 * time.Millisecond,
		ReadIdleTimeout:    10 * time.Second,
	}, nil, nil)

	transients := make(chan irc.UpstreamStateChange, 8)
	sub := conn.UpstreamState().Subscribe(func(c irc.UpstreamStateChange) {
		if c.To == irc.UpstreamStateDisconnectedTransient {
			select {
			case transients <- c:
			default:
			}
		}
	})
	defer sub.Unsubscribe()

	a := agent.New(nil)
	a.AddConnector(conn)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	// Worst case: one PING cadence (80ms) + PongTimeout (40ms) + a watchdog
	// tick (≤ 20ms). 1s is generous and well under ReadIdleTimeout.
	select {
	case ch := <-transients:
		assert.Contains(t, ch.ServerReason, "no PONG",
			"transition reason must surface the active-probe cause so attached clients see it via DescribeUpstreamState")
	case <-time.After(2 * time.Second):
		t.Fatal("pong-watchdog did not transition to disconnected_transient within 2s")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("agent did not shut down within 2s of ctx cancel")
	}
}

// TestPongAckResetsLedger: server echoes PONGs back so every PING is
// matched within the timeout window. Assert the connector stays
// Registered (no transient transition emitted).
//
// PongTimeout is held well above the worst-case localhost PONG
// round-trip under a loaded -race runner (goroutine wakeups + GC pauses
// can add tens of ms), so a healthy session never trips a false
// transient. The soak window is kept longer than PongTimeout so the
// test stays non-vacuous: had acks been broken, the first unacked PING
// would trip the watchdog (~ClientPingInterval + PongTimeout) well
// inside the window.
func TestPongAckResetsLedger(t *testing.T) {
	fs := fakeirc.New(t, fakeirc.WithPongResponses(true))
	defer fs.Close()

	conn := irc.New(&irc.Settings{
		Hostname:           "127.0.0.1",
		Port:               fs.Port(),
		Nick:               "turborg",
		Channels:           []string{"#test"},
		ClientPingInterval: 50 * time.Millisecond,
		PongTimeout:        200 * time.Millisecond,
		ReadIdleTimeout:    10 * time.Second,
	}, nil, nil)

	var (
		mu             sync.Mutex
		sawTransient   bool
		registeredOnce bool
	)
	sub := conn.UpstreamState().Subscribe(func(c irc.UpstreamStateChange) {
		mu.Lock()
		defer mu.Unlock()
		switch c.To {
		case irc.UpstreamStateRegistered:
			registeredOnce = true
		case irc.UpstreamStateDisconnectedTransient:
			sawTransient = true
		}
	})
	defer sub.Unsubscribe()

	a := agent.New(nil)
	a.AddConnector(conn)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	// Hold the session for several ping cycles, longer than PongTimeout so
	// a broken-ack regression would surface a transient inside the window.
	// Acks should keep the ledger drained continuously — no transition.
	time.Sleep(700 * time.Millisecond)

	mu.Lock()
	gotRegistered := registeredOnce
	gotTransient := sawTransient
	mu.Unlock()

	assert.True(t, gotRegistered, "handshake should have reached registered before the soak window")
	assert.False(t, gotTransient,
		"server PONGed every PING — watchdog must NOT transition to transient")

	// Sanity: server actually saw PINGs go by. Without this, a logic bug
	// that suppresses PINGs entirely would make the test pass vacuously.
	sawPing := false
	for _, line := range fs.Received() {
		if strings.HasPrefix(line, "PING ") {
			sawPing = true
			break
		}
	}
	assert.True(t, sawPing, "expected at least one PING in fs.Received() during soak window")

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("agent did not shut down within 2s of ctx cancel")
	}
}

// TestPongTimeoutDisabled: PongTimeout=0 disables the active probe. The
// server drops PINGs silently, but the watchdog must not fire — only
// the existing ReadIdleTimeout backstop should surface the dead socket.
// Asserts that the read-idle path is still the one to fire (the
// connector did NOT regress on the legacy detector).
func TestPongTimeoutDisabled(t *testing.T) {
	fs := fakeirc.New(t) // no PONG handling
	defer fs.Close()

	conn := irc.New(&irc.Settings{
		Hostname:           "127.0.0.1",
		Port:               fs.Port(),
		Nick:               "turborg",
		Channels:           []string{"#test"},
		ClientPingInterval: 30 * time.Millisecond,
		PongTimeout:        0,                      // disabled
		ReadIdleTimeout:    150 * time.Millisecond, // legacy backstop
	}, nil, nil)

	transients := make(chan irc.UpstreamStateChange, 8)
	sub := conn.UpstreamState().Subscribe(func(c irc.UpstreamStateChange) {
		if c.To == irc.UpstreamStateDisconnectedTransient {
			select {
			case transients <- c:
			default:
			}
		}
	})
	defer sub.Unsubscribe()

	a := agent.New(nil)
	a.AddConnector(conn)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	// Read-idle must surface within a couple of cycles. The reason
	// string must NOT mention "no PONG" (that's the active probe).
	select {
	case ch := <-transients:
		assert.NotContains(t, ch.ServerReason, "no PONG",
			"with PongTimeout=0 the active probe is disabled — the transition reason must come from the read-idle path, not the watchdog")
	case <-time.After(2 * time.Second):
		t.Fatal("ReadIdleTimeout did not surface a transient transition within 2s — the legacy backstop must still work")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("agent did not shut down within 2s of ctx cancel")
	}
}

// TestPongTimeoutSurfacesServerReason: when the watchdog fires, the
// transition's ServerReason flows through DescribeUpstreamState so
// attached clients see the cause in their NOTICE body / event payload.
// PR #17 wires the bouncer to call DescribeUpstreamState on each
// transition; this test asserts the wiring stays intact across the new
// transient cause.
func TestPongTimeoutSurfacesServerReason(t *testing.T) {
	fs := fakeirc.New(t) // silent server
	defer fs.Close()

	conn := irc.New(&irc.Settings{
		Hostname:           "127.0.0.1",
		Port:               fs.Port(),
		Nick:               "turborg",
		Channels:           []string{"#test"},
		ClientPingInterval: 50 * time.Millisecond,
		PongTimeout:        30 * time.Millisecond,
		ReadIdleTimeout:    10 * time.Second,
	}, nil, nil)

	reasons := make(chan string, 8)
	sub := conn.UpstreamState().Subscribe(func(c irc.UpstreamStateChange) {
		if c.To == irc.UpstreamStateDisconnectedTransient {
			select {
			case reasons <- c.ServerReason:
			default:
			}
		}
	})
	defer sub.Unsubscribe()

	a := agent.New(nil)
	a.AddConnector(conn)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	var captured string
	select {
	case captured = <-reasons:
	case <-time.After(2 * time.Second):
		t.Fatal("pong-watchdog did not transition within 2s")
	}

	require.Contains(t, captured, "no PONG")
	described := irc.DescribeUpstreamState(irc.UpstreamStateDisconnectedTransient, "127.0.0.1", captured)
	assert.Contains(t, described, "no PONG",
		"DescribeUpstreamState must thread the watchdog reason into the operator-visible body")
	assert.Contains(t, described, "Currently disconnected",
		"description must keep the existing transient framing operators rely on")

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("agent did not shut down within 2s of ctx cancel")
	}
}
