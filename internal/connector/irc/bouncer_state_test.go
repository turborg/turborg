package irc_test

import (
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/connector/irc"
)

// freshBouncerWithState wires a bouncer + state machine + joined
// channels in one call so the state-machine tests aren't boilerplate-
// heavy. Returns the bouncer, its listen addr, and the machine.
func freshBouncerWithState(t *testing.T, networkName string, joined ...string) (*irc.Bouncer, string, *irc.UpstreamStateMachine) {
	t.Helper()
	machine := irc.NewUpstreamStateMachine(nil)
	state := irc.NewChannelState()
	for _, ch := range joined {
		state.OnSelfJoin(ch)
	}
	b, addr := freshBouncer(t, "hunter2", func(b *irc.Bouncer) {
		b.AttachState(state, "turborg", "ident", "host")
		b.AttachUpstreamState(machine, networkName)
	})
	trackForwarded(b)
	return b, addr, machine
}

// TestBouncerSurfacesStateOnAttachWhenDetached covers the freshly-
// attaching-client path: client connects during a transient outage,
// authenticates, and must see a state-informative NOTICE explaining
// why no JOIN replay follows. Without this the client sees a clean
// 001 welcome + nothing else, indistinguishable from "bot is dead".
func TestBouncerSurfacesStateOnAttachWhenDetached(t *testing.T) {
	_, addr, machine := freshBouncerWithState(t, "Libera Chat")
	machine.Transition(irc.UpstreamStateDisconnectedTransient,
		irc.WithServerReason("Ping timeout: 240 seconds"))

	conn, r := authBouncerClient(t, addr)
	_ = conn
	notice := readUntilContains(r, conn, "Currently disconnected", time.Second)
	require.NotEmpty(t, notice, "client must receive a state-surfacing NOTICE on attach")
	assert.Contains(t, notice, "Libera Chat",
		"network name from AttachUpstreamState must thread into the body")
	assert.Contains(t, notice, "Ping timeout: 240 seconds",
		"server-supplied disconnect reason must appear in the surfaced body")
}

// TestBouncerSurfacesStateOnAttachWhenBanned covers the terminal-
// outage case — banned state needs a different body that signals
// "automatic reconnect stopped" rather than "reconnecting".
func TestBouncerSurfacesStateOnAttachWhenBanned(t *testing.T) {
	_, addr, machine := freshBouncerWithState(t, "Libera Chat")
	machine.Transition(irc.UpstreamStateDisconnectedBanned,
		irc.WithServerReason("K-Lined: spam"))

	conn, r := authBouncerClient(t, addr)
	_ = conn
	notice := readUntilContains(r, conn, "Banned from", time.Second)
	require.NotEmpty(t, notice)
	assert.Contains(t, notice, "Automatic reconnect stopped")
	assert.Contains(t, notice, "K-Lined")
}

// TestBouncerSkipsStateSurfacingWhenRegistered confirms the happy
// path: client attaches during normal operation, the JOIN replay
// happens, and no extra state-surfacing NOTICE is sent. Avoids
// surprising attached clients with "currently registered" chatter
// they don't need.
func TestBouncerSkipsStateSurfacingWhenRegistered(t *testing.T) {
	_, addr, machine := freshBouncerWithState(t, "Libera Chat", "#a")
	machine.Transition(irc.UpstreamStateRegistered)

	conn, r := authBouncerClient(t, addr)
	_ = conn
	// Drain everything available within a short window and confirm
	// no "Currently disconnected" / "Connecting to" surfaces.
	conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	var buf strings.Builder
	for {
		line, err := r.ReadString('\n')
		buf.WriteString(line)
		if err != nil {
			break
		}
	}
	got := buf.String()
	assert.NotContains(t, got, "Currently disconnected")
	assert.NotContains(t, got, "Currently connecting")
	assert.NotContains(t, got, "Banned from")
	// JOIN replay should still have fired.
	assert.Contains(t, got, "JOIN #a")
}

// TestBouncerRejectsPrivmsgDuringTransient covers the central
// silent-message-loss fix: client typing during a transient upstream
// outage must produce a channel-targeted NOTICE in the buffer they
// typed in, with explicit "NOT sent" wording.
func TestBouncerRejectsPrivmsgDuringTransient(t *testing.T) {
	b, addr, machine := freshBouncerWithState(t, "Libera Chat", "#archlinux")
	machine.Transition(irc.UpstreamStateRegistered)

	forwarded, mu := trackForwarded(b)

	conn, r := authBouncerClient(t, addr)
	// Drop upstream out from under the attached client.
	machine.Transition(irc.UpstreamStateDisconnectedTransient,
		irc.WithServerReason("connection reset by peer"))
	// Drain the broadcast NOTICE the transition fires.
	_ = readUntilContains(r, conn, "Currently disconnected", time.Second)

	writeLine(t, conn, "PRIVMSG #archlinux :hello")
	notice := readUntilContains(r, conn, "NOT sent", time.Second)
	require.NotEmpty(t, notice, "PRIVMSG during transient must produce a NOT-sent NOTICE")
	assert.Equal(t, "#archlinux", noticeTarget(notice),
		"detached-state PRIVMSG rejection must target the originating channel")

	mu.Lock()
	defer mu.Unlock()
	for _, l := range *forwarded {
		if strings.Contains(l, "PRIVMSG #archlinux") {
			t.Fatalf("PRIVMSG must NOT reach upstream during transient: %s", l)
		}
	}
}

// TestBouncerRejectsPrivmsgDuringBannedSaysSo asserts that the
// terminal-state reject body differs from the transient one — the
// user needs to know "this isn't going to recover on its own" rather
// than the generic "reconnecting" hint.
func TestBouncerRejectsPrivmsgDuringBannedSaysSo(t *testing.T) {
	_, addr, machine := freshBouncerWithState(t, "Libera Chat", "#a")
	machine.Transition(irc.UpstreamStateRegistered)

	conn, r := authBouncerClient(t, addr)
	machine.Transition(irc.UpstreamStateDisconnectedBanned,
		irc.WithServerReason("K-Lined: spam"))
	_ = readUntilContains(r, conn, "Banned from", time.Second)

	writeLine(t, conn, "PRIVMSG #a :hi")
	notice := readUntilContains(r, conn, "NOT sent", time.Second)
	require.NotEmpty(t, notice)
	assert.Contains(t, notice, "Banned",
		"banned-state reject body must read differently from transient")
}

// TestBouncerBroadcastsOnTransition covers the in-flight-attached
// case: a client that was already attached when upstream dropped must
// see a per-channel NOTICE in every joined buffer the moment the
// state transitions.
func TestBouncerBroadcastsOnTransition(t *testing.T) {
	_, addr, machine := freshBouncerWithState(t, "Libera Chat", "#a", "#b")
	machine.Transition(irc.UpstreamStateRegistered)

	conn, r := authBouncerClient(t, addr)
	machine.Transition(irc.UpstreamStateDisconnectedTransient,
		irc.WithServerReason("EOF"))

	// Collect every NOTICE that arrives within a short window.
	conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	var sawA, sawB bool
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			break
		}
		if !strings.Contains(line, "Currently disconnected") {
			continue
		}
		switch noticeTarget(line) {
		case "#a":
			sawA = true
		case "#b":
			sawB = true
		}
	}
	assert.True(t, sawA, "#a buffer must receive the transition NOTICE")
	assert.True(t, sawB, "#b buffer must receive the transition NOTICE")
}

// TestBouncerStateBroadcastFallsBackToServiceWhenNoChannels covers
// the zero-channel case: an attached client in no channels still
// needs to learn about upstream state transitions (disconnect,
// reconnect, banned, paused). Without the *turborg service-buffer
// fallback the channel-broadcast loop is a silent no-op and the
// user misses the signal entirely.
func TestBouncerStateBroadcastFallsBackToServiceWhenNoChannels(t *testing.T) {
	_, addr, machine := freshBouncerWithState(t, "Libera Chat")
	machine.Transition(irc.UpstreamStateRegistered)

	conn, r := authBouncerClient(t, addr)
	// sendWelcome runs async on the bouncer's handleClient goroutine —
	// drain it before transitioning so surfaceStateToClient can't race
	// the upcoming Transition and inject a NOTICE * with the same
	// "Currently disconnected" body that would confuse the read scan.
	drainBuffered(t, conn, r)
	machine.Transition(irc.UpstreamStateDisconnectedTransient,
		irc.WithServerReason("EOF"))

	// Service buffer must carry the state-change body even with zero
	// joined channels — that's the audit-and-fallback path.
	line := readUntilContains(r, conn, "Currently disconnected", time.Second)
	require.NotEmpty(t, line,
		"zero-channel state transition must reach the client via *turborg")
	assert.Equal(t, "turborg", servicePrivmsgTarget(line),
		"zero-channel broadcast routes through *turborg, not the channel loop")
}

// TestBouncerStateBroadcastAuditCopyAlongsideChannelBroadcast covers
// the audit-log copy: when channels ARE joined the broadcast goes to
// each, but the user also gets one *turborg copy so opening the
// service tab reads as a chronological log of transitions.
func TestBouncerStateBroadcastAuditCopyAlongsideChannelBroadcast(t *testing.T) {
	_, addr, machine := freshBouncerWithState(t, "Libera Chat", "#a")
	machine.Transition(irc.UpstreamStateRegistered)

	conn, r := authBouncerClient(t, addr)
	drainBuffered(t, conn, r)
	machine.Transition(irc.UpstreamStateDisconnectedTransient,
		irc.WithServerReason("EOF"))

	// Collect every line for a short window and confirm BOTH the
	// channel NOTICE and the *turborg PRIVMSG arrive.
	conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	var sawChannel, sawService bool
	for {
		l, err := r.ReadString('\n')
		if err != nil {
			break
		}
		if !strings.Contains(l, "Currently disconnected") {
			continue
		}
		if noticeTarget(l) == "#a" {
			sawChannel = true
		}
		if servicePrivmsgTarget(l) == "turborg" {
			sawService = true
		}
	}
	assert.True(t, sawChannel, "channel #a must receive the broadcast NOTICE")
	assert.True(t, sawService, "*turborg must receive the audit-log PRIVMSG copy")
}

// TestBouncerBroadcastsReconnectedOnReturnToRegistered checks the
// recovery message: when the supervisor brings upstream back online,
// every joined channel gets a "back live" broadcast so the user
// knows their next typed message will actually go through.
func TestBouncerBroadcastsReconnectedOnReturnToRegistered(t *testing.T) {
	_, addr, machine := freshBouncerWithState(t, "Libera Chat", "#a")
	machine.Transition(irc.UpstreamStateRegistered)

	conn, r := authBouncerClient(t, addr)
	machine.Transition(irc.UpstreamStateDisconnectedTransient,
		irc.WithServerReason("EOF"))
	_ = readUntilContains(r, conn, "Currently disconnected", time.Second)

	machine.Transition(irc.UpstreamStateRegistered)
	notice := readUntilContains(r, conn, "back live", time.Second)
	require.NotEmpty(t, notice, "transition back to registered must broadcast a recovery NOTICE")
	assert.Equal(t, "#a", noticeTarget(notice),
		"recovery NOTICE must target every joined channel")
}

// TestBouncerSkipsIntraDetachedTransitions confirms the no-noise rule:
// connecting → registering inside an outage must NOT fire another
// per-channel NOTICE, because the previous transition already told
// the user what was happening.
func TestBouncerSkipsIntraDetachedTransitions(t *testing.T) {
	_, addr, machine := freshBouncerWithState(t, "Libera Chat", "#a")
	machine.Transition(irc.UpstreamStateRegistered)

	conn, r := authBouncerClient(t, addr)
	machine.Transition(irc.UpstreamStateDisconnectedTransient,
		irc.WithServerReason("EOF"))
	_ = readUntilContains(r, conn, "Currently disconnected", time.Second)

	machine.Transition(irc.UpstreamStateConnecting)
	machine.Transition(irc.UpstreamStateRegistering)

	// Drain any further NOTICEs within a short window — there should
	// be none beyond the initial transient broadcast.
	conn.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			break
		}
		assert.False(t, strings.Contains(line, "Currently connecting"),
			"connecting/registering during an outage must NOT re-broadcast: %s", line)
	}
}

// Note: the warn-hook test lives in internal_test.go because the
// onUpstreamWarn method is unexported. See TestBouncerWarnHookBroadcastsStrongerNotice
// there.
