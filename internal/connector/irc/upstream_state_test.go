package irc_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/connector/irc"
)

func TestUpstreamStateClassifications(t *testing.T) {
	t.Parallel()
	t.Run("recoverable bucket", func(t *testing.T) {
		assert.True(t, irc.UpstreamStateDisconnectedTransient.IsRecoverable())
		assert.True(t, irc.UpstreamStateDisconnectedNickUnavailable.IsRecoverable())
		assert.False(t, irc.UpstreamStateRegistered.IsRecoverable())
		assert.False(t, irc.UpstreamStateIdle.IsRecoverable())
	})
	t.Run("terminal bucket", func(t *testing.T) {
		assert.True(t, irc.UpstreamStateDisconnectedAuthFailed.IsTerminal())
		assert.True(t, irc.UpstreamStateDisconnectedBanned.IsTerminal())
		assert.True(t, irc.UpstreamStatePausedIdle.IsTerminal())
		assert.True(t, irc.UpstreamStateStopped.IsTerminal())
		assert.False(t, irc.UpstreamStateDisconnectedTransient.IsTerminal())
		assert.False(t, irc.UpstreamStateRegistered.IsTerminal())
	})
}

// TestClassifyNumericTable covers every numeric the plan's failure-mode
// catalog calls out as classification-relevant. Anything outside this
// set must return ok=false so the supervisor keeps its current state.
func TestClassifyNumericTable(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		numeric string
		want    irc.UpstreamState
		ok      bool
	}{
		{"433 ERR_NICKNAMEINUSE", irc.ErrNickNameInUse, irc.UpstreamStateDisconnectedNickUnavailable, true},
		{"432 ERR_ERRONEUSNICKNAME", irc.ErrErroneusNickname, irc.UpstreamStateDisconnectedNickInvalid, true},
		{"437 ERR_UNAVAILRESOURCE", irc.ErrUnavailResource, irc.UpstreamStateDisconnectedNickInvalid, true},
		{"464 ERR_PASSWDMISMATCH", irc.ErrPasswdMismatch, irc.UpstreamStateDisconnectedAuthFailed, true},
		{"904 ERR_SASLFAIL", irc.ErrSaslFail, irc.UpstreamStateDisconnectedAuthFailed, true},
		{"905 ERR_SASLTOOLONG", irc.ErrSaslTooLong, irc.UpstreamStateDisconnectedAuthFailed, true},
		{"465 ERR_YOUREBANNEDCREEP", irc.ErrYoureBannedCreep, irc.UpstreamStateDisconnectedBanned, true},
		{"001 RPL_WELCOME (irrelevant)", irc.RplWelcome, "", false},
		{"empty string", "", "", false},
		{"unknown numeric", "999", "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := irc.ClassifyNumeric(tc.numeric, nil, "")
			assert.Equal(t, tc.ok, ok)
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestClassifyErrorMapsTransportErrors verifies every transport-level
// error shape the connector can plausibly see during a read/write maps
// to a transient state — the auth/ban states are only reachable via
// IRC numerics or ERROR preambles, never raw Go errors.
func TestClassifyErrorMapsTransportErrors(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name string
		err  error
		want irc.UpstreamState
		ok   bool
	}{
		{"nil", nil, "", false},
		{"io.EOF", io.EOF, irc.UpstreamStateDisconnectedTransient, true},
		{"io.ErrUnexpectedEOF", io.ErrUnexpectedEOF, irc.UpstreamStateDisconnectedTransient, true},
		{"syscall.EPIPE", syscall.EPIPE, irc.UpstreamStateDisconnectedTransient, true},
		{"syscall.ECONNRESET", syscall.ECONNRESET, irc.UpstreamStateDisconnectedTransient, true},
		{"syscall.ECONNREFUSED", syscall.ECONNREFUSED, irc.UpstreamStateDisconnectedTransient, true},
		{"net.ErrClosed", net.ErrClosed, irc.UpstreamStateDisconnectedTransient, true},
		{"net.OpError dial connection refused", &net.OpError{
			Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED,
		}, irc.UpstreamStateDisconnectedTransient, true},
		{"wrapped EOF", fmt.Errorf("read upstream: %w", io.EOF), irc.UpstreamStateDisconnectedTransient, true},
		{"unknown error", errors.New("???"), irc.UpstreamStateDisconnectedTransient, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, ok := irc.ClassifyError(tc.err)
			assert.Equal(t, tc.ok, ok)
			assert.Equal(t, tc.want, got)
		})
	}
}

// TestClassifyDisconnectMessageBanPatterns exercises the K/G/Z-line +
// related ban-string set. Innocuous reasons (Excess Flood, Ping timeout)
// must classify as transient, not banned.
func TestClassifyDisconnectMessageBanPatterns(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name   string
		line   string
		state  irc.UpstreamState
		reason string
		ok     bool
	}{
		{
			name:   "K-Lined",
			line:   "ERROR :Closing Link: stefan[1.2.3.4] (K-Lined: spam)",
			state:  irc.UpstreamStateDisconnectedBanned,
			reason: "K-Lined: spam",
			ok:     true,
		},
		{
			name:   "G-Lined uppercase reason",
			line:   "ERROR :Closing Link: nick[1.2.3.4] (G-Lined)",
			state:  irc.UpstreamStateDisconnectedBanned,
			reason: "G-Lined",
			ok:     true,
		},
		{
			name:   "Z-Lined alternative wording",
			line:   "ERROR :Closing Link: nick[1.2.3.4] (Z-Lined: globally banned)",
			state:  irc.UpstreamStateDisconnectedBanned,
			reason: "Z-Lined: globally banned",
			ok:     true,
		},
		{
			name:   "User has been banned",
			line:   "ERROR :Closing Link: nick (User has been banned from this server)",
			state:  irc.UpstreamStateDisconnectedBanned,
			reason: "User has been banned from this server",
			ok:     true,
		},
		{
			name:   "You are not welcome",
			line:   "ERROR :Closing Link: nick (You are not welcome on this network)",
			state:  irc.UpstreamStateDisconnectedBanned,
			reason: "You are not welcome on this network",
			ok:     true,
		},
		{
			name:   "Excess Flood is transient, not banned",
			line:   "ERROR :Closing Link: nick[1.2.3.4] (Excess Flood)",
			state:  irc.UpstreamStateDisconnectedTransient,
			reason: "Excess Flood",
			ok:     true,
		},
		{
			name:   "Ping timeout is transient",
			line:   "ERROR :Closing Link: nick (Ping timeout: 240 seconds)",
			state:  irc.UpstreamStateDisconnectedTransient,
			reason: "Ping timeout: 240 seconds",
			ok:     true,
		},
		{
			name:   "Connection reset is transient",
			line:   "ERROR :Closing Link: nick (Connection reset by peer)",
			state:  irc.UpstreamStateDisconnectedTransient,
			reason: "Connection reset by peer",
			ok:     true,
		},
		{
			name:   "ERROR with no parenthetical",
			line:   "ERROR :Closing Link",
			state:  irc.UpstreamStateDisconnectedTransient,
			reason: "Closing Link",
			ok:     true,
		},
		{
			name: "non-ERROR line",
			line: ":server 001 nick :Welcome",
			ok:   false,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			state, reason, ok := irc.ClassifyDisconnectMessage(tc.line)
			assert.Equal(t, tc.ok, ok)
			if !ok {
				return
			}
			assert.Equal(t, tc.state, state)
			assert.Equal(t, tc.reason, reason)
		})
	}
}

func TestUpstreamStateMachineStartsIdle(t *testing.T) {
	t.Parallel()
	m := irc.NewUpstreamStateMachine(nil)
	assert.Equal(t, irc.UpstreamStateIdle, m.State())
	assert.Empty(t, m.ServerReason())
}

func TestUpstreamStateMachineTransitionFiresSubscriber(t *testing.T) {
	t.Parallel()
	m := irc.NewUpstreamStateMachine(nil)

	var mu sync.Mutex
	var got []irc.UpstreamStateChange
	sub := m.Subscribe(func(c irc.UpstreamStateChange) {
		mu.Lock()
		defer mu.Unlock()
		got = append(got, c)
	})
	t.Cleanup(sub.Unsubscribe)

	m.Transition(irc.UpstreamStateConnecting)
	m.Transition(irc.UpstreamStateRegistering)
	m.Transition(irc.UpstreamStateRegistered)
	m.Transition(irc.UpstreamStateDisconnectedTransient,
		irc.WithServerReason("Ping timeout: 240 seconds"))

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, got, 4)
	assert.Equal(t, irc.UpstreamStateIdle, got[0].From)
	assert.Equal(t, irc.UpstreamStateConnecting, got[0].To)
	assert.Equal(t, irc.UpstreamStateRegistered, got[2].To)
	assert.Equal(t, irc.UpstreamStateDisconnectedTransient, got[3].To)
	assert.Equal(t, "Ping timeout: 240 seconds", got[3].ServerReason)
}

func TestUpstreamStateMachineTransitionIdempotent(t *testing.T) {
	t.Parallel()
	m := irc.NewUpstreamStateMachine(nil)
	m.Transition(irc.UpstreamStateConnecting)

	var calls int32
	sub := m.Subscribe(func(irc.UpstreamStateChange) {
		calls++
	})
	t.Cleanup(sub.Unsubscribe)

	// Same-state transition is a no-op — repeated "still transient"
	// would otherwise spam any consumer that fans out a NOTICE per
	// transition.
	m.Transition(irc.UpstreamStateConnecting)
	m.Transition(irc.UpstreamStateConnecting)
	assert.Equal(t, int32(0), calls)

	m.Transition(irc.UpstreamStateRegistering)
	assert.Equal(t, int32(1), calls)
}

func TestUpstreamStateMachineUnsubscribeStopsDelivery(t *testing.T) {
	t.Parallel()
	m := irc.NewUpstreamStateMachine(nil)
	var calls int32
	sub := m.Subscribe(func(irc.UpstreamStateChange) { calls++ })

	m.Transition(irc.UpstreamStateConnecting)
	sub.Unsubscribe()
	m.Transition(irc.UpstreamStateRegistering)

	assert.Equal(t, int32(1), calls,
		"Unsubscribe must remove the handler before the next Transition fires")
}

func TestUpstreamStateMachineDurationIn(t *testing.T) {
	t.Parallel()
	m := irc.NewUpstreamStateMachine(nil)
	m.Transition(irc.UpstreamStateConnecting)
	time.Sleep(20 * time.Millisecond)
	d := m.DurationIn()
	assert.GreaterOrEqual(t, d, 20*time.Millisecond)
	assert.Less(t, d, 5*time.Second)
}

func TestUpstreamStateMachineEnteredAtMovesOnTransition(t *testing.T) {
	t.Parallel()
	m := irc.NewUpstreamStateMachine(nil)
	first := m.EnteredAt()
	require.False(t, first.IsZero(), "EnteredAt is set at construction")

	time.Sleep(10 * time.Millisecond)
	m.Transition(irc.UpstreamStateConnecting)
	second := m.EnteredAt()
	assert.True(t, second.After(first),
		"EnteredAt must advance on a real transition")
}

func TestUpstreamStateMachineSubscribeNilFnReturnsInertHandle(t *testing.T) {
	t.Parallel()
	m := irc.NewUpstreamStateMachine(nil)
	// nil subscriber must return a Subscription whose Unsubscribe is a
	// no-op — callers may pass an optional handler from configuration.
	sub := m.Subscribe(nil)
	assert.NotPanics(t, func() { sub.Unsubscribe() })
}

func TestUpstreamSubscriptionUnsubscribeOnZeroValueIsNoOp(t *testing.T) {
	t.Parallel()
	var sub *irc.Subscription
	assert.NotPanics(t, func() { sub.Unsubscribe() })

	empty := &irc.Subscription{}
	assert.NotPanics(t, func() { empty.Unsubscribe() })
}

func TestDescribeUpstreamStateCoversEveryArm(t *testing.T) {
	t.Parallel()
	cases := []struct {
		state    irc.UpstreamState
		network  string
		reason   string
		contains []string
	}{
		{irc.UpstreamStateRegistered, "Libera", "", nil},
		{irc.UpstreamStateIdle, "Libera", "", []string{"not yet started"}},
		{irc.UpstreamStateConnecting, "Libera", "", []string{"Currently connecting", "Libera"}},
		{irc.UpstreamStateRegistering, "Libera", "", []string{"Currently connecting"}},
		{irc.UpstreamStateDisconnectedTransient, "Libera", "EOF",
			[]string{"Currently disconnected", "EOF", "NOT be delivered"}},
		{irc.UpstreamStateDisconnectedNickUnavailable, "Libera", "",
			[]string{"Nickname taken", "Libera", "reclaiming"}},
		{irc.UpstreamStateDisconnectedNickInvalid, "Libera", "Erroneous Nickname",
			[]string{"can't be used", "Libera", "Erroneous Nickname", "Pick a different"}},
		{irc.UpstreamStateDisconnectedAuthFailed, "Libera", "bad password",
			[]string{"Authentication failed", "bad password", "update credentials"}},
		{irc.UpstreamStateDisconnectedBanned, "Libera", "K-Lined: spam",
			[]string{"Banned from", "K-Lined: spam", "manual intervention"}},
		{irc.UpstreamStatePausedIdle, "Libera", "",
			[]string{"paused", "Restart"}},
		{irc.UpstreamStateStopped, "Libera", "",
			[]string{"Connector stopped"}},
		{"unknown_state", "Libera", "", nil},
		// Empty network name must fall back to a generic placeholder.
		{irc.UpstreamStateDisconnectedTransient, "", "",
			[]string{"the network"}},
	}
	for _, tc := range cases {
		t.Run(string(tc.state)+"_"+tc.network, func(t *testing.T) {
			body := irc.DescribeUpstreamState(tc.state, tc.network, tc.reason)
			if len(tc.contains) == 0 {
				assert.Empty(t, body)
				return
			}
			for _, want := range tc.contains {
				assert.Contains(t, body, want)
			}
		})
	}
}

func TestSeverityForUpstreamStateCoversEveryArm(t *testing.T) {
	t.Parallel()
	cases := []struct {
		state irc.UpstreamState
		want  string
	}{
		{irc.UpstreamStateRegistered, "info"},
		{irc.UpstreamStateConnecting, "warning"},
		{irc.UpstreamStateRegistering, "warning"},
		{irc.UpstreamStateDisconnectedTransient, "warning"},
		{irc.UpstreamStateDisconnectedNickUnavailable, "warning"},
		{irc.UpstreamStateDisconnectedAuthFailed, "error"},
		{irc.UpstreamStateDisconnectedBanned, "error"},
		{irc.UpstreamStatePausedIdle, "error"},
		{irc.UpstreamStateStopped, "error"},
		{irc.UpstreamStateIdle, ""}, // no banner severity assigned
		{"unknown_state", ""},
	}
	for _, tc := range cases {
		t.Run(string(tc.state), func(t *testing.T) {
			assert.Equal(t, tc.want, irc.SeverityForUpstreamState(tc.state))
		})
	}
}

func TestParseERRORLineEmptyTrailingReturnsEmptyOk(t *testing.T) {
	t.Parallel()
	// Server may send a bare `ERROR` with no body — protocol allows
	// it. The parser must surface ok=true with an empty reason rather
	// than treating it as a non-ERROR line.
	reason, ok := irc.ParseERRORLine("ERROR")
	assert.True(t, ok)
	assert.Empty(t, reason)
}

func TestClassifyBanReasonEmptyIsNotBanned(t *testing.T) {
	t.Parallel()
	state, ok := irc.ClassifyBanReason("")
	assert.False(t, ok)
	assert.Empty(t, state)
}

func TestUpstreamStateMachineConcurrentTransitionsAreSafe(t *testing.T) {
	t.Parallel()
	// Use a discard logger — the stress test produces hundreds of
	// transitions and would otherwise dominate the test output stream.
	m := irc.NewUpstreamStateMachine(slog.New(slog.NewTextHandler(io.Discard, nil)))

	var wg sync.WaitGroup
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	states := []irc.UpstreamState{
		irc.UpstreamStateConnecting,
		irc.UpstreamStateRegistering,
		irc.UpstreamStateRegistered,
		irc.UpstreamStateDisconnectedTransient,
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				m.Transition(states[i%len(states)])
			}
		}()
	}
	wg.Wait()
	// Just want the race detector to confirm no concurrent map writes.
	assert.Contains(t, states, m.State())
}
