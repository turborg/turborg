package irc

import (
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"syscall"
	"time"
)

// UpstreamState is the connector's view of its upstream IRC session.
// String values are part of the WS gateway protocol — emitted as
// connector.state_changed.state — so renaming a value is a breaking
// change.
type UpstreamState string

const (
	// UpstreamStateIdle is the initial state, before the connector has
	// attempted its first dial.
	UpstreamStateIdle UpstreamState = "idle"

	// UpstreamStateConnecting covers the TCP/TLS handshake.
	UpstreamStateConnecting UpstreamState = "connecting"

	// UpstreamStateRegistering covers the IRCv3 CAP / SASL / USER / NICK
	// exchange after the socket opens, up to 001 RPL_WELCOME.
	UpstreamStateRegistering UpstreamState = "registering"

	// UpstreamStateRegistered is the live, ready-to-send state. PRIVMSG
	// traffic is only forwarded upstream in this state.
	UpstreamStateRegistered UpstreamState = "registered"

	// UpstreamStateDisconnectedTransient is any recoverable upstream
	// failure: TCP reset, PING timeout, conn refused, network unreachable,
	// TLS handshake failure, write error. The reconnect supervisor retries
	// with backoff.
	UpstreamStateDisconnectedTransient UpstreamState = "disconnected_transient"

	// UpstreamStateDisconnectedNickUnavailable is 433 ERR_NICKNAMEINUSE
	// or 437 ERR_UNAVAILRESOURCE during registration. Recoverable —
	// supervisor retries with backoff in case the nick frees up.
	UpstreamStateDisconnectedNickUnavailable UpstreamState = "disconnected_nick_unavailable"

	// UpstreamStateDisconnectedAuthFailed is 464 ERR_PASSWDMISMATCH,
	// 904 ERR_SASLFAIL, 905 ERR_SASLTOOLONG, or content-matched
	// NickServ rejection. Terminal — the operator must fix credentials.
	UpstreamStateDisconnectedAuthFailed UpstreamState = "disconnected_auth_failed"

	// UpstreamStateDisconnectedBanned is 465 ERR_YOUREBANNEDCREEP or a
	// matched K-line / G-line / Z-line pattern on an ERROR :Closing Link
	// preamble. Terminal — manual operator intervention required.
	UpstreamStateDisconnectedBanned UpstreamState = "disconnected_banned"

	// UpstreamStatePausedIdle is the escalation outcome of staying in
	// disconnected_transient past the configured pause threshold. Terminal
	// from the connector's perspective; the operator resumes externally.
	UpstreamStatePausedIdle UpstreamState = "paused_idle"

	// UpstreamStateStopped is set on operator-initiated shutdown
	// (ctx cancel during the lifecycle). Terminal.
	UpstreamStateStopped UpstreamState = "stopped"
)

// IsRecoverable reports whether the supervisor should keep retrying after
// observing this state. Disconnected_transient + disconnected_nick_unavailable
// are recoverable; everything else is either live or terminal.
func (s UpstreamState) IsRecoverable() bool {
	switch s {
	case UpstreamStateDisconnectedTransient, UpstreamStateDisconnectedNickUnavailable:
		return true
	}
	return false
}

// IsTerminal reports whether the supervisor should stop retrying after
// observing this state. Mirrors the operator-policy fork between
// "automatic reconnect continues" and "needs operator action".
func (s UpstreamState) IsTerminal() bool {
	switch s {
	case UpstreamStateDisconnectedAuthFailed,
		UpstreamStateDisconnectedBanned,
		UpstreamStatePausedIdle,
		UpstreamStateStopped:
		return true
	}
	return false
}

// UpstreamStateChange is the payload delivered to subscribers when the
// state machine transitions. From is the prior state (UpstreamStateIdle
// on the very first transition). ServerReason carries any human-readable
// disconnect text the upstream supplied — empty when not applicable.
type UpstreamStateChange struct {
	From         UpstreamState
	To           UpstreamState
	At           time.Time
	ServerReason string
}

// UpstreamStateSubscriber receives state-change notifications. Handlers
// run synchronously in the goroutine that called Transition, so per-
// message ordering is preserved (Edge 2 in the plan: a state transition
// triggered by a write error must be observable in the same goroutine
// that issued the failing write before any other observer sees it).
type UpstreamStateSubscriber func(UpstreamStateChange)

// Subscription is the handle returned by Subscribe. Callers cancel by
// calling Unsubscribe — useful for tests that attach a recorder and want
// it gone before the next phase.
type Subscription struct {
	machine *UpstreamStateMachine
	id      uint64
}

// Unsubscribe removes the subscriber. Idempotent.
func (s *Subscription) Unsubscribe() {
	if s == nil || s.machine == nil {
		return
	}
	s.machine.unsubscribe(s.id)
}

// TransitionOption tweaks a Transition call. Currently only WithServerReason
// is exposed — additional options can be added without breaking callers.
type TransitionOption func(*transitionParams)

type transitionParams struct {
	serverReason string
}

// WithServerReason attaches a human-readable disconnect reason to the
// transition, parsed out of an ERROR :Closing Link line or carried from
// the underlying Go error. Surfaces through UpstreamStateChange.ServerReason
// so consumers (NOTICE bodies, WS event payloads) can render it.
func WithServerReason(reason string) TransitionOption {
	return func(p *transitionParams) { p.serverReason = reason }
}

// UpstreamStateMachine is the per-connector state holder. Concurrency-safe
// for arbitrary readers and writers. Subscribers fan out synchronously
// under the read lock — handlers must not block (push to a channel or
// spawn a goroutine if they need to).
type UpstreamStateMachine struct {
	mu           sync.RWMutex
	state        UpstreamState
	enteredAt    time.Time
	serverReason string
	log          *slog.Logger

	subMu       sync.RWMutex
	nextSubID   uint64
	subscribers map[uint64]UpstreamStateSubscriber
}

// NewUpstreamStateMachine returns a machine in UpstreamStateIdle.
func NewUpstreamStateMachine(log *slog.Logger) *UpstreamStateMachine {
	if log == nil {
		log = slog.Default()
	}
	return &UpstreamStateMachine{
		state:       UpstreamStateIdle,
		enteredAt:   time.Now(),
		log:         log,
		subscribers: map[uint64]UpstreamStateSubscriber{},
	}
}

// State returns the current upstream state.
func (m *UpstreamStateMachine) State() UpstreamState {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.state
}

// ServerReason returns the last-attached disconnect reason. Empty when
// the current state was entered without one.
func (m *UpstreamStateMachine) ServerReason() string {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.serverReason
}

// DurationIn returns how long the machine has been in its current state.
// Useful for the supervisor's transient-dwell escalation check.
func (m *UpstreamStateMachine) DurationIn() time.Duration {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return time.Since(m.enteredAt)
}

// EnteredAt returns the wall-clock time the machine entered its current
// state. Snapshot consumers (state-mirror emitter, status dashboards)
// use it to label the active state with an absolute timestamp rather
// than a relative duration.
func (m *UpstreamStateMachine) EnteredAt() time.Time {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.enteredAt
}

// Transition moves the machine to a new state and notifies subscribers.
// Transitioning to the current state is a no-op (no subscriber fire, no
// enteredAt reset) — keeps repeated "still transient" announcements from
// flooding consumers.
func (m *UpstreamStateMachine) Transition(to UpstreamState, opts ...TransitionOption) {
	params := transitionParams{}
	for _, o := range opts {
		o(&params)
	}

	m.mu.Lock()
	from := m.state
	if from == to {
		m.mu.Unlock()
		return
	}
	now := time.Now()
	m.state = to
	m.enteredAt = now
	m.serverReason = params.serverReason
	m.mu.Unlock()

	m.log.Info("upstream state",
		"from", string(from),
		"to", string(to),
		"reason", params.serverReason,
	)

	change := UpstreamStateChange{
		From:         from,
		To:           to,
		At:           now,
		ServerReason: params.serverReason,
	}
	m.subMu.RLock()
	subs := make([]UpstreamStateSubscriber, 0, len(m.subscribers))
	for _, s := range m.subscribers {
		subs = append(subs, s)
	}
	m.subMu.RUnlock()
	for _, s := range subs {
		s(change)
	}
}

// Subscribe registers a handler. Returns a Subscription whose Unsubscribe
// method removes the handler.
func (m *UpstreamStateMachine) Subscribe(fn UpstreamStateSubscriber) *Subscription {
	if fn == nil {
		return &Subscription{}
	}
	m.subMu.Lock()
	defer m.subMu.Unlock()
	m.nextSubID++
	id := m.nextSubID
	m.subscribers[id] = fn
	return &Subscription{machine: m, id: id}
}

func (m *UpstreamStateMachine) unsubscribe(id uint64) {
	m.subMu.Lock()
	defer m.subMu.Unlock()
	delete(m.subscribers, id)
}

// ClassifyError maps a transport-level Go error to a recoverable state.
// Returns ok=false only for a nil error; every non-nil error maps to a
// state so callers can adopt the result without special-casing.
//
// Caller is responsible for context cancellation: if errors.Is(err, ctx.Err()),
// the supervisor should treat that as Stopped, not classify the wrapped
// transport error.
func ClassifyError(err error) (UpstreamState, bool) {
	if err == nil {
		return "", false
	}
	// Timeouts (read-deadline, dial deadline) and other net.Error variants.
	var ne net.Error
	if errors.As(err, &ne) {
		return UpstreamStateDisconnectedTransient, true
	}
	if errors.Is(err, io.EOF) ||
		errors.Is(err, io.ErrUnexpectedEOF) ||
		errors.Is(err, net.ErrClosed) ||
		errors.Is(err, syscall.EPIPE) ||
		errors.Is(err, syscall.ECONNRESET) ||
		errors.Is(err, syscall.ECONNREFUSED) {
		return UpstreamStateDisconnectedTransient, true
	}
	// Unknown error shape — still recoverable from the transport's
	// perspective. Auth/ban states are reached via numerics + ERROR
	// preambles, never via raw Go errors.
	return UpstreamStateDisconnectedTransient, true
}

// ClassifyNumeric maps an IRC numeric reply (encountered during
// registration or runtime) to a state. Returns ok=false when the numeric
// isn't classification-relevant — the caller keeps its current state.
//
// params and trailing are accepted for forward compatibility with
// servers that encode the discriminating detail in the trailing slot
// (e.g. some NickServ-style content matches); the current implementation
// only inspects the numeric code itself.
func ClassifyNumeric(numeric string, _ []string, _ string) (UpstreamState, bool) {
	switch numeric {
	case ErrNickNameInUse, ErrUnavailResource:
		return UpstreamStateDisconnectedNickUnavailable, true
	case ErrPasswdMismatch, ErrSaslFail, ErrSaslTooLong:
		return UpstreamStateDisconnectedAuthFailed, true
	case ErrYoureBannedCreep:
		return UpstreamStateDisconnectedBanned, true
	}
	return "", false
}

// banPatterns is the case-insensitive substring set we treat as a
// permanent-ban indicator inside an ERROR :Closing Link preamble. Most
// IRC servers wrap their human reason in parentheses; matching on the
// reason text alone is more forgiving than trying to parse server-
// specific punctuation.
var banPatterns = []string{
	"k-lined",
	"k:lined",
	"g-lined",
	"z-lined",
	"user has been banned",
	"you are not welcome",
	"glined",
	"klined",
}

// ParseERRORLine extracts the human reason from a server `ERROR :...`
// preamble. Returns the parenthesised tail when present (the standard
// `ERROR :Closing Link: nick[ip] (reason)` shape), falling back to the
// full message body when no parentheses are present. ok is false when
// the line isn't an ERROR at all.
func ParseERRORLine(line string) (reason string, ok bool) {
	msg := Parse(line)
	if !strings.EqualFold(msg.Command, "ERROR") {
		return "", false
	}
	body := msg.Trailing
	if body == "" {
		return "", true
	}
	// Prefer the parenthesised reason if present — that's the IRCd
	// idiom for the operator-supplied disconnect reason.
	if open := strings.LastIndex(body, "("); open >= 0 {
		if close := strings.LastIndex(body, ")"); close > open {
			return strings.TrimSpace(body[open+1 : close]), true
		}
	}
	return strings.TrimSpace(body), true
}

// ClassifyBanReason inspects a parsed server reason for ban indicators.
// Returns (UpstreamStateDisconnectedBanned, true) when any of the known
// ban patterns matches; (_, false) otherwise. Caller falls back to its
// own classification (typically transient) when ok is false.
func ClassifyBanReason(reason string) (UpstreamState, bool) {
	if reason == "" {
		return "", false
	}
	lower := strings.ToLower(reason)
	for _, p := range banPatterns {
		if strings.Contains(lower, p) {
			return UpstreamStateDisconnectedBanned, true
		}
	}
	return "", false
}

// DescribeUpstreamState returns the operator-neutral human-readable
// body that surfaces a state change to attached clients. Both the
// IRC bouncer (NOTICE body) and the WS gateway (event message field)
// call this so the wording stays consistent across surfaces.
//
// networkName is the human-readable network identifier (e.g. "Libera
// Chat" or the IRC hostname); falls back to "the network" when empty.
// serverReason is appended when non-empty so failure causes like
// "Ping timeout: 240 seconds" or "K-Lined: spam" thread through to
// the surfaced body.
func DescribeUpstreamState(state UpstreamState, networkName, serverReason string) string {
	network := networkName
	if network == "" {
		network = "the network"
	}
	reasonSuffix := ""
	if serverReason != "" {
		reasonSuffix = ": " + serverReason
	}
	switch state {
	case UpstreamStateRegistered:
		return ""
	case UpstreamStateIdle:
		return "Connector not yet started — waiting for first network connect attempt."
	case UpstreamStateConnecting, UpstreamStateRegistering:
		return "Currently connecting to " + network +
			". Channels will appear when registration completes."
	case UpstreamStateDisconnectedTransient:
		return "Currently disconnected from " + network + reasonSuffix +
			". Reconnecting; messages sent now will NOT be delivered."
	case UpstreamStateDisconnectedNickUnavailable:
		return "Nickname unavailable on " + network +
			" — retrying with an alternate. Channels will appear when registration completes."
	case UpstreamStateDisconnectedAuthFailed:
		return "Authentication failed for " + network + reasonSuffix +
			". Automatic reconnect stopped — update credentials and restart the connector."
	case UpstreamStateDisconnectedBanned:
		return "Banned from " + network + reasonSuffix +
			". Automatic reconnect stopped — manual intervention required."
	case UpstreamStatePausedIdle:
		return "Connector paused after extended unreachability. Restart to retry."
	case UpstreamStateStopped:
		return "Connector stopped."
	}
	return ""
}

// SeverityForUpstreamState maps a state to the severity label used by
// the WS gateway's connector.state_changed event payload. Mirrors the
// three-level scheme the test UI + appui will use to colour-code
// banners and decide whether to disable the send input.
//
//   - "info" for registered (the recovery "back live" event)
//   - "warning" for connecting/registering and recoverable disconnects
//   - "error" for terminal disconnects (auth_failed, banned,
//     paused_idle, stopped)
//   - "" (empty) for idle, which doesn't warrant a banner
func SeverityForUpstreamState(state UpstreamState) string {
	switch state {
	case UpstreamStateRegistered:
		return "info"
	case UpstreamStateConnecting, UpstreamStateRegistering,
		UpstreamStateDisconnectedTransient,
		UpstreamStateDisconnectedNickUnavailable:
		return "warning"
	case UpstreamStateDisconnectedAuthFailed,
		UpstreamStateDisconnectedBanned,
		UpstreamStatePausedIdle,
		UpstreamStateStopped:
		return "error"
	}
	return ""
}

// ClassifyDisconnectMessage is the convenience wrapper that combines
// ParseERRORLine and ClassifyBanReason. Returns:
//
//   - (Banned, reason, true) when the preamble matches a ban pattern
//   - (Transient, reason, true) when the preamble parses but is innocuous
//     (Excess Flood, Ping timeout, Connection reset, …)
//   - ("", "", false) when the line isn't an ERROR
//
// The supervisor calls this on every server-originated line during a
// connected session; non-ERROR lines bypass classification entirely.
func ClassifyDisconnectMessage(line string) (UpstreamState, string, bool) {
	reason, ok := ParseERRORLine(line)
	if !ok {
		return "", "", false
	}
	if state, banned := ClassifyBanReason(reason); banned {
		return state, reason, true
	}
	return UpstreamStateDisconnectedTransient, reason, true
}
