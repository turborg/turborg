package irc

import (
	"bufio"
	"context"
	"crypto/subtle"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"
)

// channelLogCap is the per-channel ring of recent PRIVMSG / NOTICE lines
// replayed to a bouncer client on (re)connect. Bound per-channel so a
// busy channel can't starve quiet ones.
const channelLogCap = 200

// forwardable is the set of commands that bouncer clients are allowed to
// send upstream. Anything else is silently dropped post-auth.
var forwardable = map[string]bool{
	CmdPrivmsg: true,
	CmdNotice:  true,
	CmdJoin:    true,
	CmdPart:    true,
	CmdMode:    true,
	CmdTopic:   true,
	CmdKick:    true,
	CmdNick:    true,
	CmdNames:   true,
}

// requiresTarget marks commands that need a channel/user param. NICK is
// the exception — it carries the new nick as its single param, no target.
var requiresTarget = map[string]bool{
	CmdPrivmsg: true,
	CmdNotice:  true,
	CmdJoin:    true,
	CmdPart:    true,
	CmdMode:    true,
	CmdTopic:   true,
	CmdKick:    true,
	CmdNames:   true,
}

func isWellFormed(m Message) bool {
	if m.Command == CmdNick {
		return len(m.Params) > 0 || m.Trailing != ""
	}
	if requiresTarget[m.Command] {
		return len(m.Params) > 0 && m.Params[0] != ""
	}
	return true
}

// BouncerClient is one TCP client connected to the bouncer.
type BouncerClient struct {
	conn   net.Conn
	reader *bufio.Reader
	wmu    sync.Mutex

	logMu sync.RWMutex
	log   *slog.Logger

	authMu sync.Mutex
	auth   bool

	capMu sync.RWMutex
	caps  map[string]bool
	// capStarted is set on the client's first CAP LS / CAP REQ and
	// cleared on CAP END. While true, registration is "suspended" per
	// IRCv3 spec — the bouncer must hold the 001 welcome + state replay
	// back until CAP END, so the client finishes cap negotiation before
	// it processes its welcome. Without this, the 001 arrives before
	// CAP END and the client treats every cap it negotiates as inactive
	// for the current session, which (among other things) broke
	// echo-message-driven self-message routing in HexChat.
	capStarted      bool
	welcomeDeferred bool
}

func newBouncerClient(conn net.Conn, log *slog.Logger) *BouncerClient {
	return &BouncerClient{
		conn:   conn,
		reader: bufio.NewReader(conn),
		caps:   map[string]bool{},
		log:    log,
	}
}

func (b *BouncerClient) currentLog() *slog.Logger {
	b.logMu.RLock()
	defer b.logMu.RUnlock()
	return b.log
}

func (b *BouncerClient) Authenticated() bool {
	b.authMu.Lock()
	defer b.authMu.Unlock()
	return b.auth
}

func (b *BouncerClient) setAuthenticated() {
	b.authMu.Lock()
	defer b.authMu.Unlock()
	b.auth = true
}

// hasCap reports whether the client successfully negotiated the named
// IRCv3 capability. The bouncer uses this to decide whether to fan
// echoed self-messages back to clients that asked for echo-message.
func (b *BouncerClient) hasCap(name string) bool {
	b.capMu.RLock()
	defer b.capMu.RUnlock()
	return b.caps[name]
}

func (b *BouncerClient) ackCap(name string) {
	b.capMu.Lock()
	defer b.capMu.Unlock()
	b.caps[name] = true
}

// startCapNeg marks the client as having entered IRCv3 cap negotiation
// (sent CAP LS or CAP REQ). Registration is suspended until CAP END.
func (b *BouncerClient) startCapNeg() {
	b.capMu.Lock()
	defer b.capMu.Unlock()
	b.capStarted = true
}

// endCapNeg marks negotiation as done and returns whether a deferred
// welcome should be flushed.
func (b *BouncerClient) endCapNeg() bool {
	b.capMu.Lock()
	defer b.capMu.Unlock()
	b.capStarted = false
	if b.welcomeDeferred {
		b.welcomeDeferred = false
		return true
	}
	return false
}

// deferOrAllowWelcome reports whether the bouncer must defer the
// welcome / state replay until CAP END, marking it as queued when so.
// Returns true when the caller should send immediately.
func (b *BouncerClient) deferOrAllowWelcome() bool {
	b.capMu.Lock()
	defer b.capMu.Unlock()
	if b.capStarted {
		b.welcomeDeferred = true
		return false
	}
	return true
}

func (b *BouncerClient) Address() string {
	if b.conn == nil {
		return ""
	}
	return b.conn.RemoteAddr().String()
}

func (b *BouncerClient) sendLine(line string) error {
	if log := b.currentLog(); log != nil {
		log.Debug("bouncer >>", "client", b.Address(), "line", line)
	}
	b.wmu.Lock()
	defer b.wmu.Unlock()
	_, err := b.conn.Write([]byte(strings.TrimRight(line, "\r\n") + "\r\n"))
	return err
}

func (b *BouncerClient) readLine() (string, error) {
	raw, err := b.reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimRight(raw, "\r\n"), nil
}

func (b *BouncerClient) close() error { return b.conn.Close() }

// SendUpstreamFunc is the upstream-write callback the connector wires
// into the bouncer so authenticated clients can forward IRC lines to the
// real server.
type SendUpstreamFunc func(line string) error

// ForwardedObserver is fired whenever a bouncer client tunnels a
// PRIVMSG / NOTICE upstream. The connector hooks into this to publish
// MESSAGE_SENT events on its EventBus, so other subscribers (WS gateway,
// agent handlers) see what bouncer clients sent — the IRC server doesn't
// echo your own traffic back, so without this hook those messages would
// be invisible to anything that isn't itself a bouncer client.
type ForwardedObserver func(channel, sender, text, kind string)

// Bouncer is a TCP server that bridges local IRC clients into the
// agent's single upstream IRC connection.
type Bouncer struct {
	host     string
	port     int
	password string

	rateLimiter *RateLimiter
	log         *slog.Logger

	mu       sync.Mutex
	clients  map[*BouncerClient]struct{}
	listener net.Listener
	cancel   context.CancelFunc
	wg       sync.WaitGroup

	upstreamMu     sync.RWMutex
	upstreamNick   string
	upstreamIdent  string
	upstreamHost   string

	sendUpstream SendUpstreamFunc
	onForwarded  ForwardedObserver
	state        *ChannelState

	// limits gates client-initiated commands by operator policy. Zero
	// value (unrestricted) is the default — see AttachClientLimits to
	// override.
	limits ClientLimits

	// outboundThrottle gates per-target PRIVMSG flow from attached
	// clients. nil = unrestricted. Shared with the WS gateway so a
	// single user with both HexChat and the web UI open shares one
	// bucket per target.
	outboundThrottle *Throttle

	// onAttach, when non-nil, fires once per successful client auth
	// with the reason string the activity package uses. nil = no-op.
	// Wired by runtime so an external observer can be told a real user
	// just attached, distinct from "bot is alive" log scrapes.
	onAttach func(reason string)

	// machine is the per-connector upstream state machine the bouncer
	// subscribes to so it can surface state to attached clients (entry
	// NOTICEs on transition, state-informative NOTICEs on fresh
	// attach, PRIVMSG-gating). nil before AttachUpstreamState has been
	// called — treated as "always registered" so tests that don't care
	// about state can leave it un-attached.
	machine     *UpstreamStateMachine
	networkName string
	stateSub    *Subscription

	// wanted is the per-connector wanted-channels set the bouncer
	// updates when it observes a client-originated JOIN / PART. Lets
	// the supervisor's reconnect replay pick up channels the user
	// joined post-startup, with the channel keys the user supplied.
	wanted *WantedChannels

	// setPreferredNick is the hook the bouncer fires for detached-
	// state NICK commands so the supervisor's next register() uses
	// the queued nick rather than the env-configured one. nil disables
	// queuing; the bouncer still acknowledges the NICK with a NOTICE.
	setPreferredNick func(nick string)

	logMu      sync.Mutex
	channelLog map[string][]string
}

func NewBouncer(password, host string, port int, rl *RateLimiter, log *slog.Logger) (*Bouncer, error) {
	if password == "" {
		return nil, errors.New("bouncer: password cannot be empty")
	}
	if host == "" {
		host = "127.0.0.1"
	}
	if log == nil {
		log = slog.Default()
	}
	return &Bouncer{
		host:         host,
		port:         port,
		password:     password,
		rateLimiter:  rl,
		log:          log,
		clients:      map[*BouncerClient]struct{}{},
		channelLog:   map[string][]string{},
		upstreamHost: "turborg",
	}, nil
}

func (b *Bouncer) AttachUpstream(send SendUpstreamFunc) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.sendUpstream = send
}

// AttachClientLimits installs the operator-policy limits the bouncer
// consults before forwarding client-originated commands upstream. Called
// at most once from runtime.Build; left zero (unrestricted) when no
// policy applies. Safe to call before Start.
func (b *Bouncer) AttachClientLimits(l ClientLimits) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.limits = l
}

// AttachOutboundThrottle installs the per-target outbound throttle the
// bouncer consults for client-originated PRIVMSG. Pass nil to disable.
// Shared instance with the WS gateway: both surfaces consult the same
// bucket per target so two clients of the same user share one budget.
func (b *Bouncer) AttachOutboundThrottle(t *Throttle) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.outboundThrottle = t
}

// AttachActivityHook installs a callback fired once per successful client
// auth. Pass nil to disable. Lets the runtime forward "a client is using
// this bouncer right now" signals to an external observer without the
// bouncer itself knowing what the observer is.
func (b *Bouncer) AttachActivityHook(hook func(reason string)) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.onAttach = hook
}

func (b *Bouncer) AttachOutboundObserver(cb ForwardedObserver) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.onForwarded = cb
}

// AttachUpstreamState binds the per-connector upstream state machine
// + a human-readable network name (e.g. "Libera Chat") used in state-
// surfacing NOTICE bodies. Must be called before Start. Network name
// defaults to "the network" when empty. Subscription is created in
// Start so it can be torn down in Stop without leaking goroutines or
// double-firing on a fresh Start/Stop cycle.
func (b *Bouncer) AttachUpstreamState(machine *UpstreamStateMachine, networkName string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.machine = machine
	b.networkName = networkName
}

// AttachWantedChannels binds the per-connector wanted-channels set
// the bouncer mutates on observed client JOIN/PART traffic. The
// supervisor reads from this set on every reconnect to know what
// channels (and with what keys) to rejoin.
func (b *Bouncer) AttachWantedChannels(w *WantedChannels) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.wanted = w
}

// AttachPreferredNickHook installs the callback the bouncer fires
// when a client issues NICK during a detached upstream. The connector
// stores the queued nick and applies it on the next register(). nil
// disables queuing — clients still see a "queued" NOTICE on detached
// NICK but the next registration uses the env-configured nick.
func (b *Bouncer) AttachPreferredNickHook(hook func(nick string)) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.setPreferredNick = hook
}

// upstreamState returns the live upstream state. Defaults to
// UpstreamStateRegistered when no state machine has been attached —
// keeps tests that pre-date the state machine working without
// modification (they implicitly assume the bouncer never refuses to
// forward).
func (b *Bouncer) upstreamState() UpstreamState {
	b.mu.Lock()
	machine := b.machine
	b.mu.Unlock()
	if machine == nil {
		return UpstreamStateRegistered
	}
	return machine.State()
}

// network returns the human-readable network name for NOTICE bodies.
// Falls back to a generic placeholder when no name was attached.
func (b *Bouncer) network() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.networkName == "" {
		return "the network"
	}
	return b.networkName
}

// AttachState binds a live ChannelState plus the upstream identity used
// when crafting synthetic JOIN lines for state replay.
func (b *Bouncer) AttachState(state *ChannelState, nick, ident, host string) {
	b.upstreamMu.Lock()
	defer b.upstreamMu.Unlock()
	b.state = state
	b.upstreamNick = nick
	b.upstreamIdent = ident
	if host != "" {
		b.upstreamHost = host
	}
}

func (b *Bouncer) UpdateUpstreamNick(nick string) {
	b.upstreamMu.Lock()
	defer b.upstreamMu.Unlock()
	b.upstreamNick = nick
}

// UpdateUpstreamIdentity pins the ident/host the network actually
// assigned the bot, observed after the first self-prefixed message.
// Without this, clients like HexChat misroute self-PRIVMSGs because the
// nick!ident@host on broadcast doesn't match their own user record.
func (b *Bouncer) UpdateUpstreamIdentity(ident, host string) {
	b.upstreamMu.Lock()
	defer b.upstreamMu.Unlock()
	b.upstreamIdent = ident
	if host != "" {
		b.upstreamHost = host
	}
}

func (b *Bouncer) upstreamPrefix() string {
	b.upstreamMu.RLock()
	defer b.upstreamMu.RUnlock()
	if b.upstreamNick == "" {
		return ""
	}
	return fmt.Sprintf(":%s!%s@%s", b.upstreamNick, b.upstreamIdent, b.upstreamHost)
}

// Start binds the listener and spawns the accept loop. Returns once the
// listener is ready (so callers can race a client connect against
// Start). Stop must be called to release resources.
func (b *Bouncer) Start(ctx context.Context) error {
	addr := fmt.Sprintf("%s:%d", b.host, b.port)
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("bouncer listen %s: %w", addr, err)
	}
	b.mu.Lock()
	b.listener = l
	machine := b.machine
	b.mu.Unlock()

	bctx, cancel := context.WithCancel(ctx)
	b.cancel = cancel

	if machine != nil {
		sub := machine.Subscribe(b.onUpstreamStateChange)
		b.mu.Lock()
		b.stateSub = sub
		b.mu.Unlock()
	}

	b.wg.Add(1)
	go b.acceptLoop(bctx, l)

	b.log.Info("bouncer listening", "host", b.host, "port", b.port)
	return nil
}

// Stop closes every active client, the listener, and waits for in-flight
// handlers to drain. Safe to call multiple times; second and subsequent
// calls are no-ops.
func (b *Bouncer) Stop() error {
	b.mu.Lock()
	cancel := b.cancel
	listener := b.listener
	stateSub := b.stateSub
	clients := make([]*BouncerClient, 0, len(b.clients))
	for c := range b.clients {
		clients = append(clients, c)
	}
	b.clients = map[*BouncerClient]struct{}{}
	b.listener = nil
	b.cancel = nil
	b.stateSub = nil
	b.mu.Unlock()

	if stateSub != nil {
		stateSub.Unsubscribe()
	}
	if cancel != nil {
		cancel()
	}
	for _, c := range clients {
		_ = c.close()
	}
	if listener != nil {
		_ = listener.Close()
	}
	b.wg.Wait()
	return nil
}

// Addr returns the resolved listen address (useful for tests using port 0).
func (b *Bouncer) Addr() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.listener == nil {
		return ""
	}
	return b.listener.Addr().String()
}

// Clients returns a snapshot of currently-attached clients. Used by tests.
func (b *Bouncer) Clients() []*BouncerClient {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]*BouncerClient, 0, len(b.clients))
	for c := range b.clients {
		out = append(out, c)
	}
	return out
}

func (b *Bouncer) acceptLoop(ctx context.Context, l net.Listener) {
	defer b.wg.Done()
	for {
		conn, err := l.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			b.log.Debug("bouncer accept", "err", err)
			return
		}
		client := newBouncerClient(conn, b.log)
		b.mu.Lock()
		b.clients[client] = struct{}{}
		b.mu.Unlock()
		b.wg.Add(1)
		go b.handleClient(ctx, client)
	}
}

func (b *Bouncer) handleClient(ctx context.Context, client *BouncerClient) {
	defer b.wg.Done()
	defer func() {
		b.mu.Lock()
		delete(b.clients, client)
		b.mu.Unlock()
		_ = client.close()
	}()

	_ = client.sendLine(
		":turborg-bouncer NOTICE AUTH :*** This is a turborg bouncer. " +
			"Set your client's *server password* to TURBORG_IRC_BOUNCER_PASSWORD " +
			"and reconnect. (HexChat: Network List → Edit → Password.)",
	)

	for {
		if ctx.Err() != nil {
			return
		}
		line, err := client.readLine()
		if err != nil {
			return
		}
		b.handleLine(client, line)
	}
}

func (b *Bouncer) handleLine(client *BouncerClient, line string) {
	msg := Parse(line)
	b.log.Debug("bouncer <<",
		"auth", client.Authenticated(),
		"cmd", msg.Command,
		"line", line,
	)
	switch msg.Command {
	case CmdPass:
		b.handlePass(client, msg.Params)
		return
	case CmdPing:
		// Reply locally — never forward upstream. Works pre- and post-auth.
		cookie := msg.Trailing
		if cookie == "" && len(msg.Params) > 0 {
			cookie = msg.Params[0]
		}
		_ = client.sendLine(":turborg-bouncer PONG turborg-bouncer :" + cookie)
		return
	case CmdCap:
		// CAP is handled regardless of auth state — it can arrive
		// pre-registration (CAP LS / REQ / END) AND post-auth (CAP
		// LIST, mid-session re-negotiation). Without this, a CAP END
		// that arrived after PASS would fall through the forwardable
		// filter and the deferred welcome would never flush.
		b.handleCap(client, msg)
		return
	}

	if !client.Authenticated() {
		// Real clients send a varied bag of pre-PASS commands; silently drop.
		return
	}

	if !forwardable[msg.Command] {
		return
	}
	if !isWellFormed(msg) {
		return
	}

	// Gate forwardable client commands on upstream state — anything
	// other than `registered` reroutes through the detached handler,
	// which surfaces a state-appropriate NOTICE rather than silently
	// dropping the line. PRIVMSG/NOTICE specifically need their NOTICE
	// addressed to the channel target so the rejection lands in the
	// buffer where the user typed.
	if b.upstreamState() != UpstreamStateRegistered {
		b.rejectForDetached(client, msg)
		return
	}

	b.mu.Lock()
	send := b.sendUpstream
	observer := b.onForwarded
	b.mu.Unlock()

	if !b.allowByPolicy(client, msg) {
		return
	}

	if send == nil {
		return
	}
	if err := send(line); err != nil {
		b.log.Debug("bouncer forward upstream", "err", err)
		// Write failed mid-flight — upstream just went down and the
		// state machine hasn't caught up yet (we passed the gate
		// moments ago). Surface this specific failure to the client
		// rather than silent-dropping. Only PRIVMSG/NOTICE get a
		// channel-targeted NOTICE; other commands hit the generic
		// detached handler which targets the status placeholder.
		b.rejectForDetached(client, msg)
		return
	}
	b.afterForwarded(client, msg, line, observer)
}

// afterForwarded runs the post-send bookkeeping that the bouncer owes
// upon successfully forwarding a client command upstream:
//
//   - PRIVMSG/NOTICE: fan to other attached clients (server doesn't
//     echo own messages) and notify the outbound observer.
//   - JOIN/PART: update the per-connector wanted-channels set so the
//     supervisor's reconnect replay reflects the client's current
//     channel intent — including any channel keys the JOIN carried.
//
// Extracted from handleLine to keep its cyclomatic complexity within
// the project's gocyclo budget.
func (b *Bouncer) afterForwarded(client *BouncerClient, msg Message, line string, observer ForwardedObserver) {
	switch msg.Command {
	case CmdPrivmsg, CmdNotice:
		b.fanMessage(client, msg, line, observer)
	case CmdJoin:
		b.recordJoinedToWanted(msg)
	case CmdPart:
		b.recordPartedFromWanted(msg)
	}
}

func (b *Bouncer) fanMessage(client *BouncerClient, msg Message, line string, observer ForwardedObserver) {
	// If the originator negotiated echo-message (IRCv3), fan to them
	// too — that's what the cap promises: "your own messages will come
	// back to you so you can render them through the same incoming-
	// message path that every other client uses." Without the cap,
	// exclude the originator to avoid double-display.
	var exclude *BouncerClient
	if !client.hasCap("echo-message") {
		exclude = client
	}
	b.BroadcastAsSelf(line, exclude)
	if observer == nil || len(msg.Params) == 0 {
		return
	}
	b.upstreamMu.RLock()
	sender := b.upstreamNick
	b.upstreamMu.RUnlock()
	if sender == "" {
		sender = "*"
	}
	observer(msg.Params[0], sender, msg.Trailing, msg.Command)
}

func (b *Bouncer) recordJoinedToWanted(msg Message) {
	b.mu.Lock()
	wanted := b.wanted
	b.mu.Unlock()
	if wanted == nil {
		return
	}
	channels, keys := parseJoinLine(msg.Params, msg.Trailing)
	for i, ch := range channels {
		wanted.Add(ch, keys[i])
	}
}

func (b *Bouncer) recordPartedFromWanted(msg Message) {
	b.mu.Lock()
	wanted := b.wanted
	b.mu.Unlock()
	if wanted == nil || len(msg.Params) == 0 {
		return
	}
	for _, ch := range splitAndTrim(msg.Params[0], ",") {
		wanted.Remove(ch)
	}
}

// supportedCaps is the set of IRCv3 capabilities the bouncer
// advertises + ACKs. The set mirrors what ZNC offers (minus SASL,
// which the bouncer handles via PASS instead) — empirically HexChat
// 2.16's auto-cap-request only kicks in when it sees a "richer"
// IRCv3 LS reply. With just `echo-message znc.in/self-message`
// advertised, HexChat 2.16 silently skipped the REQ; with the fuller
// list below it negotiates properly.
//
// What each cap means for the bouncer:
//
//   - echo-message: standardized IRCv3 self-message cap. The bouncer
//     fans self-PRIVMSG / NOTICE to clients that negotiate it so they
//     can render in the recipient's tab as outgoing.
//   - znc.in/self-message: ZNC's pre-IRCv3 predecessor. When
//     negotiated, the fan-out line is prefixed with the
//     `@znc.in/self-message` tag so the legacy handler routes it.
//   - message-tags: framework cap that enables server-side IRCv3
//     `@...` tags on the wire. The bouncer passes through whatever
//     tags upstream emitted (notably @time= and @account=).
//   - server-time: lets upstream's `@time=ISO8601` tag round-trip to
//     the bouncer client. Pure pass-through.
//   - account-tag: lets upstream's `@account=<nick>` tag round-trip.
//     Pure pass-through.
//   - away-notify: when an upstream user goes away/back, the server
//     sends AWAY notifications. Pure pass-through.
//
// All listed caps are honored by the existing dispatchLine →
// Broadcast path; advertising them costs nothing extra.
var supportedCaps = map[string]bool{
	"echo-message":        true,
	"znc.in/self-message": true,
	"message-tags":        true,
	"server-time":         true,
	"account-tag":         true,
	"away-notify":         true,
}

func supportedCapsList() string {
	out := make([]string, 0, len(supportedCaps))
	for c := range supportedCaps {
		out = append(out, c)
	}
	return strings.Join(out, " ")
}

func (b *Bouncer) handleCap(client *BouncerClient, msg Message) {
	sub := ""
	if len(msg.Params) > 0 {
		sub = strings.ToUpper(msg.Params[0])
	}
	switch sub {
	case "LS":
		client.startCapNeg()
		_ = client.sendLine(":turborg-bouncer CAP * LS :" + supportedCapsList())
	case "REQ":
		client.startCapNeg()
		requested := msg.Trailing
		if requested == "" && len(msg.Params) > 1 {
			requested = strings.Join(msg.Params[1:], " ")
		}
		ack := make([]string, 0)
		nak := make([]string, 0)
		for _, c := range strings.Fields(requested) {
			if supportedCaps[c] {
				client.ackCap(c)
				ack = append(ack, c)
			} else {
				nak = append(nak, c)
			}
		}
		if len(ack) > 0 {
			_ = client.sendLine(":turborg-bouncer CAP * ACK :" + strings.Join(ack, " "))
		}
		if len(nak) > 0 {
			_ = client.sendLine(":turborg-bouncer CAP * NAK :" + strings.Join(nak, " "))
		}
	case "LIST":
		client.capMu.RLock()
		names := make([]string, 0, len(client.caps))
		for c := range client.caps {
			names = append(names, c)
		}
		client.capMu.RUnlock()
		_ = client.sendLine(":turborg-bouncer CAP * LIST :" + strings.Join(names, " "))
	case "END":
		// Registration unblocked. If PASS landed during negotiation,
		// flush the welcome + state replay we held back so the client
		// only sees them once its cap set is fully active.
		if client.endCapNeg() {
			b.sendWelcome(client)
		}
	}
}

func (b *Bouncer) handlePass(client *BouncerClient, params []string) {
	ip := peerIP(client)
	if b.rateLimiter != nil && ip != "" && b.rateLimiter.IsLocked(ip) {
		remaining := b.rateLimiter.TimeUntilUnlock(ip)
		b.log.Warn("bouncer auth blocked", "ip", ip, "remaining", remaining)
		_ = client.sendLine("ERROR :Closing Link (Too many attempts)")
		_ = client.close()
		return
	}

	if len(params) > 0 && subtle.ConstantTimeCompare([]byte(params[0]), []byte(b.password)) == 1 {
		client.setAuthenticated()
		if b.rateLimiter != nil && ip != "" {
			b.rateLimiter.RecordSuccess(ip)
		}
		b.log.Info("bouncer auth success", "ip", ip)
		b.mu.Lock()
		hook := b.onAttach
		b.mu.Unlock()
		if hook != nil {
			hook("bouncer_attach")
		}

		// IRCv3: if the client is mid-CAP-negotiation, hold the
		// welcome + state replay until CAP END. Sending 001 now would
		// finalize registration before the client's cap set is active
		// — echo-message in particular wouldn't apply to the channel
		// replay that follows, and HexChat then treats every
		// bouncer-echoed self-PRIVMSG as an incoming stranger message.
		if client.deferOrAllowWelcome() {
			b.sendWelcome(client)
		}
		return
	}

	if b.rateLimiter != nil && ip != "" {
		outcome := b.rateLimiter.RecordFailure(ip)
		switch {
		case outcome.Locked:
			b.log.Warn("bouncer auth lockout", "ip", ip, "duration", b.rateLimiter.Lockout)
		default:
			b.log.Info("bouncer auth fail",
				"ip", ip, "count", outcome.Count, "threshold", b.rateLimiter.MaxFailures)
		}
	} else {
		b.log.Info("bouncer auth fail", "ip", ip)
	}
	_ = client.sendLine("ERROR :Closing Link (Bad password)")
	_ = client.close()
}

// sendWelcome emits the 001 welcome + state-surfacing NOTICE (when
// upstream isn't registered) + state replay + channel-log replay for
// a freshly-authenticated client. Called either inline from handlePass
// (no CAP negotiation in progress) or from CAP END (client was
// negotiating caps when PASS authenticated).
func (b *Bouncer) sendWelcome(client *BouncerClient) {
	b.upstreamMu.RLock()
	target := b.upstreamNick
	b.upstreamMu.RUnlock()
	if target == "" {
		target = "*"
	}
	_ = client.sendLine(fmt.Sprintf(":turborg-bouncer 001 %s :Welcome to the turborg bouncer", target))
	// Tell the client where upstream stands BEFORE the JOIN replay so
	// a client attaching during a long outage sees an explanation
	// instead of a silent gap. When upstream is registered this is a
	// no-op and the normal channel replay carries the live state.
	b.surfaceStateToClient(client)
	b.replayState(client)
	b.replayChannelLogs(client)
}

func (b *Bouncer) replayState(client *BouncerClient) {
	b.upstreamMu.RLock()
	state := b.state
	prefix := ""
	target := b.upstreamNick
	if b.upstreamNick != "" {
		prefix = fmt.Sprintf(":%s!%s@%s", b.upstreamNick, b.upstreamIdent, b.upstreamHost)
	}
	b.upstreamMu.RUnlock()
	if state == nil || prefix == "" {
		return
	}

	for _, info := range state.JoinedChannels() {
		_ = client.sendLine(fmt.Sprintf("%s JOIN %s", prefix, info.Name))
		if info.TopicSet {
			_ = client.sendLine(fmt.Sprintf(
				":turborg-bouncer 332 %s %s :%s", target, info.Name, info.Topic,
			))
			if info.TopicSetBy != "" && info.TopicSetAt > 0 {
				_ = client.sendLine(fmt.Sprintf(
					":turborg-bouncer 333 %s %s %s %d",
					target, info.Name, info.TopicSetBy, info.TopicSetAt,
				))
			}
		}
		if len(info.Members) > 0 {
			parts := make([]string, 0, len(info.Members))
			for nick, modePrefix := range info.Members {
				parts = append(parts, modePrefix+nick)
			}
			_ = client.sendLine(fmt.Sprintf(
				":turborg-bouncer 353 %s = %s :%s", target, info.Name, strings.Join(parts, " "),
			))
		}
		_ = client.sendLine(fmt.Sprintf(
			":turborg-bouncer 366 %s %s :End of /NAMES list", target, info.Name,
		))
	}
}

func (b *Bouncer) replayChannelLogs(client *BouncerClient) {
	b.logMu.Lock()
	snap := make(map[string][]string, len(b.channelLog))
	for k, v := range b.channelLog {
		if len(v) == 0 {
			continue
		}
		cp := make([]string, len(v))
		copy(cp, v)
		snap[k] = cp
	}
	b.logMu.Unlock()

	b.upstreamMu.RLock()
	hasUpstream := b.upstreamNick != ""
	b.upstreamMu.RUnlock()
	if !hasUpstream {
		return
	}

	for channel, lines := range snap {
		_ = client.sendLine(fmt.Sprintf(
			":turborg-bouncer NOTICE %s :--- buffer playback for %s (%d lines) ---",
			channel, channel, len(lines),
		))
		for _, line := range lines {
			if err := client.sendLine(line); err != nil {
				return
			}
		}
		_ = client.sendLine(fmt.Sprintf(
			":turborg-bouncer NOTICE %s :--- end of buffer ---", channel,
		))
	}
}

// Broadcast writes line to every authenticated client except exclude.
// Also captures channel-targeted PRIVMSG / NOTICE into the per-channel
// allowByPolicy consults ClientLimits for a client-initiated command and
// returns true when it may flow upstream. A false return implies the
// caller already notified the client and should drop the line. JOIN
// folds in the current channel count from b.state; other commands ignore
// the count argument. PRIVMSG additionally consults the per-target
// outbound throttle when one is attached.
func (b *Bouncer) allowByPolicy(client *BouncerClient, msg Message) bool {
	b.mu.Lock()
	limits := b.limits
	throttle := b.outboundThrottle
	b.mu.Unlock()

	currentChannels := 0
	if msg.Command == CmdJoin && b.state != nil {
		currentChannels = b.state.Count()
	}
	if allow, reason := limits.AllowCommand(msg.Command, currentChannels); !allow {
		// AllowCommand currently gates NICK / USER / JOIN — none of
		// which carry channel context the user has open in their
		// client (a JOIN over-cap means the user never made it into
		// the target channel). Route the NOTICE to the bot's nick so
		// it lands in the client's server status tab.
		b.notifyPolicyDenial(client, b.statusTarget(), reason)
		b.log.Info("cap_hit",
			"surface", "bouncer",
			"kind", CapHitKind(msg.Command),
			"source_cmd", msg.Command,
		)
		return false
	}

	// Per-target outbound throttle. Only PRIVMSG is gated here; bot-
	// originated command replies (which take a different path) keep
	// their existing per-sender command throttle. The NOTICE is
	// addressed to the PRIVMSG's target channel/nick so it lands in
	// the buffer where the user typed, not the server status tab.
	if msg.Command == CmdPrivmsg && throttle != nil && len(msg.Params) > 0 {
		target := msg.Params[0]
		if res := throttle.AllowWithReason(target); !res.Allow {
			seconds := int(res.RetryAfter.Round(time.Second) / time.Second)
			if seconds < 1 {
				seconds = 1
			}
			b.notifyPolicyDenial(client, target,
				fmt.Sprintf("Message rate-limited — wait %ds before sending to %s again. NOT sent.", seconds, target))
			b.log.Info("cap_hit",
				"surface", "bouncer",
				"kind", "outbound_rate",
				"target", target,
				"retry_after_s", seconds,
			)
			return false
		}
	}

	return true
}

// statusTarget returns the target string used for service NOTICEs that
// have no specific channel context (nick changes, channel-cap rejects,
// realname locks). When the bouncer has learned the bot's upstream
// nick, that's the right target — most clients route nick-targeted
// NOTICEs into the server status tab. Pre-registration the IRC `*`
// placeholder is used.
func (b *Bouncer) statusTarget() string {
	b.upstreamMu.RLock()
	defer b.upstreamMu.RUnlock()
	if b.upstreamNick == "" {
		return "*"
	}
	return b.upstreamNick
}

// notifyPolicyDenial sends a NOTICE back to the originating client when
// an operator-policy gate refuses one of its commands. The target
// chooses where the NOTICE lands in the client's UI: pass a channel
// name for messages tied to a channel (rate limits, channel-targeted
// rejects), or the bot's nick / `*` for messages that have no channel
// context (nick locks, channel-cap rejects). Errors are logged at
// debug level — a failed NOTICE shouldn't fail the surrounding
// handler.
func (b *Bouncer) notifyPolicyDenial(client *BouncerClient, target, reason string) {
	if target == "" {
		target = "*"
	}
	line := ":turborg-bouncer NOTICE " + target + " :" + reason
	if err := client.sendLine(line); err != nil {
		b.log.Debug("bouncer policy notice", "err", err)
	}
}

// rejectForDetached is the per-command handler the upstream-state
// gate in handleLine routes to when upstream isn't `registered`. It
// distinguishes "queue-and-tell-the-truth" semantics from "refuse-
// and-explain": JOIN / PART / NICK get queued into the connector's
// wanted-set or preferred-nick slot for the next reconnect (with a
// "queued" NOTICE so the user knows what's going to happen), while
// PRIVMSG / NOTICE / MODE / TOPIC / KICK / NAMES get refused with a
// channel-targeted NOTICE that explains why.
//
// Never fakes a successful acknowledgement upstream — that's the
// silent-message-loss bug this plan exists to fix. If the operation
// can't actually flow upstream right now, the client must learn that
// at the moment it asks, not five minutes later when something
// inconsistent surfaces.
func (b *Bouncer) rejectForDetached(client *BouncerClient, msg Message) {
	switch msg.Command {
	case CmdJoin:
		b.handleDetachedJoin(client, msg)
	case CmdPart:
		b.handleDetachedPart(client, msg)
	case CmdNick:
		b.handleDetachedNick(client, msg)
	default:
		b.handleDetachedRefusal(client, msg)
	}
}

// handleDetachedJoin queues the channel(s) into the wanted-set so the
// supervisor's next reconnect rejoins them, then NOTICEs the client
// per channel with what was queued. Channel keys from the JOIN line
// are preserved.
func (b *Bouncer) handleDetachedJoin(client *BouncerClient, msg Message) {
	b.mu.Lock()
	wanted := b.wanted
	b.mu.Unlock()
	channels, keys := parseJoinLine(msg.Params, msg.Trailing)
	if len(channels) == 0 {
		return
	}
	for i, ch := range channels {
		if wanted != nil {
			wanted.Add(ch, keys[i])
		}
		body := "Queued JOIN " + ch + " — channel will be available when reconnected to " +
			b.network() + "."
		_ = client.sendLine(":turborg-bouncer NOTICE " + ch + " :" + body)
	}
}

// handleDetachedPart removes the channel(s) from the wanted-set so
// the supervisor doesn't silently rejoin them on the next reconnect,
// then NOTICEs the client per channel.
func (b *Bouncer) handleDetachedPart(client *BouncerClient, msg Message) {
	b.mu.Lock()
	wanted := b.wanted
	b.mu.Unlock()
	if len(msg.Params) == 0 {
		return
	}
	for _, ch := range splitAndTrim(msg.Params[0], ",") {
		if wanted != nil {
			wanted.Remove(ch)
		}
		body := "Removed " + ch + " from the auto-join list. Will not rejoin on next reconnect."
		_ = client.sendLine(":turborg-bouncer NOTICE " + ch + " :" + body)
	}
}

// handleDetachedNick queues the requested nick for the next
// registration via the connector's preferred-nick hook (when wired)
// and NOTICEs the client. Doesn't fake a NICK echo — that would lie
// about the bot's identity during the outage.
func (b *Bouncer) handleDetachedNick(client *BouncerClient, msg Message) {
	newNick := msg.Trailing
	if newNick == "" && len(msg.Params) > 0 {
		newNick = msg.Params[0]
	}
	newNick = strings.TrimSpace(newNick)
	if newNick == "" {
		return
	}
	b.mu.Lock()
	hook := b.setPreferredNick
	b.mu.Unlock()
	if hook != nil {
		hook(newNick)
	}
	body := "Nick change queued — " + newNick + " will be used on next reconnect to " +
		b.network() + "."
	_ = client.sendLine(":turborg-bouncer NOTICE " + b.statusTarget() + " :" + body)
}

// handleDetachedRefusal handles every forwardable command that can't
// be queued and replayed: PRIVMSG / NOTICE / MODE / TOPIC / KICK /
// NAMES. PRIVMSG and NOTICE rejections target the channel/nick the
// message was destined for (the buffer where the user typed). Others
// target the status placeholder.
func (b *Bouncer) handleDetachedRefusal(client *BouncerClient, msg Message) {
	state := b.upstreamState()
	body := b.detachedRejectBody(state, msg.Command)
	target := b.statusTarget()
	switch msg.Command {
	case CmdPrivmsg, CmdNotice:
		if len(msg.Params) > 0 {
			target = msg.Params[0]
		}
	case CmdMode, CmdTopic, CmdKick, CmdNames:
		if len(msg.Params) > 0 {
			target = msg.Params[0]
		}
	}
	if err := client.sendLine(":turborg-bouncer NOTICE " + target + " :" + body); err != nil {
		b.log.Debug("bouncer detached reject", "err", err)
	}
}

// detachedRejectBody returns the per-state NOTICE body for a rejected
// client command. The body explains both the operation outcome ("NOT
// sent" / "NOT performed") and the reason (transient / banned /
// auth-failed / paused).
func (b *Bouncer) detachedRejectBody(state UpstreamState, cmd string) string {
	verb := "operation NOT performed"
	if cmd == CmdPrivmsg || cmd == CmdNotice {
		verb = "message NOT sent"
	}
	network := b.network()
	switch state {
	case UpstreamStateDisconnectedNickUnavailable:
		return "Nickname unavailable on " + network +
			" — " + verb + ". Retrying with an alternate."
	case UpstreamStateDisconnectedAuthFailed:
		return "Authentication failed for " + network +
			" — " + verb + ". Update credentials and restart the connector."
	case UpstreamStateDisconnectedBanned:
		return "Banned from " + network +
			" — " + verb + ". Manual intervention required."
	case UpstreamStatePausedIdle:
		return "Connector paused after extended unreachability — " +
			verb + ". Restart to retry."
	case UpstreamStateStopped:
		return "Connector stopped — " + verb + "."
	case UpstreamStateIdle, UpstreamStateConnecting, UpstreamStateRegistering:
		return "Not yet connected to " + network +
			" — " + verb + ". Connecting…"
	}
	// disconnected_transient + safety default.
	return "Not connected to " + network +
		" — " + verb + ". Reconnecting."
}

// describeUpstreamState returns the human-readable NOTICE body the
// bouncer surfaces when the upstream is in the given state. Empty
// return = no NOTICE for this state (registered is the live happy
// path and needs no surfacing).
func (b *Bouncer) describeUpstreamState(state UpstreamState, serverReason string) string {
	network := b.network()
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
		return "Currently connecting to " + network + ". Channels will appear when registration completes."
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

// surfaceStateToClient sends a state-informative NOTICE to a single
// client based on the current upstream state. Fired on client attach
// after the welcome banner so a freshly-connecting HexChat doesn't see
// "connected, no channels, no error" when upstream is detached.
// Skips when state == registered (the normal JOIN replay path covers
// it).
func (b *Bouncer) surfaceStateToClient(c *BouncerClient) {
	b.mu.Lock()
	machine := b.machine
	b.mu.Unlock()
	if machine == nil {
		return
	}
	state := machine.State()
	body := b.describeUpstreamState(state, machine.ServerReason())
	if body == "" {
		return
	}
	_ = c.sendLine(":turborg-bouncer NOTICE * :" + body)
}

// onUpstreamStateChange is the state-machine subscriber. Broadcasts
// the entry NOTICE to every joined channel × every attached client
// on state-class change (registered ↔ non-registered). Intra-detach
// transitions (e.g. connecting → registering) are suppressed because
// the previous non-registered broadcast already covered the user.
func (b *Bouncer) onUpstreamStateChange(change UpstreamStateChange) {
	enteringRegistered := change.To == UpstreamStateRegistered
	leavingRegistered := change.From == UpstreamStateRegistered && change.To != UpstreamStateRegistered
	// Skip intra-non-registered transitions to keep the channel buffer
	// from filling with "still connecting" repeats.
	if !enteringRegistered && !leavingRegistered {
		return
	}
	var body string
	if enteringRegistered {
		// Only fire on transition FROM a disconnected/paused state —
		// the initial idle→connecting→registering→registered cycle on
		// agent startup would otherwise spam every channel with a
		// "reconnected" NOTICE the user never asked for.
		if change.From == UpstreamStateIdle || change.From == UpstreamStateConnecting ||
			change.From == UpstreamStateRegistering {
			return
		}
		body = "Reconnected to " + b.network() + ". You're back live."
	} else {
		body = b.describeUpstreamState(change.To, change.ServerReason)
		if body == "" {
			return
		}
	}
	b.broadcastChannelNotice(body)
}

// onUpstreamWarn is wired from the connector's escalation watchdog.
// Fires once per transient-outage window when UpstreamWarnAfter has
// elapsed without a successful reconnect — gives the user a
// progressively-stronger signal that the outage is real and ongoing
// rather than a momentary blip.
func (b *Bouncer) onUpstreamWarn(serverReason string, dwell time.Duration) {
	rounded := dwell.Round(time.Minute)
	if rounded <= 0 {
		rounded = dwell.Round(time.Second)
	}
	body := "Network unreachable for over " + rounded.String() +
		" — still retrying."
	if serverReason != "" {
		body += " Last reason: " + serverReason + "."
	}
	b.broadcastChannelNotice(body)
}

// broadcastChannelNotice fans the same service NOTICE into every
// joined channel × every attached client. The double iteration is the
// shape of "tell everyone who's watching every channel they're
// watching it." Single-attached-client clients get one NOTICE per
// channel; multi-attached deployments get the same NOTICE per channel
// per client.
func (b *Bouncer) broadcastChannelNotice(body string) {
	if body == "" || b.state == nil {
		return
	}
	channels := b.state.JoinedChannels()
	for _, info := range channels {
		line := ":turborg-bouncer NOTICE " + info.Name + " :" + body
		b.Broadcast(line, nil)
	}
}

// NotifyJoinFailure broadcasts a per-channel NOTICE to every attached
// client when the supervisor's reconnect-time rejoin of `channel`
// failed for a server-given reason. Called by the connector's
// dispatchLine when it observes 474/475/471/473/476 — the wanted-set
// drop happens in the same flow so the failed channel doesn't haunt
// future reconnects.
func (b *Bouncer) NotifyJoinFailure(channel, reason string) {
	if channel == "" {
		return
	}
	if reason == "" {
		reason = "rejected by network"
	}
	body := "Could not rejoin " + channel + ": " + reason +
		". Removed from auto-join. Use /join " + channel + " to retry."
	line := ":turborg-bouncer NOTICE " + channel + " :" + body
	b.Broadcast(line, nil)
}

// replay ring so a bouncer client that reconnects later sees the
// traffic it missed.
func (b *Bouncer) Broadcast(line string, exclude *BouncerClient) {
	b.recordForReplay(line)
	b.mu.Lock()
	clients := make([]*BouncerClient, 0, len(b.clients))
	for c := range b.clients {
		clients = append(clients, c)
	}
	b.mu.Unlock()
	for _, c := range clients {
		if c == exclude || !c.Authenticated() {
			continue
		}
		if err := c.sendLine(line); err != nil {
			b.mu.Lock()
			delete(b.clients, c)
			b.mu.Unlock()
		}
	}
}

// BroadcastAsSelf prepends the bot's nick!ident@host prefix and fans
// the resulting line to every authenticated bouncer client (modulo
// `exclude`).
//
// We always fan, including for nick-targeted (DM) messages, even to
// clients that didn't negotiate echo-message / znc.in/self-message.
// Rationale: the user's primary IRC client is HexChat 2.16, whose
// auto-cap-request unconditionally skips both self-message caps —
// there's no way to opt in from the client side without manual
// per-network config. Choosing between "DM not visible at all in
// HexChat" and "DM visible under a wrong-named tab" is a UX trade-off,
// and the user has been explicit: visibility wins. Channel messages
// always fan regardless.
//
// Clients that DID negotiate znc.in/self-message additionally get the
// `@znc.in/self-message` IRCv3 tag prefix so their pre-echo-message
// handler routes correctly. Clients without it just see the prefixed
// PRIVMSG line and render however their parser handles a self-prefix.
func (b *Bouncer) BroadcastAsSelf(line string, exclude *BouncerClient) {
	prefix := b.upstreamPrefix()
	if prefix != "" {
		line = prefix + " " + line
	}
	b.recordForReplay(line)

	b.mu.Lock()
	clients := make([]*BouncerClient, 0, len(b.clients))
	for c := range b.clients {
		clients = append(clients, c)
	}
	b.mu.Unlock()
	for _, c := range clients {
		if c == exclude || !c.Authenticated() {
			continue
		}
		toSend := line
		if c.hasCap("znc.in/self-message") {
			toSend = "@znc.in/self-message " + line
		}
		if err := c.sendLine(toSend); err != nil {
			b.mu.Lock()
			delete(b.clients, c)
			b.mu.Unlock()
		}
	}
}

func (b *Bouncer) recordForReplay(line string) {
	if b.state == nil {
		return
	}
	upper := strings.ToUpper(line)
	if !strings.Contains(upper, "PRIVMSG") && !strings.Contains(upper, "NOTICE") {
		return
	}
	msg := Parse(line)
	if msg.Command != CmdPrivmsg && msg.Command != CmdNotice {
		return
	}
	if len(msg.Params) == 0 {
		return
	}
	target := msg.Params[0]
	if !startsWithChannelSigil(target) {
		return
	}
	if b.state.Get(target) == nil {
		return
	}
	b.logMu.Lock()
	defer b.logMu.Unlock()
	bucket := b.channelLog[target]
	bucket = append(bucket, line)
	if len(bucket) > channelLogCap {
		bucket = bucket[len(bucket)-channelLogCap:]
	}
	b.channelLog[target] = bucket
}

func startsWithChannelSigil(s string) bool {
	if s == "" {
		return false
	}
	switch s[0] {
	case '#', '&', '+', '!':
		return true
	}
	return false
}

func peerIP(c *BouncerClient) string {
	if c == nil || c.conn == nil {
		return ""
	}
	if addr, ok := c.conn.RemoteAddr().(*net.TCPAddr); ok {
		return addr.IP.String()
	}
	// Fallback for non-TCP transports (won't happen in production).
	return c.conn.RemoteAddr().String()
}
