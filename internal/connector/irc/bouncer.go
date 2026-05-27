package irc

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/turborg/turborg/internal/messages"
)

// serverTimeLayout is the IRCv3 server-time tag format: ISO8601 UTC
// with millisecond precision. See
// https://ircv3.net/specs/extensions/server-time
const serverTimeLayout = "2006-01-02T15:04:05.000Z"

const (
	// defaultClientPingInterval is how often the bouncer PINGs each attached
	// client. Two jobs: (1) keep an idle connection's bytes flowing so a NAT
	// or proxy idle timeout — e.g. the HAProxy SNI router's tunnel timeout —
	// never reaps a live-but-quiet attachment; (2) detect a dead client
	// within ~one interval instead of leaving a phantom attached. Kept well
	// under typical NAT idle windows (~60–120s).
	defaultClientPingInterval = 60 * time.Second
	// defaultClientPongGrace pads the read deadline past the ping interval so
	// a client has time to answer before silence is treated as a dead
	// connection. The read loop reaps a client that sends nothing (not even a
	// PONG) for pingInterval+pongGrace.
	defaultClientPongGrace = 30 * time.Second
)

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
	// Read-only lookups attached clients expect to work (e.g. /whois in
	// HexChat). Their reply numerics already fan out to clients via
	// shouldFanOutToBouncer; without forwarding the command itself the
	// query never reaches upstream and the client just sees nothing.
	CmdWhois: true,
	CmdWho:   true,
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
	// Require a target so a bare WHOIS/WHO (which would query everything /
	// list all visible users upstream) is dropped rather than forwarded.
	CmdWhois: true,
	CmdWho:   true,
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
	// runCtx is the cancelable context every client handler runs under,
	// captured at Start/StartListenerless and cleared at Stop. The listener
	// path passes it to acceptLoop; the listenerless path hands it to each
	// ServeConn so injected connections share the same lifecycle.
	runCtx context.Context
	wg     sync.WaitGroup

	upstreamMu    sync.RWMutex
	upstreamNick  string
	upstreamIdent string
	upstreamHost  string

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

	// messageStore is the read seam the bouncer consults on attach
	// (state replay) and for CHATHISTORY queries. The runtime wires
	// this to a process-wide messages.Store so the bouncer + WS
	// gateway share the same history view — see runtime.Build. nil =
	// no replay (an unattached test bouncer behaves as a brand-new
	// install with empty history).
	messageStoreMu sync.RWMutex
	messageStore   messages.Store

	// welcomeReplayDepth is the per-channel cap passed to
	// store.Recent on each fresh attach. Overridable via
	// AttachWelcomeReplayDepth; defaults to defaultReplayDepthOnAttach.
	welcomeReplayMu    sync.RWMutex
	welcomeReplayDepth int

	// keepalive timings for the server→client PING that holds idle
	// attachments open and reaps dead ones. Defaults set in NewBouncer;
	// override (e.g. short intervals in tests) via AttachClientKeepalive.
	// A non-positive pingInterval disables the keepalive entirely, restoring
	// the plain block-forever read loop.
	keepaliveMu  sync.RWMutex
	pingInterval time.Duration
	pongGrace    time.Duration
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
		upstreamHost: "turborg",
		pingInterval: defaultClientPingInterval,
		pongGrace:    defaultClientPongGrace,
	}, nil
}

// AttachClientKeepalive overrides the server→client PING interval and the
// pong grace period. A non-positive interval disables the keepalive (the
// read loop blocks forever, as it did before keepalive existed). Call
// before Start; tests use it to drive the loop on millisecond timings.
func (b *Bouncer) AttachClientKeepalive(interval, grace time.Duration) {
	b.keepaliveMu.Lock()
	defer b.keepaliveMu.Unlock()
	b.pingInterval = interval
	b.pongGrace = grace
}

func (b *Bouncer) clientKeepalive() (interval, grace time.Duration) {
	b.keepaliveMu.RLock()
	defer b.keepaliveMu.RUnlock()
	return b.pingInterval, b.pongGrace
}

// AttachMessageStore wires the bouncer to a shared messages.Store.
// Replay-on-attach and CHATHISTORY both query through this seam. nil
// disables replay; the bouncer still serves live traffic normally.
func (b *Bouncer) AttachMessageStore(s messages.Store) {
	b.messageStoreMu.Lock()
	defer b.messageStoreMu.Unlock()
	b.messageStore = s
}

func (b *Bouncer) currentMessageStore() messages.Store {
	b.messageStoreMu.RLock()
	defer b.messageStoreMu.RUnlock()
	return b.messageStore
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
	return b.start(ctx, true)
}

// StartListenerless brings the bouncer up without binding a TCP listener:
// connections are delivered by the caller via ServeConn instead of an accept
// loop. This is the pooled-runtime path — one turborg-server process fronts
// every tenant behind a single SNI/PROXY-v2 router, so per-tenant bouncers
// must not each bind a port. Dedicated mode still uses Start (own host port).
// Everything past the listener — auth, replay, state surfacing, keepalive —
// is identical across both, so the bouncer behaviour is one source of truth.
func (b *Bouncer) StartListenerless(ctx context.Context) error {
	return b.start(ctx, false)
}

func (b *Bouncer) start(ctx context.Context, listen bool) error {
	var l net.Listener
	if listen {
		addr := fmt.Sprintf("%s:%d", b.host, b.port)
		var err error
		l, err = net.Listen("tcp", addr)
		if err != nil {
			return fmt.Errorf("bouncer listen %s: %w", addr, err)
		}
	}

	bctx, cancel := context.WithCancel(ctx)

	b.mu.Lock()
	b.listener = l
	b.cancel = cancel
	b.runCtx = bctx
	machine := b.machine
	b.mu.Unlock()

	if machine != nil {
		sub := machine.Subscribe(b.onUpstreamStateChange)
		b.mu.Lock()
		b.stateSub = sub
		b.mu.Unlock()
	}

	if listen {
		b.wg.Add(1)
		go b.acceptLoop(bctx, l)
		b.log.Info("bouncer listening", "host", b.host, "port", b.port)
	} else {
		b.log.Info("bouncer ready (listenerless; served via ServeConn)")
	}
	return nil
}

// ServeConn handles one already-accepted client connection instead of one
// taken from the bouncer's own listener — the pooled router calls this after
// reading the PROXY-v2 header and resolving which tenant the connection is
// for. The bouncer must have been brought up first (Start or
// StartListenerless). Blocks until the client disconnects, so callers run it
// per-connection in their own goroutine. A nil run context (never started, or
// already stopped) closes the connection rather than leaking it.
func (b *Bouncer) ServeConn(conn net.Conn) {
	b.mu.Lock()
	ctx := b.runCtx
	b.mu.Unlock()
	if ctx == nil {
		_ = conn.Close()
		return
	}

	client := newBouncerClient(conn, b.log)
	b.mu.Lock()
	b.clients[client] = struct{}{}
	b.mu.Unlock()

	b.wg.Add(1)
	b.handleClient(ctx, client)
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
	b.runCtx = nil
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

	// Keepalive: PING the client on an interval so an idle attachment keeps
	// bytes flowing (no NAT/proxy idle reap) and a dead one is detected. The
	// read deadline below is the reaper — any inbound line, the client's PONG
	// included, resets it; pingInterval+pongGrace of total silence ends the
	// loop. A non-positive interval disables both, keeping the old behaviour.
	interval, grace := b.clientKeepalive()
	if interval > 0 {
		pingCtx, stopPing := context.WithCancel(ctx)
		defer stopPing()
		b.wg.Add(1)
		go b.pingClientLoop(pingCtx, client, interval)
	}

	for {
		if ctx.Err() != nil {
			return
		}
		if interval > 0 {
			// Errors here only mean the conn is already gone; the readLine
			// below will surface it, so don't special-case the deadline set.
			_ = client.conn.SetReadDeadline(time.Now().Add(interval + grace))
		}
		line, err := client.readLine()
		if err != nil {
			return
		}
		b.handleLine(client, line)
	}
}

// pingClientLoop sends a server PING to one attached client every interval
// until ctx is cancelled (the client's handleClient returned, or the bouncer
// is stopping). The PONG it provokes — like any inbound line — resets the
// read deadline in handleClient, so a live client stays attached while a dead
// one trips the deadline and gets reaped. A send error means the conn is
// already broken; the read side will observe it and clean up, so the loop
// just exits.
func (b *Bouncer) pingClientLoop(ctx context.Context, client *BouncerClient, interval time.Duration) {
	defer b.wg.Done()
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			token := fmt.Sprintf("tb-%d", time.Now().UnixNano())
			if err := client.sendLine(CmdPing + " :" + token); err != nil {
				return
			}
		}
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
	case CmdPong:
		// The client's reply to our keepalive PING (pingClientLoop). It has
		// already done its job — receiving any line reset the read deadline
		// in handleClient — so just consume it. Never forward upstream.
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

	// CHATHISTORY is a bouncer-local query against messages.Store —
	// never forwarded upstream, handled regardless of upstream-state
	// (clients can ask for scrollback even while reconnecting). Must
	// run BEFORE the forwardable filter.
	if msg.Command == CmdChathistory {
		b.handleChathistory(client, msg)
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
//   - batch: framework cap for grouping a sequence of lines under a
//     server-generated batch id. The bouncer uses this to wrap
//     channel-log replay in a `chathistory`-typed batch so capable
//     clients render the block as historical instead of highlighting
//     each line as fresh activity. Clients that didn't negotiate it
//     get the legacy NOTICE-bracketed replay.
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
	"batch":               true,
	// draft/chathistory advertises that the bouncer answers
	// CHATHISTORY queries against the configured messages.Store.
	// Clients that negotiate this cap learn they can scroll back past
	// the welcome replay depth (the 200-msg cap above) by issuing
	// CHATHISTORY BEFORE / LATEST against any joined channel.
	"draft/chathistory": true,
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
	// Discovery hint for the *turborg service buffer — most clients
	// render this in the server status tab on attach. First-time
	// users learn that policy denials + queued-action confirmations
	// arrive in a query buffer from the virtual `*turborg` nick.
	_ = client.sendLine(":turborg-bouncer NOTICE * :System messages from this bouncer arrive " +
		"in the " + serviceNick + " query tab — open it for policy denials, queued actions, " +
		"and similar meta-info.")
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

// defaultReplayDepthOnAttach pins the welcome-replay depth when the
// operator hasn't overridden it via AttachWelcomeReplayDepth.
// Matches the historical 200/channel ring used before the store
// seam landed — change the value at the runtime boundary, not here.
const defaultReplayDepthOnAttach = 200

// AttachWelcomeReplayDepth overrides how many recent messages per
// joined channel the bouncer ships to a freshly-attached client.
// Larger values let HexChat-class clients (no CHATHISTORY support)
// see a deeper backfill at the cost of a slower welcome on every
// reconnect. Pass 0 to use the default.
func (b *Bouncer) AttachWelcomeReplayDepth(n int) {
	b.welcomeReplayMu.Lock()
	defer b.welcomeReplayMu.Unlock()
	if n <= 0 {
		n = defaultReplayDepthOnAttach
	}
	b.welcomeReplayDepth = n
}

func (b *Bouncer) currentWelcomeReplayDepth() int {
	b.welcomeReplayMu.RLock()
	defer b.welcomeReplayMu.RUnlock()
	if b.welcomeReplayDepth <= 0 {
		return defaultReplayDepthOnAttach
	}
	return b.welcomeReplayDepth
}

func (b *Bouncer) replayChannelLogs(client *BouncerClient) {
	store := b.currentMessageStore()
	if store == nil || b.state == nil {
		return
	}
	b.upstreamMu.RLock()
	hasUpstream := b.upstreamNick != ""
	b.upstreamMu.RUnlock()
	if !hasUpstream {
		return
	}

	tagTime := client.hasCap("server-time")
	useBatch := client.hasCap("batch")
	depth := b.currentWelcomeReplayDepth()

	for _, info := range b.state.JoinedChannels() {
		msgs, err := store.Recent(context.Background(), info.Name, time.Time{}, depth)
		if err != nil || len(msgs) == 0 {
			continue
		}
		// store.Recent returns newest-first; replay is chronological
		// so the client sees the conversation in the order it
		// happened.
		reverseMessages(msgs)
		b.writeReplayBatch(client, info.Name, msgs, tagTime, useBatch)
	}
}

// writeReplayBatch emits one channel's worth of historical messages
// using the cap-aware framing (BATCH for batch-capable clients,
// NOTICE markers for everyone else). Each message is decorated with
// the IRCv3 tags the client negotiated.
func (b *Bouncer) writeReplayBatch(client *BouncerClient, channel string, msgs []messages.Message, tagTime, useBatch bool) {
	var batchID string
	if useBatch {
		batchID = newBatchID()
		_ = client.sendLine(fmt.Sprintf("BATCH +%s chathistory %s", batchID, channel))
	} else {
		_ = client.sendLine(fmt.Sprintf(
			":turborg-bouncer NOTICE %s :--- buffer playback for %s (%d lines) ---",
			channel, channel, len(msgs),
		))
	}
	for _, m := range msgs {
		line := b.formatMessageForReplay(m)
		if err := client.sendLine(decorateReplayLine(loggedLine{line: line, ts: m.TS}, tagTime, batchID)); err != nil {
			return
		}
	}
	if useBatch {
		_ = client.sendLine("BATCH -" + batchID)
	} else {
		_ = client.sendLine(fmt.Sprintf(
			":turborg-bouncer NOTICE %s :--- end of buffer ---", channel,
		))
	}
}

// formatMessageForReplay reconstructs an IRC PRIVMSG wire line from a
// stored messages.Message. The prefix uses the bouncer's known
// upstream identity for self-messages (so echo-message-aware clients
// match it against the prefix they learned at attach) and a synthetic
// host for other senders — the receiving client only renders the nick
// portion anyway, and we don't store ident/host across the wire.
func (b *Bouncer) formatMessageForReplay(m messages.Message) string {
	b.upstreamMu.RLock()
	selfNick := b.upstreamNick
	selfIdent := b.upstreamIdent
	selfHost := b.upstreamHost
	b.upstreamMu.RUnlock()

	var prefix string
	if m.Nick == selfNick && selfNick != "" {
		prefix = fmt.Sprintf(":%s!%s@%s", selfNick, selfIdent, selfHost)
	} else {
		// Synthetic ident@host — clients don't render this visibly
		// for non-self messages. A future PR can extend the wire
		// contract with ident/host preservation if needed.
		prefix = fmt.Sprintf(":%s!~user@upstream", m.Nick)
	}
	return fmt.Sprintf("%s PRIVMSG %s :%s", prefix, m.Channel, m.Text)
}

// loggedLine survives as the input shape decorateReplayLine accepts —
// keeping the helper signature stable across the in-memory ring
// removal so the cap-merging tests in internal_test.go continue to
// exercise the same code path the replay loop hits.
type loggedLine struct {
	line string
	ts   time.Time
}

// reverseMessages flips a slice in place so a newest-first store
// reply renders oldest-first in the replay.
func reverseMessages(msgs []messages.Message) {
	for i, j := 0, len(msgs)-1; i < j; i, j = i+1, j-1 {
		msgs[i], msgs[j] = msgs[j], msgs[i]
	}
}

// decorateReplayLine builds the wire form of a replay line for the
// attached client's negotiated caps:
//
//   - server-time: prepend `time=<ISO8601 UTC>` so HexChat / mIRC /
//     irssi render the line as historical and don't highlight the
//     channel tab. Upstream's own `@time=` (when the line already
//     carries tags) takes precedence — we don't second-guess what the
//     network declared the message arrival time was.
//   - batch:       prepend `batch=<id>` so the line is associated with
//     the surrounding `BATCH +<id> chathistory <channel>` envelope.
//
// When neither cap was negotiated, the line passes through unchanged —
// preserving today's behavior for clients on legacy stacks.
func decorateReplayLine(entry loggedLine, tagTime bool, batchID string) string {
	line := entry.line
	hasExistingTags := strings.HasPrefix(line, "@")

	var added []string
	if batchID != "" {
		added = append(added, "batch="+batchID)
	}
	if tagTime && !hasExistingTags {
		added = append(added, "time="+entry.ts.UTC().Format(serverTimeLayout))
	}
	if len(added) == 0 {
		return line
	}

	if !hasExistingTags {
		return "@" + strings.Join(added, ";") + " " + line
	}
	// Merge: keep upstream's tags as-is, prepend ours. IRCv3 message-tags
	// allows duplicate keys ordering-wise, but to be conservative we
	// place ours first and let upstream's win on conflict (clients that
	// parse left-to-right will see upstream's value last and store it).
	sp := strings.IndexByte(line, ' ')
	if sp < 0 {
		// Malformed (tags without a body) — pass through.
		return line
	}
	existing := line[1:sp]
	rest := line[sp+1:]
	return "@" + strings.Join(added, ";") + ";" + existing + " " + rest
}

// newBatchID returns a short, sufficiently-unique batch reference tag
// per the IRCv3 batch spec. 8 hex chars (32 bits) easily clears the
// per-attach collision bar — there are at most a few hundred channels
// in flight at once for one bouncer client.
func newBatchID() string {
	var b [4]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
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
		// Per-command NOTICE routing — the goal is to put the
		// rejection in the buffer the user was looking at when they
		// typed the command, not the server status tab they aren't.
		//
		//   - JOIN denial: the user typed /join #foo from somewhere
		//     (an existing channel, the server tab, wherever). The
		//     target channel is where their attention will land once
		//     their client opens that tab on receiving any NOTICE
		//     addressed to it. Route the NOTICE to msg.Params[0]
		//     (first of any comma-list) so the JOIN-attempt tab
		//     surfaces the rejection inline.
		//
		//   - NICK denial: nick changes are global, with no channel
		//     context. Broadcast the NOTICE to every joined channel
		//     so the user sees it wherever they happen to be looking.
		//     Fall back to statusTarget when no channels are joined.
		//
		//   - USER denial: this only fires during pre-registration
		//     /USER which has no channel context. Keep statusTarget.
		b.notifyPolicyDenialForCommand(client, msg, reason)
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

// notifyPolicyDenialForCommand routes a policy-denial NOTICE per
// command. JOIN / NICK / USER all surface as PRIVMSGs from a virtual
// *turborg service nick — IRC clients open a dedicated query buffer
// for it, so every bouncer-meta message lives in one predictable tab
// rather than spamming real channel buffers or fake-opening a tab
// the user never wanted (the previous "JOIN denial → channel-targeted
// NOTICE" pattern opened a #foo tab when the user's /join was
// rejected, which is wrong UX).
//
// What stays channel-targeted (NOT routed through *turborg):
//   - Rate-limited PRIVMSG → channel-targeted NOTICE inline with the
//     user's attempt (allowByPolicy's throttle branch). Feedback at
//     the typing site is the right UX here.
//   - Per-channel rejoin failures → channel-targeted NOTICE
//     (Bouncer.NotifyJoinFailure). Channel-scoped failure naturally
//     belongs in the channel buffer.
//   - Upstream state transitions → broadcast to every joined channel
//     (onUpstreamStateChange). Outage signal is too important to bury
//     in a tab the user might have closed.
func (b *Bouncer) notifyPolicyDenialForCommand(client *BouncerClient, msg Message, baseReason string) {
	switch msg.Command {
	case CmdJoin:
		target := firstJoinTarget(msg)
		body := "Channel cap reached — NOT joined."
		if target != "" {
			body = "Channel cap reached — NOT joined " + target +
				". /part another channel first, or raise the operator policy limit."
		}
		b.notifyService(client, body)
	case CmdNick:
		newNick := msg.Trailing
		if newNick == "" && len(msg.Params) > 0 {
			newNick = msg.Params[0]
		}
		newNick = strings.TrimSpace(newNick)
		body := "Nick change rejected — locked by operator policy."
		if newNick != "" {
			body = "Nick change to " + newNick + " rejected — locked by operator policy."
		}
		b.notifyService(client, body)
	default:
		b.notifyService(client, baseReason)
	}
}

// firstJoinTarget pulls the first channel name out of a JOIN line —
// handles both the comma-list form (`JOIN #a,#b key1,key2`) and the
// single-target form. Returns "" when the message carries no channel
// param at all.
func firstJoinTarget(msg Message) string {
	target := ""
	if len(msg.Params) > 0 {
		target = msg.Params[0]
	}
	if target == "" {
		target = msg.Trailing
	}
	channels := splitAndTrim(target, ",")
	if len(channels) == 0 {
		return ""
	}
	return channels[0]
}

// serviceNick is the virtual-user nickname the bouncer uses as the
// source of meta-conversation PRIVMSGs (policy denials, queued-
// action acknowledgements, future interactive commands). The
// asterisk prefix is the well-established convention from ZNC's
// `*status` and soju's `BouncerServ` for service-internal sources —
// most IRC clients render the resulting query buffer as a "system"
// tab and don't try to whois the sender.
const serviceNick = "*turborg"

// notifyService delivers a meta-conversation message to a single
// attached client by wrapping it in a PRIVMSG from the *turborg
// virtual user, addressed to the bot's own nick (which is also what
// the attached client knows as its own nick). IRC clients open a
// dedicated query buffer for *turborg the first time one of these
// arrives — everything bouncer-meta then accumulates there instead
// of polluting real channel buffers.
//
// Falls back to a NOTICE-to-status when the bouncer hasn't learned
// the bot's nick yet (pre-Dial / mid-registration), since a PRIVMSG
// with no resolvable target nick would be confusing to clients.
func (b *Bouncer) notifyService(client *BouncerClient, body string) {
	target := b.statusTarget()
	if target == "" || target == "*" {
		b.notifyPolicyDenial(client, "*", body)
		return
	}
	line := ":" + serviceNick + "!turborg@bouncer PRIVMSG " + target + " :" + body
	if err := client.sendLine(line); err != nil {
		b.log.Debug("bouncer service notice", "err", err)
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
// supervisor's next reconnect rejoins them, then surfaces one service-
// buffer line per channel with what was queued. Channel keys from the
// JOIN line are preserved.
//
// Routes through *turborg rather than addressing the about-to-be-
// joined channel directly: opening a #foo tab before the JOIN has
// actually succeeded is the same fake-tab UX bug that the policy-
// denial routing already rejects.
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
		b.notifyService(client, body)
	}
}

// handleDetachedPart removes the channel(s) from the wanted-set so
// the supervisor doesn't silently rejoin them on the next reconnect,
// then surfaces one service-buffer line per channel. Routes through
// *turborg rather than the parted channel itself — a "you removed
// this" message addressed to the channel the user is REMOVING is
// confusing UX.
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
		b.notifyService(client, body)
	}
}

// handleDetachedNick queues the requested nick for the next
// registration via the connector's preferred-nick hook (when wired)
// and surfaces a service-buffer line. Doesn't fake a NICK echo —
// that would lie about the bot's identity during the outage.
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
	b.notifyService(client, body)
}

// handleDetachedRefusal handles every forwardable command that can't
// be queued and replayed: PRIVMSG / NOTICE / MODE / TOPIC / KICK /
// NAMES.
//
// PRIVMSG and NOTICE keep their channel-targeted NOTICE — same logic
// as the rate-limited-PRIVMSG path: the user typed in that channel
// buffer, so the rejection lands inline with the attempt where
// they're already looking.
//
// MODE / TOPIC / KICK / NAMES route through *turborg. They're admin-
// style commands with no inline-with-attempt UX win, and grouping
// them in the service buffer keeps real channel logs clean.
func (b *Bouncer) handleDetachedRefusal(client *BouncerClient, msg Message) {
	state := b.upstreamState()
	body := b.detachedRejectBody(state, msg.Command)
	switch msg.Command {
	case CmdPrivmsg, CmdNotice:
		target := b.statusTarget()
		if len(msg.Params) > 0 {
			target = msg.Params[0]
		}
		if err := client.sendLine(":turborg-bouncer NOTICE " + target + " :" + body); err != nil {
			b.log.Debug("bouncer detached reject", "err", err)
		}
	default:
		b.notifyService(client, body)
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

// describeUpstreamState is a thin wrapper that threads the bouncer's
// network name into the package-level DescribeUpstreamState helper so
// the IRC bouncer + WS gateway render identical bodies. Empty return
// = no NOTICE for this state (registered is the live happy path and
// needs no surfacing).
func (b *Bouncer) describeUpstreamState(state UpstreamState, serverReason string) string {
	return DescribeUpstreamState(state, b.network(), serverReason)
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
// joined channel × every attached client. The double iteration is
// the shape of "tell everyone who's watching every channel they're
// watching it." Single-attached-client clients get one NOTICE per
// channel; multi-attached deployments get the same NOTICE per
// channel per client.
//
// Two additional surfaces:
//   - When no channels are joined (fresh container, all-PART'd
//     session), the channel-broadcast loop is a silent no-op and the
//     attached client would miss the state change entirely. Fall back
//     to a service-buffer message per attached client so the
//     disconnect/reconnect/banned signal still reaches them.
//   - Whether or not channels were broadcast to, also write one copy
//     to each attached client's service buffer. That gives users a
//     chronological log of state transitions in *turborg ("09:15
//     disconnected; 09:16 reconnected") — useful for "did I miss
//     anything?" debugging without scrolling through channel buffers.
func (b *Bouncer) broadcastChannelNotice(body string) {
	if body == "" {
		return
	}
	channels := []*ChannelInfo(nil)
	if b.state != nil {
		channels = b.state.JoinedChannels()
	}
	for _, info := range channels {
		line := ":turborg-bouncer NOTICE " + info.Name + " :" + body
		b.Broadcast(line, nil)
	}
	b.broadcastServiceAudit(body)
}

// broadcastServiceAudit writes one *turborg service-buffer line per
// attached authenticated client. Used by the upstream-state
// broadcaster as the always-reaches-the-user fallback (when no
// channels are joined) and as an audit-log copy alongside the
// channel broadcast.
func (b *Bouncer) broadcastServiceAudit(body string) {
	if body == "" {
		return
	}
	b.mu.Lock()
	clients := make([]*BouncerClient, 0, len(b.clients))
	for c := range b.clients {
		clients = append(clients, c)
	}
	b.mu.Unlock()
	for _, c := range clients {
		if !c.Authenticated() {
			continue
		}
		b.notifyService(c, body)
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

// Broadcast fans line to every authenticated bouncer client except
// exclude. Recording for replay is no longer the bouncer's concern —
// the EventBus subscriber wired in runtime.Build feeds the shared
// messages.Store so the bouncer + WS gateway see one canonical history.
func (b *Bouncer) Broadcast(line string, exclude *BouncerClient) {
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
