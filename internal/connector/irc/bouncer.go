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

func (b *Bouncer) AttachOutboundObserver(cb ForwardedObserver) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.onForwarded = cb
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
	b.mu.Unlock()

	bctx, cancel := context.WithCancel(ctx)
	b.cancel = cancel

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
	clients := make([]*BouncerClient, 0, len(b.clients))
	for c := range b.clients {
		clients = append(clients, c)
	}
	b.clients = map[*BouncerClient]struct{}{}
	b.listener = nil
	b.cancel = nil
	b.mu.Unlock()

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
		return
	}
	if msg.Command == CmdPrivmsg || msg.Command == CmdNotice {
		// Server doesn't echo PRIVMSG / NOTICE back; broadcast to other
		// bouncer clients ourselves. If the originator negotiated
		// echo-message (IRCv3), fan to them too — that's what the cap
		// promises: "your own messages will come back to you so you
		// can render them through the same incoming-message path that
		// every other client uses." Without the cap, exclude the
		// originator to avoid double-display.
		var exclude *BouncerClient
		if !client.hasCap("echo-message") {
			exclude = client
		}
		b.BroadcastAsSelf(line, exclude)
		if observer != nil && len(msg.Params) > 0 {
			b.upstreamMu.RLock()
			sender := b.upstreamNick
			b.upstreamMu.RUnlock()
			if sender == "" {
				sender = "*"
			}
			observer(msg.Params[0], sender, msg.Trailing, msg.Command)
		}
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

// sendWelcome emits the 001 welcome + state replay + channel-log
// replay for a freshly-authenticated client. Called either inline
// from handlePass (no CAP negotiation in progress) or from CAP END
// (client was negotiating caps when PASS authenticated).
func (b *Bouncer) sendWelcome(client *BouncerClient) {
	b.upstreamMu.RLock()
	target := b.upstreamNick
	b.upstreamMu.RUnlock()
	if target == "" {
		target = "*"
	}
	_ = client.sendLine(fmt.Sprintf(":turborg-bouncer 001 %s :Welcome to the turborg bouncer", target))
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
// the count argument.
func (b *Bouncer) allowByPolicy(client *BouncerClient, msg Message) bool {
	b.mu.Lock()
	limits := b.limits
	b.mu.Unlock()

	currentChannels := 0
	if msg.Command == CmdJoin && b.state != nil {
		currentChannels = b.state.Count()
	}
	allow, reason := limits.AllowCommand(msg.Command, currentChannels)
	if !allow {
		b.notifyPolicyDenial(client, reason)
	}
	return allow
}

// notifyPolicyDenial sends a NOTICE back to the originating client when
// an operator-policy gate refuses one of its commands. Uses the same
// synthetic prefix the bouncer already uses for PONG so existing IRC
// clients render it in the network/status tab rather than treating it
// as a channel message. Errors are logged at debug level — a failed
// NOTICE shouldn't fail the surrounding handler.
func (b *Bouncer) notifyPolicyDenial(client *BouncerClient, reason string) {
	line := ":turborg-bouncer NOTICE * :" + reason
	if err := client.sendLine(line); err != nil {
		b.log.Debug("bouncer policy notice", "err", err)
	}
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
