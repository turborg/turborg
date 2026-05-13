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

	authMu sync.Mutex
	auth   bool
}

func newBouncerClient(conn net.Conn) *BouncerClient {
	return &BouncerClient{conn: conn, reader: bufio.NewReader(conn)}
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

func (b *BouncerClient) Address() string {
	if b.conn == nil {
		return ""
	}
	return b.conn.RemoteAddr().String()
}

func (b *BouncerClient) sendLine(line string) error {
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
		client := newBouncerClient(conn)
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
	}

	if !client.Authenticated() {
		if msg.Command == CmdCap {
			b.handleCap(client, msg)
		}
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

	if send == nil {
		return
	}
	if err := send(line); err != nil {
		b.log.Debug("bouncer forward upstream", "err", err)
		return
	}
	if msg.Command == CmdPrivmsg || msg.Command == CmdNotice {
		// Server doesn't echo PRIVMSG / NOTICE back; broadcast to other
		// bouncer clients ourselves.
		b.BroadcastAsSelf(line, client)
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

func (b *Bouncer) handleCap(client *BouncerClient, msg Message) {
	sub := ""
	if len(msg.Params) > 0 {
		sub = strings.ToUpper(msg.Params[0])
	}
	switch sub {
	case "LS":
		_ = client.sendLine(":turborg-bouncer CAP * LS :")
	case "REQ":
		requested := msg.Trailing
		if requested == "" && len(msg.Params) > 1 {
			requested = strings.Join(msg.Params[1:], " ")
		}
		_ = client.sendLine(":turborg-bouncer CAP * NAK :" + requested)
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

		b.upstreamMu.RLock()
		target := b.upstreamNick
		b.upstreamMu.RUnlock()
		if target == "" {
			target = "*"
		}
		_ = client.sendLine(fmt.Sprintf(":turborg-bouncer 001 %s :Welcome to the turborg bouncer", target))
		b.replayState(client)
		b.replayChannelLogs(client)
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

// BroadcastAsSelf prepends the bot's nick!ident@host prefix and
// broadcasts. Used so other bouncer clients see traffic the bot (or one
// of them) sent — IRC servers don't echo own messages back.
func (b *Bouncer) BroadcastAsSelf(line string, exclude *BouncerClient) {
	if prefix := b.upstreamPrefix(); prefix != "" {
		line = prefix + " " + line
	}
	b.Broadcast(line, exclude)
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

