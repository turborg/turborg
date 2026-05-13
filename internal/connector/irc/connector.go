package irc

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/turborg/turborg/internal/agent"
	"golang.org/x/sync/errgroup"
)

// Connector is the IRC adapter: TLS dial + IRCv3 CAP / SASL handshake,
// supervised read+ping loop, channel-state cache, optional bouncer.
//
// Lifecycle (mirrors Python connectors/irc/connector.py):
//   - Start: open the connection, run CAP/SASL/USER/NICK, await
//     RPL_ENDOFMOTD or ERR_NOMOTD, JOIN configured channels, send
//     NickServ IDENTIFY if configured, start the bouncer if configured.
//   - Run: long-lived supervised loop owned by Agent's errgroup. Reader
//     + dispatcher, all unwound on ctx cancel via SetReadDeadline.
//   - Stop: send QUIT, close the upstream, stop the bouncer. Idempotent.
type Connector struct {
	settings *Settings
	log      *slog.Logger
	events   *agent.EventBus
	client   *Client
	inbox    chan *agent.InboundEnvelope

	state    *ChannelState
	bouncer  *Bouncer
	ctcp     *Throttle

	// currentNick is the live nick the server confirmed for us. It
	// starts as settings.Nick (the requested value), gets updated by
	// the 001 RPL_WELCOME (where the server tells us the nick it
	// actually assigned — may differ if it had to truncate / disambig),
	// and again on any observed self-NICK change. Everything that
	// surfaces "the bot's nick" outside the connector (bouncer
	// state replay, web state op, web MESSAGE_SENT.sender) reads it
	// through CurrentNick so the displays stay in sync.
	nickMu      sync.RWMutex
	currentNick string

	stopOnce sync.Once
}

// New constructs an IRC Connector. Pass nil for events when the agent is
// not in the loop (e.g. standalone tests that don't care about lifecycle
// events).
func New(s *Settings, log *slog.Logger, events *agent.EventBus) *Connector {
	if log == nil {
		log = slog.Default()
	}
	return &Connector{
		settings:    s,
		log:         log,
		events:      events,
		inbox:       make(chan *agent.InboundEnvelope, 64),
		state:       NewChannelState(),
		currentNick: s.Nick,
	}
}

func (c *Connector) Name() string                          { return "irc" }
func (c *Connector) Inbound() <-chan *agent.InboundEnvelope { return c.inbox }
func (c *Connector) ClaimSupervision() bool                { return true }
func (c *Connector) State() *ChannelState                  { return c.state }

// CurrentNick returns the live nick the server confirmed for the bot.
// Initially the requested TURBORG_IRC_NICK, then overwritten by the
// 001 welcome's target field (the nick the server actually assigned)
// and by any observed self-NICK change.
func (c *Connector) CurrentNick() string {
	c.nickMu.RLock()
	defer c.nickMu.RUnlock()
	return c.currentNick
}

// setCurrentNick updates the live nick and propagates the change to
// the bouncer's upstreamNick so its synthetic JOIN prefixes + the
// observer's "sender" field stay in sync. Idempotent — calling with
// the same value is a no-op.
func (c *Connector) setCurrentNick(nick string) {
	if nick == "" {
		return
	}
	c.nickMu.Lock()
	changed := c.currentNick != nick
	c.currentNick = nick
	c.nickMu.Unlock()
	if changed && c.bouncer != nil {
		c.bouncer.UpdateUpstreamNick(nick)
	}
}

// SendRaw writes a raw IRC line directly to the upstream socket. The
// web gateway uses this to forward client→server ops (JOIN, PART, NICK,
// WHOIS, …) that don't fit the Envelope model.
func (c *Connector) SendRaw(line string) error {
	if c.client == nil {
		return errors.New("irc: not connected")
	}
	return c.client.WriteLine(line)
}

func (c *Connector) Send(env *agent.OutboundEnvelope) error {
	if c.client == nil {
		return errors.New("irc: not connected")
	}
	line := fmt.Sprintf("%s %s :%s", CmdPrivmsg, env.Channel, env.Text)
	// "irc >>" log line fires from Client.WriteLine for every outbound
	// write — no per-callsite duplication.
	if err := c.client.WriteLine(line); err != nil {
		return err
	}
	if c.bouncer != nil {
		c.bouncer.BroadcastAsSelf(line, nil)
	}
	return nil
}

// Start opens the TCP/TLS connection and completes the IRCv3 handshake,
// returning only when RPL_ENDOFMOTD or ERR_NOMOTD has arrived (or the
// handshake-timeout elapses).
//
// If the bouncer is enabled, its TCP listener is bound BEFORE the
// upstream Dial — this way HexChat / irssi-style clients can connect
// to the bouncer immediately on bot startup instead of seeing
// connection-refused during the ~6s upstream-handshake window. Pre-
// bound clients see an empty initial state on auth; once the upstream
// handshake finishes and the bot starts observing JOIN echoes, those
// JOIN lines fan to the connected client and the channel state
// catches up live.
func (c *Connector) Start(ctx context.Context) error {
	if c.settings.BouncerEnabled() {
		if err := c.startBouncer(ctx); err != nil {
			return err
		}
	}

	c.log.Info("irc connecting",
		"host", c.settings.Hostname,
		"port", c.settings.Port,
		"tls", c.settings.UseTLS,
		"nick", c.settings.Nick,
	)
	cli, err := Dial(ctx, c.settings.Hostname, c.settings.Port, c.settings.UseTLS)
	if err != nil {
		return err
	}
	cli.SetLog(c.log)
	c.client = cli

	if err := c.register(ctx); err != nil {
		_ = cli.Close()
		return err
	}
	if err := c.awaitHandshake(ctx); err != nil {
		_ = cli.Close()
		return err
	}
	c.log.Info("irc handshake complete", "nick", c.settings.Nick)
	for _, ch := range c.settings.NormalizedChannels() {
		c.log.Info("irc joining channel", "channel", ch)
		if err := c.client.WriteLine(CmdJoin + " " + ch); err != nil {
			_ = cli.Close()
			return fmt.Errorf("irc JOIN %s: %w", ch, err)
		}
	}
	if c.settings.NickServPassword != "" {
		c.log.Info("irc identifying with NickServ")
		if err := c.client.WriteLine(
			fmt.Sprintf("%s NickServ :IDENTIFY %s", CmdPrivmsg, c.settings.NickServPassword),
		); err != nil {
			_ = cli.Close()
			return fmt.Errorf("irc NickServ IDENTIFY: %w", err)
		}
	}

	if c.settings.CTCPMaxPerWindow > 0 && c.settings.CTCPWindowSeconds > 0 {
		tr, err := NewThrottle(
			c.settings.CTCPMaxPerWindow,
			time.Duration(c.settings.CTCPWindowSeconds)*time.Second,
			nil,
		)
		if err != nil {
			_ = cli.Close()
			return fmt.Errorf("irc CTCP throttle: %w", err)
		}
		c.ctcp = tr
	}

	// Bouncer was pre-bound at the top of Start; nothing to do here —
	// its sendUpstream closure references c.client which is now set,
	// so forwarded commands from clients work transparently.

	c.publish(ctx, agent.Event{
		Type: agent.EventReady,
		Fields: map[string]any{
			"connector": c.Name(),
			"nick":      c.settings.Nick,
		},
	})
	return nil
}

func (c *Connector) startBouncer(ctx context.Context) error {
	var rl *RateLimiter
	if c.settings.BouncerRatelimitEnabled {
		got, err := NewRateLimiter(
			c.settings.BouncerMaxFailedAttempts,
			time.Duration(c.settings.BouncerFailureWindowSeconds)*time.Second,
			time.Duration(c.settings.BouncerLockoutSeconds)*time.Second,
			nil,
		)
		if err != nil {
			return fmt.Errorf("bouncer ratelimit: %w", err)
		}
		rl = got
	}
	b, err := NewBouncer(
		c.settings.BouncerPassword,
		c.settings.BouncerHost,
		c.settings.BouncerPort,
		rl,
		c.log,
	)
	if err != nil {
		return err
	}
	b.AttachState(c.state, c.CurrentNick(), c.settings.EffectiveUsername(), "turborg")
	// sendUpstream is set before c.client is, so guard the dereference.
	// Pre-bound bouncer clients that try to send forwardable commands
	// before the upstream Dial completes get an error back rather than
	// a nil-deref panic. Once Connector.Start has set c.client, sends
	// go through transparently.
	b.AttachUpstream(func(line string) error {
		if c.client == nil {
			return errors.New("irc: upstream not yet connected; please retry")
		}
		return c.client.WriteLine(line)
	})
	if c.events != nil {
		b.AttachOutboundObserver(func(channel, sender, text, kind string) {
			c.publish(ctx, agent.Event{
				Type: agent.EventMessageSent,
				Fields: map[string]any{
					"connector": c.Name(),
					"channel":   channel,
					"sender":    sender,
					"text":      text,
					"kind":      kind,
				},
			})
		})
	}
	if err := b.Start(ctx); err != nil {
		return err
	}
	c.bouncer = b
	return nil
}

func (c *Connector) register(ctx context.Context) error {
	useSASL := c.settings.SASLEnabled()
	caps := []string{"server-time", "account-tag"}
	if useSASL {
		caps = append(caps, "sasl")
	}
	if err := c.client.WriteLine(FormatCommand(CmdCap, []string{"REQ"}, strings.Join(caps, " "), true)); err != nil {
		return fmt.Errorf("irc CAP REQ: %w", err)
	}
	if useSASL {
		if err := c.runSASLPlain(ctx); err != nil {
			return err
		}
	}
	if c.settings.ServerPassword != "" {
		if err := c.client.WriteLine(CmdPass + " " + c.settings.ServerPassword); err != nil {
			return fmt.Errorf("irc PASS: %w", err)
		}
	}
	user := FormatCommand(
		CmdUser,
		[]string{c.settings.EffectiveUsername(), "0", "*"},
		c.settings.RealName,
		true,
	)
	if err := c.client.WriteLine(user); err != nil {
		return fmt.Errorf("irc USER: %w", err)
	}
	if err := c.client.WriteLine(CmdNick + " " + c.settings.Nick); err != nil {
		return fmt.Errorf("irc NICK: %w", err)
	}
	if err := c.client.WriteLine(CmdCap + " END"); err != nil {
		return fmt.Errorf("irc CAP END: %w", err)
	}
	return nil
}

// runSASLPlain executes the SASL PLAIN exchange. PINGs received mid-flight
// are answered transparently so the server doesn't drop us.
func (c *Connector) runSASLPlain(ctx context.Context) error {
	ack, err := c.awaitCapAck(ctx, "sasl")
	if err != nil {
		return err
	}
	if !ack {
		c.log.Warn("irc: SASL not supported by server; falling back to unauthenticated")
		return nil
	}
	if err := c.client.WriteLine(CmdAuthenticate + " PLAIN"); err != nil {
		return fmt.Errorf("irc AUTHENTICATE PLAIN: %w", err)
	}
	cont, err := c.awaitAuthenticateContinue(ctx)
	if err != nil {
		return err
	}
	if !cont {
		return nil
	}
	creds := "\x00" + c.settings.SASLUser + "\x00" + c.settings.SASLPassword
	encoded := base64.StdEncoding.EncodeToString([]byte(creds))
	if err := c.client.WriteLine(CmdAuthenticate + " " + encoded); err != nil {
		return fmt.Errorf("irc AUTHENTICATE creds: %w", err)
	}
	return c.awaitSASLResult(ctx)
}

func (c *Connector) awaitCapAck(ctx context.Context, capability string) (bool, error) {
	for {
		line, err := c.readLineRespectingCtx(ctx)
		if err != nil {
			return false, err
		}
		msg := Parse(line)
		if msg.Command == CmdPing {
			c.respondPong(msg)
			continue
		}
		if msg.Command == CmdCap && len(msg.Params) >= 2 {
			sub := strings.ToUpper(msg.Params[1])
			payload := msg.Trailing
			if payload == "" && len(msg.Params) > 2 {
				payload = strings.Join(msg.Params[2:], " ")
			}
			if sub == "ACK" && strings.Contains(payload, capability) {
				return true, nil
			}
			if sub == "NAK" && strings.Contains(payload, capability) {
				return false, nil
			}
		}
	}
}

func (c *Connector) awaitAuthenticateContinue(ctx context.Context) (bool, error) {
	for {
		line, err := c.readLineRespectingCtx(ctx)
		if err != nil {
			return false, err
		}
		msg := Parse(line)
		if msg.Command == CmdPing {
			c.respondPong(msg)
			continue
		}
		if msg.Command == CmdAuthenticate {
			challenge := msg.Trailing
			if challenge == "" && len(msg.Params) > 0 {
				challenge = msg.Params[0]
			}
			return challenge == "+", nil
		}
		switch msg.Command {
		case ErrSaslFail, ErrSaslAborted, ErrSaslAlready, ErrSaslTooLong:
			return false, fmt.Errorf("irc SASL rejected: %s %s", msg.Command, msg.Trailing)
		}
	}
}

func (c *Connector) awaitSASLResult(ctx context.Context) error {
	for {
		line, err := c.readLineRespectingCtx(ctx)
		if err != nil {
			return err
		}
		msg := Parse(line)
		if msg.Command == CmdPing {
			c.respondPong(msg)
			continue
		}
		switch msg.Command {
		case RplSaslSuccess, RplSaslLoggedIn:
			c.log.Info("irc: SASL authenticated", "user", c.settings.SASLUser)
			return nil
		case ErrSaslFail, ErrSaslAborted, ErrSaslAlready, ErrSaslTooLong:
			return fmt.Errorf("irc SASL failed: %s %s", msg.Command, msg.Trailing)
		}
	}
}

func (c *Connector) awaitHandshake(ctx context.Context) error {
	deadline := c.settings.HandshakeTimeout
	if deadline <= 0 {
		deadline = 30 * time.Second
	}
	hctx, cancel := context.WithTimeout(ctx, deadline)
	defer cancel()
	for {
		line, err := c.readLineRespectingCtx(hctx)
		if err != nil {
			return fmt.Errorf("irc handshake: %w", err)
		}
		c.log.Debug("irc <<", "line", line)
		msg := Parse(line)
		if msg.Command == CmdPing {
			c.respondPong(msg)
			continue
		}
		// Surface the common pre-MOTD errors loudly. Without this the
		// connector silently sits waiting for 376 until the handshake
		// timeout fires — looks identical to "bot is dead" from the
		// outside.
		switch msg.Command {
		case ErrNickNameInUse:
			return fmt.Errorf("irc handshake: nickname %q already in use (433); set TURBORG_IRC_NICK to a free nick", c.settings.Nick)
		case ErrPasswdMismatch:
			return fmt.Errorf("irc handshake: server password rejected (464)")
		case ErrNotRegistered:
			return fmt.Errorf("irc handshake: server rejected pre-registration command (451)")
		}
		if IsHandshakeComplete(msg.Command) {
			return nil
		}
	}
}

func (c *Connector) respondPong(msg Message) {
	target := msg.Trailing
	if target == "" && len(msg.Params) > 0 {
		target = msg.Params[0]
	}
	_ = c.client.WriteLine(CmdPong + " :" + target)
}

// readLineRespectingCtx reads one line, honoring ctx via SetReadDeadline.
// The deadline is cleared on return so the post-handshake Run() reader
// starts with a clean slate. (An earlier goroutine-watch pattern raced —
// when both ctx.Done() and the cleanup signal became ready at the same
// time, select would 50% of the time fire Unblock, setting a past
// deadline and breaking every subsequent read in Run().)
func (c *Connector) readLineRespectingCtx(ctx context.Context) (string, error) {
	if deadline, ok := ctx.Deadline(); ok {
		_ = c.client.SetReadDeadline(deadline)
	}
	defer func() { _ = c.client.SetReadDeadline(time.Time{}) }()
	return c.client.ReadLine()
}

func (c *Connector) Run(ctx context.Context) error {
	if c.client == nil {
		return errors.New("irc: Run before Start")
	}

	g, gctx := errgroup.WithContext(ctx)
	lines := make(chan string, 64)

	g.Go(func() error {
		<-gctx.Done()
		_ = c.client.Unblock()
		return nil
	})

	// Reader: enforces the silent-death idle timeout from settings.
	// Each iteration resets the read deadline to now + idle; if no
	// data arrives within the window, Go's net layer returns a
	// timeout error and Run unwinds. Without this, a half-dead TLS
	// socket (NAT idle, peer crashed) parks the bot indefinitely —
	// the exact bug the Python silent-death fix addressed.
	g.Go(func() error {
		defer close(lines)
		idle := c.settings.ReadIdleTimeout
		for {
			if idle > 0 {
				_ = c.client.SetReadDeadline(time.Now().Add(idle))
			}
			line, err := c.client.ReadLine()
			if err != nil {
				if gctx.Err() != nil {
					return nil
				}
				var ne net.Error
				if errors.As(err, &ne) && ne.Timeout() {
					// Distinguish operator-initiated shutdown (Unblock)
					// from real silent-death idle. The former implies
					// gctx is done (handled above); arriving here means
					// the read genuinely timed out with no data.
					if idle > 0 {
						return fmt.Errorf("irc read idle: no data for %s", idle)
					}
					return nil
				}
				return fmt.Errorf("irc read: %w", err)
			}
			select {
			case lines <- line:
			case <-gctx.Done():
				return nil
			}
		}
	})

	// Client-initiated PING keep-alive. Forces the read side to receive
	// SOMETHING (a PONG) at a predictable cadence so NAT mappings stay
	// warm and a stalled socket trips the idle timeout above instead of
	// hanging forever. Cadence must be lower than ReadIdleTimeout (the
	// Settings.Validate cross-check enforces this).
	g.Go(func() error {
		interval := c.settings.ClientPingInterval
		if interval <= 0 {
			return nil
		}
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-gctx.Done():
				return nil
			case <-ticker.C:
				token := strconv.FormatInt(time.Now().Unix(), 10)
				if err := c.client.WriteLine(CmdPing + " :" + token); err != nil {
					if gctx.Err() != nil {
						return nil
					}
					return fmt.Errorf("irc client ping: %w", err)
				}
			}
		}
	})

	g.Go(func() error { return c.dispatch(gctx, lines) })

	return g.Wait()
}

func (c *Connector) dispatch(ctx context.Context, lines <-chan string) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case line, ok := <-lines:
			if !ok {
				return nil
			}
			c.dispatchLine(ctx, line)
		}
	}
}

func (c *Connector) dispatchLine(ctx context.Context, line string) {
	c.log.Debug("irc <<", "line", line)
	msg := Parse(line)

	// Forward every observed upstream line to attached bouncer clients
	// so they see the same wire view we do — modulo PING/PONG and the
	// CAP * ACK chatter that's only meaningful between this connector
	// and the server.
	if c.bouncer != nil && shouldFanOutToBouncer(msg.Command) {
		c.bouncer.Broadcast(line, nil)
	}

	// Every self-prefixed upstream line carries the real ident@host
	// the network assigned to the bot — JOIN echo, MODE echo, NICK
	// echo, etc. Feed those values to the bouncer so its synthetic
	// JOIN replay + BroadcastAsSelf fan-out use the same prefix the
	// attached client learned for its own self-identity. Without
	// this, the bouncer prefix is "ident@turborg" (a synthetic
	// hardcoded host) while HexChat's self-identity from upstream is
	// "~ident@user/account-cloak", and echo-message-routed self
	// PRIVMSGs don't match — HexChat treats them as messages from a
	// stranger who happens to share the user's nick.
	if msg.Prefix != "" && c.bouncer != nil {
		if nick := Nick(msg.Prefix); nick != "" && nick == c.CurrentNick() {
			if ident, host, ok := splitIdentHost(msg.Prefix); ok {
				c.bouncer.UpdateUpstreamIdentity(ident, host)
			}
		}
	}

	switch msg.Command {
	case CmdPing:
		c.respondPong(msg)
	case RplWelcome:
		// :server 001 <actualnick> :Welcome to the [...] network <actualnick>
		// The first param is the nick the server actually assigned.
		// On normal-case servers this equals settings.Nick, but Libera
		// (and any server with truncation / disambiguation policies)
		// can hand us a different value. Sync the live nick so the
		// bouncer's upstreamNick + the web's state.nick + the gateway's
		// MESSAGE_SENT.sender all reflect reality.
		if len(msg.Params) > 0 {
			c.setCurrentNick(msg.Params[0])
		}
	case CmdPrivmsg:
		c.handlePrivmsg(ctx, msg, line)
	case CmdJoin:
		c.handleJoin(ctx, msg)
	case CmdPart:
		c.handlePart(ctx, msg)
	case CmdQuit:
		c.handleQuit(ctx, msg)
	case CmdKick:
		c.handleKick(ctx, msg)
	case CmdNick:
		c.handleNickChange(ctx, msg)
	case CmdTopic:
		c.handleTopic(ctx, msg)
	case RplTopic:
		// :server 332 nick #ch :topic
		if len(msg.Params) >= 2 {
			c.state.SetTopic(msg.Params[1], msg.Trailing)
		}
	case RplNoTopic:
		if len(msg.Params) >= 2 {
			c.state.ClearTopic(msg.Params[1])
		}
	case RplTopicWhoTime:
		// :server 333 nick #ch setter unix
		if len(msg.Params) >= 4 {
			setAt := parseUnixSeconds(msg.Params[3])
			c.state.SetTopicMeta(msg.Params[1], msg.Params[2], setAt)
		}
	case RplNamReply:
		// :server 353 nick = #ch :a b @c +d
		if len(msg.Params) >= 3 && msg.Trailing != "" {
			channel := msg.Params[len(msg.Params)-1]
			c.state.OnNamesReply(channel, strings.Fields(msg.Trailing))
		}
	case RplEndOfNames:
		if len(msg.Params) >= 2 {
			channel := msg.Params[1]
			c.state.OnNamesEnd(channel)
			// Publish so the web gateway can push a fresh member
			// list to attached WS clients. Without this, the
			// `state` op the gateway sends on WS connect can be
			// empty (server hasn't yet replied to JOIN with NAMES
			// when the client connected) and the UI shows an empty
			// member list until a full page reload triggers another
			// `state` op.
			if info := c.state.Get(channel); info != nil {
				members := make([]map[string]string, 0, len(info.Members))
				for nick, mode := range info.Members {
					members = append(members, map[string]string{"nick": nick, "mode": mode})
				}
				c.publish(ctx, agent.Event{
					Type: agent.EventChannelNames,
					Fields: map[string]any{
						"connector": c.Name(),
						"channel":   channel,
						"members":   members,
					},
				})
			}
		}
	}
}

// shouldFanOutToBouncer keeps protocol noise off bouncer clients.
func shouldFanOutToBouncer(cmd string) bool {
	switch cmd {
	case "", CmdPing, CmdPong, CmdCap, CmdAuthenticate,
		RplSaslLoggedIn, RplSaslSuccess,
		ErrSaslFail, ErrSaslAborted, ErrSaslAlready, ErrSaslTooLong:
		return false
	}
	return true
}

func (c *Connector) handlePrivmsg(ctx context.Context, msg Message, raw string) {
	if len(msg.Params) < 1 {
		return
	}
	target := msg.Params[0]
	sender := Nick(msg.Prefix)
	text := msg.Trailing

	// CTCP auto-reply. Standard CTCP is \x01CMD[ args]\x01 inside a
	// PRIVMSG; ACTION (/me) is chat, not metadata, so we leave it alone.
	if isCTCP(text) && !isACTION(text) {
		if c.settings.CTCPAutoReply && c.ctcp != nil && c.ctcp.Allow(strings.ToLower(sender)) {
			if reply := ctcpReply(text); reply != "" {
				_ = c.client.WriteLine(fmt.Sprintf("%s %s :\x01%s\x01", CmdNotice, sender, reply))
			}
		}
		return
	}

	env := agent.NewInbound(c.Name(), target, sender, text)
	env.Raw = raw
	if !strings.HasPrefix(target, "#") && !strings.HasPrefix(target, "&") {
		env.IsDirect = true
		env.Channel = sender
	}

	// EventMessage is the agent's responsibility — Agent.handle publishes
	// it when it drains the inbox. Publishing from here too produced
	// duplicate fan-outs to subscribers (most visibly: the WS gateway
	// broadcasting every inbound PRIVMSG twice). The connector only
	// owns lifecycle + IRC-specific events (USER_JOIN, TOPIC_CHANGED,
	// etc.); message-shaped events are the agent's lane.

	select {
	case c.inbox <- env:
	case <-ctx.Done():
	}
}

func (c *Connector) handleJoin(ctx context.Context, msg Message) {
	channel := joinTarget(msg)
	if channel == "" {
		return
	}
	nick := Nick(msg.Prefix)
	if nick == c.settings.Nick {
		c.state.OnSelfJoin(channel)
	} else {
		c.state.OnMemberJoin(channel, nick)
	}
	c.publish(ctx, agent.Event{
		Type:   agent.EventUserJoin,
		Fields: map[string]any{"connector": c.Name(), "channel": channel, "nick": nick},
	})
}

func (c *Connector) handlePart(ctx context.Context, msg Message) {
	if len(msg.Params) < 1 {
		return
	}
	channel := msg.Params[0]
	nick := Nick(msg.Prefix)
	if nick == c.settings.Nick {
		c.state.OnSelfPart(channel)
	} else {
		c.state.OnMemberPart(channel, nick)
	}
	c.publish(ctx, agent.Event{
		Type:   agent.EventUserLeave,
		Fields: map[string]any{"connector": c.Name(), "channel": channel, "nick": nick},
	})
}

func (c *Connector) handleQuit(ctx context.Context, msg Message) {
	nick := Nick(msg.Prefix)
	if nick == "" {
		return
	}
	c.state.OnMemberQuit(nick)
	c.publish(ctx, agent.Event{
		Type:   agent.EventUserLeave,
		Fields: map[string]any{"connector": c.Name(), "nick": nick, "reason": msg.Trailing},
	})
}

func (c *Connector) handleKick(ctx context.Context, msg Message) {
	if len(msg.Params) < 2 {
		return
	}
	channel := msg.Params[0]
	victim := msg.Params[1]
	c.state.OnMemberKick(channel, victim)
	c.publish(ctx, agent.Event{
		Type: agent.EventUserKicked,
		Fields: map[string]any{
			"connector": c.Name(),
			"channel":   channel,
			"nick":      victim,
			"by":        Nick(msg.Prefix),
			"reason":    msg.Trailing,
		},
	})
}

func (c *Connector) handleNickChange(ctx context.Context, msg Message) {
	old := Nick(msg.Prefix)
	newNick := msg.Trailing
	if newNick == "" && len(msg.Params) > 0 {
		newNick = msg.Params[0]
	}
	if old == "" || newNick == "" {
		return
	}
	c.state.OnNickChange(old, newNick)
	// Self-rename: bump the live nick (and the bouncer's upstreamNick
	// via setCurrentNick) so every surface that reads CurrentNick
	// reflects the new identity.
	if old == c.CurrentNick() {
		c.setCurrentNick(newNick)
	}
	c.publish(ctx, agent.Event{
		Type: agent.EventUserNickChange,
		Fields: map[string]any{
			"connector": c.Name(),
			"old":       old,
			"new":       newNick,
		},
	})
}

func (c *Connector) handleTopic(ctx context.Context, msg Message) {
	if len(msg.Params) < 1 {
		return
	}
	channel := msg.Params[0]
	topic := msg.Trailing
	c.state.SetTopic(channel, topic)
	c.publish(ctx, agent.Event{
		Type: agent.EventTopicChanged,
		Fields: map[string]any{
			"connector": c.Name(),
			"channel":   channel,
			"topic":     topic,
			"by":        Nick(msg.Prefix),
		},
	})
}

// joinTarget extracts the channel from a JOIN — most servers use a single
// param ("JOIN #ch"), some send it as trailing (":nick!~u@h JOIN :#ch").
func joinTarget(msg Message) string {
	if len(msg.Params) > 0 && msg.Params[0] != "" {
		return msg.Params[0]
	}
	return msg.Trailing
}

func parseUnixSeconds(s string) int64 {
	var v int64
	for _, b := range []byte(s) {
		if b < '0' || b > '9' {
			return 0
		}
		v = v*10 + int64(b-'0')
	}
	return v
}

func (c *Connector) publish(ctx context.Context, ev agent.Event) {
	if c.events == nil {
		return
	}
	c.events.Publish(ctx, &ev)
}

func (c *Connector) Stop(_ context.Context) error {
	if c.bouncer != nil {
		_ = c.bouncer.Stop()
	}
	if c.client == nil {
		return nil
	}
	var err error
	c.stopOnce.Do(func() {
		_ = c.client.WriteLine(CmdQuit + " :bye")
		err = c.client.Close()
	})
	return err
}

// --- CTCP helpers ---------------------------------------------------------

const ctcpDelim = "\x01"

func isCTCP(text string) bool {
	return len(text) >= 2 && strings.HasPrefix(text, ctcpDelim) && strings.HasSuffix(text, ctcpDelim)
}

func isACTION(text string) bool {
	inner := strings.Trim(text, ctcpDelim)
	return strings.HasPrefix(strings.ToUpper(inner), "ACTION")
}

func ctcpReply(text string) string {
	inner := strings.Trim(text, ctcpDelim)
	if inner == "" {
		return ""
	}
	parts := strings.SplitN(inner, " ", 2)
	cmd := strings.ToUpper(parts[0])
	arg := ""
	if len(parts) > 1 {
		arg = parts[1]
	}
	switch cmd {
	case "VERSION":
		return "VERSION turborg-go"
	case "PING":
		return "PING " + arg
	case "TIME":
		return "TIME " + time.Now().UTC().Format(time.RFC3339)
	case "CLIENTINFO":
		return "CLIENTINFO VERSION PING TIME CLIENTINFO SOURCE USERINFO"
	case "SOURCE":
		return "SOURCE https://github.com/turborg/turborg"
	case "USERINFO":
		return "USERINFO turborg agent"
	}
	return ""
}
