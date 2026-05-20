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
	"sync/atomic"
	"time"

	"github.com/turborg/turborg/internal/agent"
	"github.com/turborg/turborg/internal/version"
	"golang.org/x/sync/errgroup"
)

// pongTimeoutError is the sentinel returned from the pong-watchdog when
// no PONG arrived within Settings.PongTimeout. Implements net.Error so
// the supervisor's ClassifyError path maps it to DisconnectedTransient
// even when the eager state transition loses a race.
type pongTimeoutError struct{ age time.Duration }

func (e *pongTimeoutError) Error() string {
	return fmt.Sprintf("irc pong timeout: outstanding for %s", e.age)
}

func (e *pongTimeoutError) Timeout() bool   { return true }
func (e *pongTimeoutError) Temporary() bool { return true }

// Connector is the IRC adapter: TLS dial + IRCv3 CAP / SASL handshake,
// supervised read+ping loop, channel-state cache, optional bouncer.
//
// Lifecycle:
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
	inbox    chan *agent.InboundEnvelope

	// clientMu guards client. The reconnect supervisor in Run swaps
	// client across reconnect attempts, while the bouncer's
	// AttachUpstream closure + Send / SendRaw / Stop callers read it
	// from arbitrary goroutines. Read with getClient(); write with
	// setClient().
	clientMu sync.RWMutex
	client   *Client

	// machine is the per-connector upstream state machine. External
	// surfaces (bouncer, web gateway) subscribe via UpstreamState() to
	// learn when the connector is registered, reconnecting, banned, or
	// paused. Initialised in New so callers can subscribe before Start.
	machine *UpstreamStateMachine

	state    *ChannelState
	bouncer  *Bouncer
	ctcp     *Throttle

	// wanted is the "channels I want to be in" set the reconnect
	// supervisor replays JOINs from. Seeded at construction from the
	// operator-configured channel list; grown/shrunk at runtime by
	// upstream-observed self-JOIN/PART echoes and by client-originated
	// JOIN/PART commands routed through the bouncer.
	wanted *WantedChannels

	// clientLimits is the operator-policy struct the bouncer consults
	// before forwarding client-originated commands upstream. Held here
	// so runtime.Build can hand it to the connector before Start; it
	// gets attached to the bouncer when (and if) the bouncer is created
	// during Start.
	clientLimits ClientLimits

	// outboundThrottle is the per-target client-originated PRIVMSG
	// throttle. Shared with the WS gateway so a user running both
	// HexChat and the web UI shares one bucket per target.
	outboundThrottle *Throttle

	// bouncerAttachHook is forwarded onto the bouncer once it is built
	// during startBouncer. Held here so SetBouncerAttachHook can run
	// before Start without depending on bouncer existence yet.
	bouncerAttachHook func(reason string)

	// onUpstreamWarn fires from the escalation watchdog once per
	// transient-outage window when UpstreamWarnAfter elapses without
	// a successful reconnect. Wired by the bouncer to broadcast a
	// "still retrying" NOTICE to every joined channel; nil = disabled.
	onUpstreamWarn func(serverReason string, dwell time.Duration)

	// ownerNudge, when non-nil, emits a periodic usage-summary DM to
	// the configured owner nick. nil = disabled.
	ownerNudge *OwnerNudge

	// onBotSpoke, when non-nil, fires for each bot-originated outbound
	// PRIVMSG. Wired by runtime so an external observer can distinguish
	// bot-driven activity from incoming channel chatter. Bouncer-tunneled
	// PRIVMSGs go through a different code path (AttachUpstream callback)
	// and are NOT considered bot_spoke — those are bouncer-attach activity.
	onBotSpoke func(reason string)

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

	// preferredNickMu guards preferredNick. The bouncer SetPreferredNick
	// during detached-state NICK handling so the supervisor's next
	// register() uses the queued nick instead of settings.Nick.
	// Cleared after the supervisor consumes it on the next handshake.
	preferredNickMu       sync.RWMutex
	preferredNick         string
	preferredNickChangeCB func()

	// pingLedgerRef is the per-session outstanding-PING ledger,
	// installed by runSession at session start and cleared on session
	// exit. dispatchLine reads it on each inbound PONG to record the
	// roundtrip; a nil value (between sessions, or under test fixtures
	// that bypass runSession) makes the ack a no-op.
	pingLedgerRef atomic.Pointer[pingLedger]

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
		machine:     NewUpstreamStateMachine(log),
		wanted:      NewWantedChannels(s.NormalizedChannels()),
		currentNick: s.Nick,
	}
}

// WantedChannels returns the connector's wanted-channels set. Used by
// the bouncer (to record keys from client-originated JOIN frames) and
// by tests. The pointer is stable for the lifetime of the Connector.
func (c *Connector) WantedChannels() *WantedChannels { return c.wanted }

// getClient returns the live upstream client pointer. nil before the
// first Dial and during the gap between a session ending and the
// supervisor re-Dialing.
func (c *Connector) getClient() *Client {
	c.clientMu.RLock()
	defer c.clientMu.RUnlock()
	return c.client
}

// setClient swaps the upstream client. Used by Start (initial dial) and
// the reconnect supervisor (subsequent dials).
func (c *Connector) setClient(cli *Client) {
	c.clientMu.Lock()
	defer c.clientMu.Unlock()
	c.client = cli
}

// UpstreamState returns the per-connector upstream state machine.
// Subscribers attach here to learn when the connector is registered,
// reconnecting, banned, or paused. Returned pointer is stable for the
// lifetime of the Connector.
func (c *Connector) UpstreamState() *UpstreamStateMachine { return c.machine }

func (c *Connector) Name() string                          { return "irc" }
func (c *Connector) Inbound() <-chan *agent.InboundEnvelope { return c.inbox }
func (c *Connector) ClaimSupervision() bool                { return true }
func (c *Connector) State() *ChannelState                  { return c.state }

// SetClientLimits installs the operator-policy struct that gates
// client-initiated commands (NICK, USER realname, JOIN-vs-channel-cap).
// Must be called before Start so the limits are in place when the
// bouncer is constructed. Calling later is a no-op.
func (c *Connector) SetClientLimits(l ClientLimits) { c.clientLimits = l }

// ClientLimits returns the operator-policy struct currently installed
// on the connector. Used by adjacent surfaces (e.g. the WS gateway) to
// enforce the same rules against client-originated actions that don't
// flow through the bouncer.
func (c *Connector) ClientLimits() ClientLimits { return c.clientLimits }

// SetOutboundThrottle installs the per-target outbound PRIVMSG throttle.
// Pass nil to disable. Must be called before Start so the throttle is
// in place when the bouncer is constructed; the WS gateway picks up
// the same instance via OutboundThrottle().
func (c *Connector) SetOutboundThrottle(t *Throttle) { c.outboundThrottle = t }

// OutboundThrottle returns the per-target outbound throttle currently
// installed on the connector (may be nil). Exposed so the WS gateway
// can consult the same instance for web-originated PRIVMSG.
func (c *Connector) OutboundThrottle() *Throttle { return c.outboundThrottle }

// SetOwnerNudge installs the periodic owner-DM usage summary. Pass nil
// to disable. When set, Connector.Send increments the nudge counter
// after each successful PRIVMSG write and the nudge emits a DM when
// the count crosses a multiple of EveryN.
func (c *Connector) SetOwnerNudge(n *OwnerNudge) { c.ownerNudge = n }

// SetActivityHook installs a callback fired for each bot-originated
// outbound PRIVMSG. Pass nil to disable. Wired by runtime so an
// observer learns the bot is doing work, even on dashboard-only or
// silent-channel deployments where log scraping for PRIVMSG would miss
// the signal. Bouncer-tunneled and incoming PRIVMSG do NOT fire this
// hook — those reflect user activity, not bot activity.
func (c *Connector) SetActivityHook(hook func(reason string)) { c.onBotSpoke = hook }

// SetBouncerAttachHook installs a callback the bouncer fires on each
// successful client auth. Pass nil to disable. Must be called before
// Start so the bouncer picks it up when it is constructed.
func (c *Connector) SetBouncerAttachHook(hook func(reason string)) { c.bouncerAttachHook = hook }

// SetUpstreamWarnHook installs the callback the escalation watchdog
// fires when a transient outage persists past UpstreamWarnAfter. Pass
// nil to disable. Typically wired by the bouncer to broadcast a "still
// retrying" NOTICE into every joined channel buffer so attached
// clients know the connector is in a long outage rather than a brief
// blip.
func (c *Connector) SetUpstreamWarnHook(hook func(serverReason string, dwell time.Duration)) {
	c.onUpstreamWarn = hook
}

// SetPreferredNick queues a nick to use on the next registration.
// Called by the bouncer when a client issues NICK during a detached
// upstream — the supervisor's next bringUp picks it up via
// effectiveNick(). Passing the empty string clears any pending queued
// nick.
//
// Fires the change callback (when one is installed via
// SetPreferredNickChangeHook) when the stored value actually moves.
// Repeated SetPreferredNick("x") calls with the same x don't refire.
func (c *Connector) SetPreferredNick(nick string) {
	c.preferredNickMu.Lock()
	changed := c.preferredNick != nick
	c.preferredNick = nick
	cb := c.preferredNickChangeCB
	c.preferredNickMu.Unlock()
	if changed && cb != nil {
		cb()
	}
}

// SetPreferredNickChangeHook installs a callback fired whenever
// SetPreferredNick changes the queued nick (no-op on calls that
// leave it unchanged). Pass nil to disable. Used by the state-sync
// emitter to learn when the snapshot's "nick" field needs re-pushing.
func (c *Connector) SetPreferredNickChangeHook(hook func()) {
	c.preferredNickMu.Lock()
	defer c.preferredNickMu.Unlock()
	c.preferredNickChangeCB = hook
}

// PreferredNick returns the currently-queued preferred nick, or empty
// when none is queued. Used by tests; the supervisor consumes the
// value via effectiveNick().
func (c *Connector) PreferredNick() string {
	c.preferredNickMu.RLock()
	defer c.preferredNickMu.RUnlock()
	return c.preferredNick
}

// setPingLedger installs (or clears, with nil) the per-session ledger
// the dispatch loop acks PONGs into. Idempotent.
func (c *Connector) setPingLedger(l *pingLedger) {
	c.pingLedgerRef.Store(l)
}

// pongLedger returns the per-session ledger, or nil if no session is
// currently running. dispatchLine consults this on every inbound PONG.
func (c *Connector) pongLedger() *pingLedger {
	return c.pingLedgerRef.Load()
}

// effectiveNick returns the nick the supervisor should register with:
// the queued preferred nick when set, falling back to the env-configured
// settings.Nick. The queued nick is consumed on success — cleared so a
// later transient reconnect doesn't keep re-applying the override.
func (c *Connector) effectiveNick() string {
	c.preferredNickMu.RLock()
	queued := c.preferredNick
	c.preferredNickMu.RUnlock()
	if queued != "" {
		return queued
	}
	return c.settings.Nick
}

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
	cli := c.getClient()
	if cli == nil {
		return errors.New("irc: not connected")
	}
	return cli.WriteLine(line)
}

func (c *Connector) Send(env *agent.OutboundEnvelope) error {
	cli := c.getClient()
	if cli == nil {
		return errors.New("irc: not connected")
	}
	line := fmt.Sprintf("%s %s :%s", CmdPrivmsg, env.Channel, env.Text)
	// "irc >>" log line fires from Client.WriteLine for every outbound
	// write — no per-callsite duplication.
	if err := cli.WriteLine(line); err != nil {
		return err
	}
	if c.bouncer != nil {
		c.bouncer.BroadcastAsSelf(line, nil)
	}
	// Owner-nudge counter. Passing cli.WriteLine directly (not c.Send)
	// so the nudge DM doesn't recurse back through this method and
	// double-count.
	c.ownerNudge.Note(cli.WriteLine)
	if c.onBotSpoke != nil {
		c.onBotSpoke("bot_spoke")
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

	if err := c.bringUp(ctx); err != nil {
		return err
	}

	if c.settings.CTCPMaxPerWindow > 0 && c.settings.CTCPWindowSeconds > 0 {
		tr, err := NewThrottle(
			c.settings.CTCPMaxPerWindow,
			time.Duration(c.settings.CTCPWindowSeconds)*time.Second,
			nil,
		)
		if err != nil {
			if cli := c.getClient(); cli != nil {
				_ = cli.Close()
				c.setClient(nil)
			}
			return fmt.Errorf("irc CTCP throttle: %w", err)
		}
		c.ctcp = tr
	}

	// Bouncer was pre-bound at the top of Start; nothing to do here —
	// its sendUpstream closure resolves the live client through
	// getClient(), so forwarded commands from clients work both on
	// first connect and across reconnects.

	c.publish(ctx, agent.Event{
		Type: agent.EventReady,
		Fields: map[string]any{
			"connector": c.Name(),
			"nick":      c.settings.Nick,
		},
	})
	return nil
}

// bringUp performs one upstream connect cycle: Dial → register → MOTD
// → JOIN configured channels → NickServ IDENTIFY, publishing state
// transitions through the state machine along the way. Used by Start
// for the initial connect and by the reconnect supervisor for every
// subsequent attempt.
//
// On failure, bringUp closes any opened client, clears c.client, and
// transitions the state machine to the classified disconnected_* state
// before returning the wrapped error. On success it leaves the new
// client installed and transitions to registered.
func (c *Connector) bringUp(ctx context.Context) error {
	c.log.Info("irc connecting",
		"host", c.settings.Hostname,
		"port", c.settings.Port,
		"tls", c.settings.UseTLS,
		"nick", c.settings.Nick,
	)
	c.machine.Transition(UpstreamStateConnecting)

	cli, err := Dial(ctx, c.settings.Hostname, c.settings.Port, c.settings.UseTLS)
	if err != nil {
		c.classifyFallback(err)
		return err
	}
	cli.SetLog(c.log)
	c.setClient(cli)

	c.machine.Transition(UpstreamStateRegistering)
	if err := c.register(ctx); err != nil {
		_ = cli.Close()
		c.setClient(nil)
		c.classifyFallback(err)
		return err
	}
	if err := c.awaitHandshake(ctx); err != nil {
		_ = cli.Close()
		c.setClient(nil)
		c.classifyFallback(err)
		return err
	}
	c.log.Info("irc handshake complete", "nick", c.settings.Nick)

	// Replay every channel the supervisor's wanted-set has tracked —
	// initially this is just the operator-configured channel list, but
	// across reconnects it grows / shrinks with client-driven JOIN /
	// PART activity and carries channel keys captured from those
	// JOINs so +k channels rejoin cleanly without 475 ERR_BADCHANNELKEY.
	for _, w := range c.wanted.Snapshot() {
		line := CmdJoin + " " + w.Name
		if w.Key != "" {
			line += " " + w.Key
		}
		c.log.Info("irc joining channel", "channel", w.Name, "keyed", w.Key != "")
		if err := cli.WriteLine(line); err != nil {
			_ = cli.Close()
			c.setClient(nil)
			c.classifyFallback(err)
			return fmt.Errorf("irc JOIN %s: %w", w.Name, err)
		}
	}
	if c.settings.NickServPassword != "" {
		c.log.Info("irc identifying with NickServ")
		if err := cli.WriteLine(
			fmt.Sprintf("%s NickServ :IDENTIFY %s", CmdPrivmsg, c.settings.NickServPassword),
		); err != nil {
			_ = cli.Close()
			c.setClient(nil)
			c.classifyFallback(err)
			return fmt.Errorf("irc NickServ IDENTIFY: %w", err)
		}
	}

	// Re-pin the bouncer's view of the upstream identity. On first
	// connect this is a no-op replay; on reconnect it picks up the
	// fresh ident/host the network assigns.
	if c.bouncer != nil {
		c.bouncer.AttachState(c.state, c.CurrentNick(), c.settings.EffectiveUsername(), "turborg")
	}

	c.machine.Transition(UpstreamStateRegistered)
	// Successful registration consumes any queued preferred-nick — a
	// later transient disconnect must not silently re-apply it on the
	// next reconnect (the user only asked for the nick change once).
	c.SetPreferredNick("")
	return nil
}

// transitionFromError classifies a transport-level Go error and publishes
// the resulting state. ctx cancellation is mapped to stopped so the
// supervisor distinguishes operator shutdown from network failure.
func (c *Connector) transitionFromError(err error) {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		c.machine.Transition(UpstreamStateStopped)
		return
	}
	if state, ok := ClassifyError(err); ok {
		c.machine.Transition(state, WithServerReason(err.Error()))
	}
}

// classifyFallback publishes a transport-error-derived state only when
// no upstream classifier has already moved the machine to a recoverable
// or terminal state. The SASL / numeric / ERROR-line classifiers run
// before the wrapping Go error returns up the call stack — their result
// is more specific than the wrapped error's "broken pipe" or "EOF" and
// must not be clobbered.
func (c *Connector) classifyFallback(err error) {
	if cur := c.machine.State(); cur.IsRecoverable() || cur.IsTerminal() {
		return
	}
	c.transitionFromError(err)
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
	b.AttachClientLimits(c.clientLimits)
	b.AttachOutboundThrottle(c.outboundThrottle)
	b.AttachActivityHook(c.bouncerAttachHook)
	b.AttachUpstreamState(c.machine, c.settings.Hostname)
	b.AttachWantedChannels(c.wanted)
	b.AttachPreferredNickHook(c.SetPreferredNick)
	// Reuse the existing onUpstreamWarn hook slot: the supervisor's
	// long-outage watchdog calls into the bouncer's broadcast so
	// channels get a stronger "still retrying" NOTICE at the warn
	// threshold. Operators who want a different observer can override
	// before Start by calling SetUpstreamWarnHook again — last setter
	// wins.
	c.SetUpstreamWarnHook(b.onUpstreamWarn)
	// sendUpstream is set before c.client is, so guard the dereference.
	// Pre-bound bouncer clients that try to send forwardable commands
	// before the upstream Dial completes get an error back rather than
	// a nil-deref panic. Once bringUp has set c.client, sends go
	// through transparently — and across reconnects the closure picks
	// up whatever client the supervisor most recently installed via
	// getClient().
	b.AttachUpstream(func(line string) error {
		cli := c.getClient()
		if cli == nil {
			return errors.New("irc: upstream not yet connected; please retry")
		}
		return cli.WriteLine(line)
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
	if err := c.client.WriteLine(CmdNick + " " + c.effectiveNick()); err != nil {
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
			if state, ok := ClassifyNumeric(msg.Command, msg.Params, msg.Trailing); ok {
				c.machine.Transition(state, WithServerReason(msg.Trailing))
			}
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
		// Classifier-driven state transitions: anything that signals
		// nick-unavailable / auth-failed / banned during the pre-MOTD
		// window must surface as the correct supervisor state so the
		// reconnect loop knows whether to retry or give up.
		if state, ok := ClassifyNumeric(msg.Command, msg.Params, msg.Trailing); ok {
			c.machine.Transition(state, WithServerReason(msg.Trailing))
		} else if state, reason, ok := ClassifyDisconnectMessage(line); ok {
			c.machine.Transition(state, WithServerReason(reason))
		}
		// Surface the common pre-MOTD errors loudly. Without this the
		// connector silently sits waiting for 376 until the handshake
		// timeout fires — looks identical to "bot is dead" from the
		// outside.
		switch msg.Command {
		case ErrNickNameInUse:
			return fmt.Errorf("irc handshake: nickname %q already in use (433); set TURBORG_IRC_NICK to a free nick", c.settings.Nick)
		case ErrUnavailResource:
			return fmt.Errorf("irc handshake: nickname %q unavailable (437)", c.settings.Nick)
		case ErrPasswdMismatch:
			return fmt.Errorf("irc handshake: server password rejected (464)")
		case ErrYoureBannedCreep:
			return fmt.Errorf("irc handshake: banned from network (465): %s", msg.Trailing)
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
	if cli := c.getClient(); cli != nil {
		_ = cli.WriteLine(CmdPong + " :" + target)
	}
}

// readLineRespectingCtx reads one line, honoring ctx via SetReadDeadline.
// The deadline is cleared on return so the post-handshake Run() reader
// starts with a clean slate. (An earlier goroutine-watch pattern raced —
// when both ctx.Done() and the cleanup signal became ready at the same
// time, select would 50% of the time fire Unblock, setting a past
// deadline and breaking every subsequent read in Run().)
func (c *Connector) readLineRespectingCtx(ctx context.Context) (string, error) {
	cli := c.getClient()
	if cli == nil {
		return "", errors.New("irc: no upstream client")
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = cli.SetReadDeadline(deadline)
	}
	defer func() { _ = cli.SetReadDeadline(time.Time{}) }()
	return cli.ReadLine()
}

// Run is the reconnect supervisor. It loops over runSession, classifies
// the exit reason, and decides whether to retry (recoverable states),
// give up (terminal states), or unwind (ctx cancel). A parallel watchdog
// goroutine escalates long-running transient outages — fires the warn
// callback at UpstreamWarnAfter and transitions to paused_idle at
// UpstreamPauseAfter.
//
// Returns:
//   - nil when ctx is cancelled (state → stopped)
//   - non-nil error when the connector reaches a terminal state
//     (auth_failed / banned / paused_idle) — agent.Run treats that as
//     fatal and unwinds the rest of the connectors via Stop.
func (c *Connector) Run(ctx context.Context) error {
	if c.getClient() == nil {
		return errors.New("irc: Run before Start")
	}

	backoff := NewBackoffSchedule()
	// sessionStart is the moment the current upstream session became
	// active (last successful bringUp). Used to gate backoff.Reset on a
	// minimum-stable-session window: without this gate, networks that
	// accept the IRC handshake and then immediately tear the connection
	// down (Libera's drone-BL post-register KILL, a forwarded K-line,
	// etc.) keep the supervisor pinned at the start of the schedule and
	// reconnecting every ~1s in a tight loop. Initialised to now because
	// Start() runs bringUp once before Run() takes over, so the first
	// runSession iteration corresponds to that already-active session.
	sessionStart := time.Now()
	const sessionStableWindow = 30 * time.Second

	watchdogCtx, cancelWatchdog := context.WithCancel(ctx)
	defer cancelWatchdog()
	watchdogDone := c.runEscalationWatchdog(watchdogCtx)

	// runCtx cancels on either operator shutdown (parent ctx) OR the
	// state machine reaching a terminal state. Passing it to runSession
	// + bringUp means any blocking read / dial unblocks promptly when
	// the watchdog escalates to paused_idle (or anything else trips
	// terminal), so the supervisor doesn't continue iterating against
	// a dead client.
	runCtx, cancelRunCtx := context.WithCancel(ctx)
	defer cancelRunCtx()
	terminalSub := c.machine.Subscribe(func(change UpstreamStateChange) {
		if change.To.IsTerminal() {
			cancelRunCtx()
		}
	})
	defer terminalSub.Unsubscribe()

	exitTerminal := func(err error) error {
		cancelWatchdog()
		<-watchdogDone
		state := c.machine.State()
		if err != nil {
			return fmt.Errorf("irc: terminal upstream state %s: %w", state, err)
		}
		return fmt.Errorf("irc: terminal upstream state %s", state)
	}

	for {
		err := c.runSession(runCtx)

		// Operator-initiated cancellation takes precedence over any
		// terminal transition the supervisor itself published.
		if ctx.Err() != nil {
			c.machine.Transition(UpstreamStateStopped)
			<-watchdogDone
			return nil
		}

		// If the read/dispatch path observed a classified ERROR or
		// numeric, the state machine is already in the right place.
		// Otherwise, fall back to classifying the Go error.
		if cur := c.machine.State(); !cur.IsRecoverable() && !cur.IsTerminal() {
			c.transitionFromError(err)
		}

		if c.machine.State().IsTerminal() {
			return exitTerminal(err)
		}

		// Only credit a backoff reset if the session that just ended ran
		// long enough to look healthy — see sessionStart comment at the
		// top of Run for why. A nil sessionStart means the previous
		// bringUp failed (no session ran at all) and the schedule must
		// keep advancing.
		if !sessionStart.IsZero() && time.Since(sessionStart) >= sessionStableWindow {
			backoff.Reset()
		}
		sessionStart = time.Time{}

		// Recoverable: sleep with backoff, then bring upstream back up.
		// runCtx cancels mid-sleep when the watchdog transitions to
		// paused_idle — that surfaces as runCtx.Done.
		delay := backoff.Next()
		c.log.Info("irc reconnecting", "state", c.machine.State(), "after", delay, "err", err)
		select {
		case <-runCtx.Done():
			if ctx.Err() != nil {
				c.machine.Transition(UpstreamStateStopped)
				<-watchdogDone
				return nil
			}
			return exitTerminal(nil)
		case <-time.After(delay):
		}

		if err := c.bringUp(runCtx); err != nil {
			if c.machine.State().IsTerminal() {
				return exitTerminal(err)
			}
			if ctx.Err() != nil {
				c.machine.Transition(UpstreamStateStopped)
				<-watchdogDone
				return nil
			}
			c.log.Warn("irc reconnect failed", "err", err)
			continue
		}
		sessionStart = time.Now()
	}
}

// runSession runs one upstream session: the reader / ping / dispatch
// goroutines, all bound to the client snapshot taken at session start.
// Returns when the session ends — either ctx cancel (nil) or upstream
// failure (non-nil error). The supervisor loops on the return.
//
//nolint:gocyclo
func (c *Connector) runSession(ctx context.Context) error {
	cli := c.getClient()
	if cli == nil {
		return errors.New("irc: no upstream client")
	}

	g, gctx := errgroup.WithContext(ctx)
	lines := make(chan string, 64)

	// Per-session PING/PONG ledger: the ping-writer Adds a token before
	// every PING write; the dispatch goroutine Acks it on the matching
	// PONG; the pong-watchdog (below) fails the session when an entry
	// outlives PongTimeout. Fresh per runSession iteration so a reconnect
	// starts with a clean slate.
	ledger := newPingLedger()
	c.setPingLedger(ledger)
	defer c.setPingLedger(nil)

	// Monotonic counter for PING token assignment. The previous time-
	// based token risked collisions when two ticks fired within the same
	// second (e.g. an unconfigured 0-or-1s ClientPingInterval); a counter
	// makes every token globally unique within the session.
	var pingSeq uint64
	nextToken := func() string {
		n := atomic.AddUint64(&pingSeq, 1)
		return "tb-" + strconv.FormatUint(n, 36)
	}

	g.Go(func() error {
		<-gctx.Done()
		_ = cli.Unblock()
		return nil
	})

	// Reader: enforces the silent-death idle timeout from settings.
	// Each iteration resets the read deadline to now + idle; if no
	// data arrives within the window, Go's net layer returns a
	// timeout error and the session unwinds. Without this, a half-
	// dead TLS socket (NAT idle, peer crashed) would park the bot
	// indefinitely.
	g.Go(func() error {
		defer close(lines)
		idle := c.settings.ReadIdleTimeout
		for {
			if idle > 0 {
				_ = cli.SetReadDeadline(time.Now().Add(idle))
			}
			line, err := cli.ReadLine()
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
	// Settings.Validate cross-check enforces this). Every tick records
	// the token in the ledger BEFORE the WriteLine so a write that
	// races a pong arrival can never be acked-before-tracked.
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
				token := nextToken()
				ledger.Add(token, time.Now())
				if err := cli.WriteLine(CmdPing + " :" + token); err != nil {
					if gctx.Err() != nil {
						return nil
					}
					return fmt.Errorf("irc client ping: %w", err)
				}
			}
		}
	})

	// Pong-watchdog: actively probes upstream liveness. When the oldest
	// outstanding PING is older than PongTimeout the session is failed
	// with a transient classification — the supervisor's existing
	// backoff + bringUp path takes over, and the state-emitter +
	// bouncer state-subscription surface the disconnect to attached
	// clients well before ReadIdleTimeout would have caught the
	// silently-dead socket.
	g.Go(func() error {
		timeout := c.settings.PongTimeout
		if timeout <= 0 || c.settings.ClientPingInterval <= 0 {
			return nil
		}
		// Cadence: timeout/2 so an expired token is observed within ~1.5x
		// the timeout in the worst case. Capped at 1s so production
		// (timeout=30s) doesn't bother polling at 15s granularity; and
		// floored at 10ms so millisecond-scale test timings stay
		// responsive.
		tickEvery := timeout / 2
		if tickEvery > time.Second {
			tickEvery = time.Second
		}
		if tickEvery < 10*time.Millisecond {
			tickEvery = 10 * time.Millisecond
		}
		ticker := time.NewTicker(tickEvery)
		defer ticker.Stop()
		for {
			select {
			case <-gctx.Done():
				return nil
			case <-ticker.C:
				oldest, ok := ledger.Oldest()
				if !ok {
					continue
				}
				age := time.Since(oldest)
				if age <= timeout {
					continue
				}
				// Eagerly transition so the state-emitter PUT and the
				// bouncer's channel-targeted NOTICE both fire before
				// the reconnect supervisor cycles. ClassifyError also
				// maps our returned error to transient — both paths
				// are idempotent under Transition's same-state no-op.
				c.machine.Transition(
					UpstreamStateDisconnectedTransient,
					WithServerReason(fmt.Sprintf("no PONG for %s", timeout)),
				)
				return &pongTimeoutError{age: age}
			}
		}
	})

	g.Go(func() error { return c.dispatch(gctx, lines) })

	return g.Wait()
}

// runEscalationWatchdog runs the long-outage escalation timers in
// parallel with the reconnect loop. It measures outage duration as
// "time since the connector was last in registered" — NOT dwell in a
// single transient state — so the timer doesn't reset every time the
// supervisor cycles transient → connecting → registering on a failing
// reconnect attempt. When the outage exceeds UpstreamWarnAfter, the
// warn callback fires once; when it exceeds UpstreamPauseAfter, the
// supervisor transitions to UpstreamStatePausedIdle (terminal).
//
// Returns a channel that closes when the watchdog goroutine exits —
// Run blocks on it during shutdown so the goroutine doesn't outlive
// Run.
func (c *Connector) runEscalationWatchdog(ctx context.Context) <-chan struct{} {
	done := make(chan struct{})
	warnAfter := c.settings.UpstreamWarnAfter
	pauseAfter := c.settings.UpstreamPauseAfter
	if warnAfter <= 0 && pauseAfter <= 0 {
		// Nothing to watch — close immediately so callers don't block.
		close(done)
		return done
	}

	go func() {
		defer close(done)

		var (
			outageMu    sync.Mutex
			outageSince time.Time
		)
		startOutage := func() {
			outageMu.Lock()
			defer outageMu.Unlock()
			if outageSince.IsZero() {
				outageSince = time.Now()
			}
		}
		endOutage := func() {
			outageMu.Lock()
			defer outageMu.Unlock()
			outageSince = time.Time{}
		}
		outageDuration := func() time.Duration {
			outageMu.Lock()
			defer outageMu.Unlock()
			if outageSince.IsZero() {
				return 0
			}
			return time.Since(outageSince)
		}

		sub := c.machine.Subscribe(func(change UpstreamStateChange) {
			if change.To == UpstreamStateRegistered {
				endOutage()
				return
			}
			startOutage()
		})
		defer sub.Unsubscribe()

		// Seed in case the watchdog launched while the state was
		// already non-registered (the supervisor in Run starts the
		// watchdog AFTER bringUp; if bringUp moved through transient
		// and recovered, state is registered — no outage. If bringUp
		// is still mid-flight on first reconnect, state is connecting
		// — count that as outage start).
		if c.machine.State() != UpstreamStateRegistered {
			startOutage()
		}

		interval := watchdogPollInterval(warnAfter, pauseAfter)
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		warned := false
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			if c.machine.State().IsTerminal() {
				return
			}
			dwell := outageDuration()
			if dwell == 0 {
				warned = false
				continue
			}
			if !warned && warnAfter > 0 && dwell >= warnAfter {
				warned = true
				if c.onUpstreamWarn != nil {
					c.onUpstreamWarn(c.machine.ServerReason(), dwell)
				}
			}
			if pauseAfter > 0 && dwell >= pauseAfter {
				c.machine.Transition(UpstreamStatePausedIdle,
					WithServerReason(c.machine.ServerReason()))
				return
			}
		}
	}()

	return done
}

// watchdogPollInterval derives a poll cadence from the configured warn
// and pause windows: aim for ~4 polls per warn window with a floor of
// 10 ms (for fast tests) and a ceiling of 1 s (for hour-long pauses).
func watchdogPollInterval(warn, pause time.Duration) time.Duration {
	target := warn
	if pause > 0 && (target == 0 || pause < target) {
		target = pause
	}
	if target == 0 {
		return time.Second
	}
	d := target / 4
	if d < 10*time.Millisecond {
		d = 10 * time.Millisecond
	}
	if d > time.Second {
		d = time.Second
	}
	return d
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

// dispatchLine is a per-command switch; complexity grows with the IRC spec.
//
//nolint:gocyclo
func (c *Connector) dispatchLine(ctx context.Context, line string) {
	c.log.Debug("irc <<", "line", line)
	msg := Parse(line)

	// Classifier-driven runtime state transitions: a server-initiated
	// ERROR :Closing Link or a late-arriving 465 must move the state
	// machine BEFORE the read loop unwinds, so the supervisor's
	// terminal-vs-recoverable decision after session exit reads the
	// correct state rather than falling back to ClassifyError of an
	// EOF.
	if state, reason, ok := ClassifyDisconnectMessage(line); ok {
		c.machine.Transition(state, WithServerReason(reason))
	} else if state, ok := ClassifyNumeric(msg.Command, msg.Params, msg.Trailing); ok {
		c.machine.Transition(state, WithServerReason(msg.Trailing))
	}

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
	case CmdPong:
		// Match the token back to the outstanding-PING ledger so the
		// pong-watchdog sees a fresh oldest-entry on its next tick.
		// Servers vary: some put the token in the trailing slot
		// (PONG :token), some in the param slot (PONG server token).
		if ledger := c.pongLedger(); ledger != nil {
			token := msg.Trailing
			if token == "" && len(msg.Params) > 0 {
				token = msg.Params[len(msg.Params)-1]
			}
			if token != "" {
				ledger.Ack(token)
			}
		}
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
		c.publishServerNotice(ctx, "welcome", msg.Trailing)
	case RplYourHost, RplCreated, RplMyInfo, RplISupport,
		RplLUserClient, RplLUserOp, RplLUserUnknown, RplLUserChannels, RplLUserMe:
		// Connection-info numerics (server name, version, user counts,
		// supported features). Forward as `info` to the server tab so
		// users see something land during the handshake instead of a
		// blank Server tab. Trailing carries the human-readable text
		// for these; if a server pulls a non-RFC shape that puts the
		// content in params, fall back to the joined params.
		c.publishServerNotice(ctx, "info", serverNoticeText(msg))
	case RplMOTDStart, RplMOTD, RplEndOfMOTD, ErrNoMOTD:
		// The MOTD block. RPL_MOTDSTART (375) / RPL_MOTD (372) /
		// RPL_ENDOFMOTD (376) are the standard shape; ERR_NOMOTD (422)
		// stands in for servers that have no MOTD configured.
		c.publishServerNotice(ctx, "motd", serverNoticeText(msg))
	case CmdError:
		// :server ERROR :<reason> — pre-disconnect server-originated
		// failure (banned, throttled, etc.). The supervisor's
		// ClassifyERRORLine already drives the upstream state machine
		// for this; mirror the body to the server tab so the user
		// sees the reason in the chat surface too.
		c.publishServerNotice(ctx, "error", serverNoticeText(msg))
	case CmdNotice:
		// Server-originated NOTICE (no `!user@host` in the prefix) —
		// pre-registration "NOTICE AUTH :*** Looking up your
		// hostname" lines, services bot greetings, etc. — flow into
		// the server tab. Client-originated NOTICEs (with a user
		// hostmask prefix) are intentionally left unhandled here:
		// the bot has no PRIVMSG-equivalent path for NOTICE today,
		// and silently dropping matches the pre-existing behaviour.
		if isServerPrefix(msg.Prefix) {
			c.publishServerNotice(ctx, "notice", serverNoticeText(msg))
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
	case ErrBannedFromChan, ErrBadChannelKey, ErrChannelIsFull,
		ErrInviteOnlyChan, ErrBadChanMask:
		c.handleJoinFailure(ctx, msg)
	}
}

// handleJoinFailure processes per-channel JOIN rejection numerics
// (474/475/471/473/476). The supervisor's reconnect replay can issue a
// burst of JOINs and have individual channels rejected for unrelated
// reasons (banned, bad key, full, invite-only, malformed name) — each
// failure must be honest with the user rather than silently retried.
//
// Per-failure work:
//   - Drop the channel from the wanted-set so the next reconnect
//     doesn't re-attempt it. The user can /join again to retry.
//   - Notify attached bouncer clients via a channel-targeted NOTICE.
//   - Publish EventJoinFailed so the web gateway can render the same
//     signal in its UI.
func (c *Connector) handleJoinFailure(ctx context.Context, msg Message) {
	// Most servers format these numerics as `:server <code> <ournick>
	// <channel> :<reason>` — channel is params[1] (second positional).
	if len(msg.Params) < 2 {
		return
	}
	channel := msg.Params[1]
	reason := msg.Trailing
	if reason == "" {
		reason = humanReadableJoinFailure(msg.Command)
	}
	c.wanted.Remove(channel)
	if c.bouncer != nil {
		c.bouncer.NotifyJoinFailure(channel, reason)
	}
	c.publish(ctx, agent.Event{
		Type: agent.EventJoinFailed,
		Fields: map[string]any{
			"connector": c.Name(),
			"channel":   channel,
			"code":      msg.Command,
			"reason":    reason,
		},
	})
}

// humanReadableJoinFailure maps the join-failure numerics to a generic
// reason when the server didn't supply one. Used as a fallback so the
// NOTICE body is never empty.
func humanReadableJoinFailure(code string) string {
	switch code {
	case ErrBannedFromChan:
		return "banned from channel"
	case ErrBadChannelKey:
		return "channel key required or incorrect"
	case ErrChannelIsFull:
		return "channel is full"
	case ErrInviteOnlyChan:
		return "channel is invite-only"
	case ErrBadChanMask:
		return "channel name not accepted by the network"
	}
	return "rejected by network"
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
		// Server doesn't echo the key on JOIN echo, so Add with an
		// empty key — WantedChannels.Add preserves any previously-
		// stored key for this channel.
		c.wanted.Add(channel, "")
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
		c.wanted.Remove(channel)
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
	// Snapshot which channels the nick was in BEFORE the state mutation,
	// so we can fan out a per-channel EventUserLeave for each. The web
	// gateway translates EventUserLeave → op:part on the wire, and the
	// SPA's part handler looks up the channel by name. Without a
	// channel field the SPA can't find the channel and silently no-ops,
	// leaving the QUITting nick ghosted in every member list it was
	// in. IRC QUIT is network-wide; the user-facing model in clients
	// is per-channel removal.
	affected := c.state.ChannelsContaining(nick)
	c.state.OnMemberQuit(nick)
	reason := msg.Trailing
	for _, channel := range affected {
		c.publish(ctx, agent.Event{
			Type: agent.EventUserLeave,
			Fields: map[string]any{
				"connector": c.Name(),
				"channel":   channel,
				"nick":      nick,
				"reason":    reason,
			},
		})
	}
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

// publishServerNotice fans a server-originated line out to the agent
// event bus for the gateway to broadcast as `op:server` to attached
// WS clients. Used for welcome, MOTD, info numerics, server NOTICEs
// and pre-disconnect ERROR lines — the content that lands in the
// SPA's synthetic "server" tab on connect.
func (c *Connector) publishServerNotice(ctx context.Context, kind, text string) {
	if text == "" {
		return
	}
	c.publish(ctx, agent.Event{
		Type: agent.EventServerNotice,
		Fields: map[string]any{
			"connector": c.Name(),
			"kind":      kind,
			"text":      text,
		},
	})
}

// serverNoticeText pulls a human-readable body out of a server message.
// Most server numerics carry their description in the trailing parameter
// (`:Welcome to the network`), but a few RFC-loose servers stuff the
// content into space-separated params instead. Fall back to joining the
// non-target params so 002/003/004-style messages still render
// something useful.
func serverNoticeText(msg Message) string {
	if msg.Trailing != "" {
		return msg.Trailing
	}
	// Skip the first param when it looks like the bot's own nick (a
	// recipient identifier most numerics emit). Without this we'd
	// surface "alice" prefixed to every line.
	params := msg.Params
	if len(params) > 1 {
		params = params[1:]
	}
	return strings.TrimSpace(strings.Join(params, " "))
}

// isServerPrefix reports whether a message prefix identifies a server
// rather than a user. User prefixes carry `!user@host`; server prefixes
// are a bare hostname like `irc.libera.chat`. Empty prefix also counts
// as server-originated (some servers omit it for global notices).
func isServerPrefix(prefix string) bool {
	if prefix == "" {
		return true
	}
	return !strings.ContainsRune(prefix, '!')
}

func (c *Connector) Stop(_ context.Context) error {
	if c.bouncer != nil {
		_ = c.bouncer.Stop()
	}
	cli := c.getClient()
	if cli == nil {
		c.machine.Transition(UpstreamStateStopped)
		return nil
	}
	var err error
	c.stopOnce.Do(func() {
		_ = cli.WriteLine(CmdQuit + " :" + c.settings.EffectiveQuitMessage())
		err = cli.Close()
		c.setClient(nil)
		c.machine.Transition(UpstreamStateStopped)
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
		return "VERSION turborg " + version.Version + " (https://github.com/turborg/turborg)"
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
