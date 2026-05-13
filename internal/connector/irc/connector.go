package irc

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"log/slog"
	"net"
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
//     NickServ IDENTIFY if configured.
//   - Run: long-lived supervised loop owned by Agent's errgroup. Reader
//     + dispatcher + client-ping ticker, all unwound on ctx cancel via
//     SetReadDeadline.
//   - Stop: send QUIT and close. Idempotent.
type Connector struct {
	settings *Settings
	log      *slog.Logger
	client   *Client
	inbox    chan *agent.InboundEnvelope

	stopOnce sync.Once
}

func New(s *Settings, log *slog.Logger) *Connector {
	if log == nil {
		log = slog.Default()
	}
	return &Connector{
		settings: s,
		log:      log,
		inbox:    make(chan *agent.InboundEnvelope, 64),
	}
}

func (c *Connector) Name() string                          { return "irc" }
func (c *Connector) Inbound() <-chan *agent.InboundEnvelope { return c.inbox }
func (c *Connector) ClaimSupervision() bool                { return true }

func (c *Connector) Send(env *agent.OutboundEnvelope) error {
	if c.client == nil {
		return errors.New("irc: not connected")
	}
	return c.client.WriteLine(fmt.Sprintf("%s %s :%s", CmdPrivmsg, env.Channel, env.Text))
}

// Start opens the TCP/TLS connection and completes the IRCv3 handshake,
// returning only when RPL_ENDOFMOTD or ERR_NOMOTD has arrived (or the
// handshake-timeout elapses).
func (c *Connector) Start(ctx context.Context) error {
	cli, err := Dial(ctx, c.settings.Hostname, c.settings.Port, c.settings.UseTLS)
	if err != nil {
		return err
	}
	c.client = cli

	if err := c.register(ctx); err != nil {
		_ = cli.Close()
		return err
	}
	if err := c.awaitHandshake(ctx); err != nil {
		_ = cli.Close()
		return err
	}
	for _, ch := range c.settings.NormalizedChannels() {
		if err := c.client.WriteLine(CmdJoin + " " + ch); err != nil {
			_ = cli.Close()
			return fmt.Errorf("irc JOIN %s: %w", ch, err)
		}
	}
	if c.settings.NickServPassword != "" {
		if err := c.client.WriteLine(
			fmt.Sprintf("%s NickServ :IDENTIFY %s", CmdPrivmsg, c.settings.NickServPassword),
		); err != nil {
			_ = cli.Close()
			return fmt.Errorf("irc NickServ IDENTIFY: %w", err)
		}
	}
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
		msg := Parse(line)
		if msg.Command == CmdPing {
			c.respondPong(msg)
			continue
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

	g.Go(func() error {
		defer close(lines)
		for {
			line, err := c.client.ReadLine()
			if err != nil {
				if gctx.Err() != nil {
					return nil
				}
				var ne net.Error
				if errors.As(err, &ne) && ne.Timeout() {
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
			msg := Parse(line)
			switch msg.Command {
			case CmdPing:
				c.respondPong(msg)
			case CmdPrivmsg:
				c.handlePrivmsg(ctx, msg, line)
			}
		}
	}
}

func (c *Connector) handlePrivmsg(ctx context.Context, msg Message, raw string) {
	if len(msg.Params) < 1 {
		return
	}
	target := msg.Params[0]
	env := agent.NewInbound(c.Name(), target, Nick(msg.Prefix), msg.Trailing)
	env.Raw = raw
	if !strings.HasPrefix(target, "#") && !strings.HasPrefix(target, "&") {
		env.IsDirect = true
		env.Channel = Nick(msg.Prefix)
	}
	select {
	case c.inbox <- env:
	case <-ctx.Done():
	}
}

func (c *Connector) Stop(_ context.Context) error {
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
