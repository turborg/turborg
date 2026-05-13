package irc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"github.com/turborg/turborg/internal/agent"
	"golang.org/x/sync/errgroup"
)

const (
	rplWelcome    = "001"
	rplEndOfMOTD  = "376"
	errNoMOTD     = "422"
)

type Config struct {
	Host    string
	Port    int
	TLS     bool
	Nick    string
	User    string
	Real    string
	Channel string
}

type Connector struct {
	cfg    Config
	log    *slog.Logger
	client *Client
	inbox  chan *agent.InboundEnvelope

	stopOnce sync.Once
}

func New(cfg Config, log *slog.Logger) *Connector {
	if log == nil {
		log = slog.Default()
	}
	return &Connector{
		cfg:   cfg,
		log:   log,
		inbox: make(chan *agent.InboundEnvelope, 32),
	}
}

func (c *Connector) Name() string                          { return "irc" }
func (c *Connector) Inbound() <-chan *agent.InboundEnvelope { return c.inbox }
func (c *Connector) ClaimSupervision() bool                { return true }

func (c *Connector) Send(env *agent.OutboundEnvelope) error {
	if c.client == nil {
		return errors.New("irc: not connected")
	}
	return c.client.WriteLine(fmt.Sprintf("PRIVMSG %s :%s", env.Channel, env.Text))
}

func (c *Connector) Start(ctx context.Context) error {
	cli, err := Dial(ctx, c.cfg.Host, c.cfg.Port, c.cfg.TLS)
	if err != nil {
		return err
	}
	c.client = cli
	if err := cli.WriteLine("NICK " + c.cfg.Nick); err != nil {
		return fmt.Errorf("irc NICK: %w", err)
	}
	if err := cli.WriteLine(fmt.Sprintf("USER %s 0 * :%s", c.cfg.User, c.cfg.Real)); err != nil {
		return fmt.Errorf("irc USER: %w", err)
	}
	return nil
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
	joined := false
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
			case "PING":
				target := msg.Trailing
				if target == "" && len(msg.Params) > 0 {
					target = msg.Params[0]
				}
				if err := c.client.WriteLine("PONG :" + target); err != nil {
					return fmt.Errorf("irc PONG: %w", err)
				}
			case rplEndOfMOTD, errNoMOTD:
				if !joined && c.cfg.Channel != "" {
					if err := c.client.WriteLine("JOIN " + c.cfg.Channel); err != nil {
						return fmt.Errorf("irc JOIN: %w", err)
					}
					joined = true
				}
			case "PRIVMSG":
				if len(msg.Params) < 1 {
					continue
				}
				env := &agent.InboundEnvelope{
					Connector: c.Name(),
					Channel:   msg.Params[0],
					Sender:    Nick(msg.Prefix),
					Text:      msg.Trailing,
					Time:      time.Now(),
				}
				select {
				case c.inbox <- env:
				case <-ctx.Done():
					return nil
				}
			}
		}
	}
}

func (c *Connector) Stop(ctx context.Context) error {
	if c.client == nil {
		return nil
	}
	var err error
	c.stopOnce.Do(func() {
		_ = c.client.WriteLine("QUIT :bye")
		err = c.client.Close()
	})
	return err
}
