package agent

import (
	"context"
	"errors"
	"log/slog"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"
)

type Agent struct {
	connectors []Connector
	log        *slog.Logger
}

func New(log *slog.Logger) *Agent {
	if log == nil {
		log = slog.Default()
	}
	return &Agent{log: log}
}

func (a *Agent) AddConnector(c Connector) {
	a.connectors = append(a.connectors, c)
}

// Run starts every connector, supervises their Run loops inside an errgroup,
// and dispatches inbound envelopes through the PoC echo handler. It returns
// when ctx cancels or any goroutine in the group errors. Stop is then called
// on every connector with a 500ms budget so SIGTERM unwinds quickly.
func (a *Agent) Run(ctx context.Context) error {
	for _, c := range a.connectors {
		if err := c.Start(ctx); err != nil {
			return err
		}
	}

	g, gctx := errgroup.WithContext(ctx)

	for _, c := range a.connectors {
		c := c
		if c.ClaimSupervision() {
			g.Go(func() error { return c.Run(gctx) })
		}
		g.Go(func() error { return a.drain(gctx, c) })
	}

	runErr := g.Wait()

	stopCtx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	for _, c := range a.connectors {
		if err := c.Stop(stopCtx); err != nil {
			a.log.Warn("connector stop", "name", c.Name(), "err", err)
		}
	}

	if runErr != nil && !errors.Is(runErr, context.Canceled) {
		return runErr
	}
	return nil
}

func (a *Agent) drain(ctx context.Context, c Connector) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case env, ok := <-c.Inbound():
			if !ok {
				return nil
			}
			if reply := echoHandler(env); reply != nil {
				if err := c.Send(reply); err != nil {
					a.log.Warn("connector send", "name", c.Name(), "err", err)
				}
			}
		}
	}
}

// echoHandler is the PoC's only command handler: it replies to "!ping" with
// "pong" in the same channel. A real Agent will dispatch through a
// CommandRegistry; for the PoC we hardcode it to prove the end-to-end loop.
func echoHandler(env *InboundEnvelope) *OutboundEnvelope {
	if !strings.HasPrefix(env.Text, "!ping") {
		return nil
	}
	return &OutboundEnvelope{
		Connector: env.Connector,
		Channel:   env.Channel,
		Text:      "pong",
	}
}
