package agent

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"golang.org/x/sync/errgroup"
)

type Agent struct {
	connectors []Connector
	Commands   *CommandRegistry
	Events     *EventBus
	log        *slog.Logger
}

func New(log *slog.Logger) *Agent {
	if log == nil {
		log = slog.Default()
	}
	return &Agent{
		Commands: NewCommandRegistry("!"),
		Events:   NewEventBus(log),
		log:      log,
	}
}

// NewWithPrefix is the same as New but lets callers pick a non-default
// command prefix (e.g. "." or ".bot"). Useful for SaaS multi-tenancy where
// every agent owns its own prefix.
//
// Agents ship with no commands registered — all commands are user-defined
// and installed via the registry's dynamic path (ReplaceDynamic), wired by
// the runtime from the operator's / tenant's command set.
func NewWithPrefix(log *slog.Logger, prefix string) *Agent {
	a := New(log)
	a.Commands = NewCommandRegistry(prefix)
	return a
}

func (a *Agent) AddConnector(c Connector) {
	a.connectors = append(a.connectors, c)
}

// Log exposes the agent's logger for handlers + runtime composers that
// want to attribute log lines back to the agent's scope.
func (a *Agent) Log() *slog.Logger { return a.log }

// Run starts every connector, supervises their Run loops inside an
// errgroup, and drives the inbound envelope → CommandRegistry → outbound
// envelope pipeline. Returns when ctx cancels or any goroutine in the
// group errors. Stop is then called on every connector with a 500ms
// budget so SIGTERM unwinds quickly.
func (a *Agent) Run(ctx context.Context) error {
	a.Events.Publish(ctx, &Event{Type: EventBoot})

	for _, c := range a.connectors {
		if err := c.Start(ctx); err != nil {
			return err
		}
	}

	a.Events.Publish(ctx, &Event{Type: EventReady})

	g, gctx := errgroup.WithContext(ctx)

	for _, c := range a.connectors {
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
	a.Events.Publish(context.Background(), &Event{Type: EventShutdown})

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
			a.handle(ctx, c, env)
		}
	}
}

func (a *Agent) handle(ctx context.Context, c Connector, env *InboundEnvelope) {
	a.Events.Publish(ctx, &Event{
		Type:   EventMessage,
		Fields: map[string]any{"envelope": env},
	})

	out, err := a.Commands.Dispatch(ctx, env)
	if err != nil {
		a.log.Warn("command dispatch", "name", c.Name(), "command", env.Command, "err", err)
		return
	}
	if out == nil {
		return
	}

	a.Events.Publish(ctx, &Event{
		Type:   EventCommand,
		Fields: map[string]any{"envelope": env, "command": env.Command},
	})

	if err := c.Send(out); err != nil {
		a.log.Warn("connector send", "name", c.Name(), "err", err)
		return
	}
	a.Events.Publish(ctx, &Event{
		Type:   EventMessageSent,
		Fields: map[string]any{"envelope": out},
	})
}
