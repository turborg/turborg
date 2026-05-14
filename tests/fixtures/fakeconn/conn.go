// Package fakeconn is an in-memory Connector used by unit tests. It has no
// real I/O: feed pushes an InboundEnvelope into the channel the Agent
// drains, Sent returns every OutboundEnvelope the Agent has dispatched
// back, and the lifecycle methods are tracked so tests can assert
// Start/Stop ordering without timing flakes.
package fakeconn

import (
	"context"
	"sync"

	"github.com/turborg/turborg/internal/agent"
)

type Conn struct {
	NameStr     string
	SupervisedB bool

	inbox chan *agent.InboundEnvelope

	mu       sync.Mutex
	sent     []*agent.OutboundEnvelope
	started  bool
	stopped  bool
	inboxClosed bool
}

func New(name string) *Conn {
	return &Conn{
		NameStr:     name,
		SupervisedB: true,
		inbox:       make(chan *agent.InboundEnvelope, 16),
	}
}

func (c *Conn) Name() string                                { return c.NameStr }
func (c *Conn) Inbound() <-chan *agent.InboundEnvelope      { return c.inbox }
func (c *Conn) ClaimSupervision() bool                      { return c.SupervisedB }

func (c *Conn) Start(_ context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.started = true
	return nil
}

// Run blocks until ctx is cancelled. The fake connector has no I/O loop
// of its own — its job is to receive envelopes via Feed and surface
// Sent() to assertions.
func (c *Conn) Run(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

func (c *Conn) Stop(_ context.Context) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.stopped = true
	if !c.inboxClosed {
		close(c.inbox)
		c.inboxClosed = true
	}
	return nil
}

func (c *Conn) Send(env *agent.OutboundEnvelope) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sent = append(c.sent, env)
	return nil
}

func (c *Conn) Feed(env *agent.InboundEnvelope) {
	c.inbox <- env
}

// CloseInbound closes the inbox channel. Tests use this to exercise the
// drain-loop branch that returns when its inbound channel is closed
// (Agent.drain), without going through the full Stop() lifecycle.
func (c *Conn) CloseInbound() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !c.inboxClosed {
		close(c.inbox)
		c.inboxClosed = true
	}
}

func (c *Conn) Sent() []*agent.OutboundEnvelope {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*agent.OutboundEnvelope, len(c.sent))
	copy(out, c.sent)
	return out
}

func (c *Conn) Started() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.started
}

func (c *Conn) Stopped() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.stopped
}
