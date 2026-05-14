package agent_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/agent"
	"github.com/turborg/turborg/tests/fixtures/fakeconn"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestAgentBuiltinPing(t *testing.T) {
	a := agent.New(nil)
	c := fakeconn.New("fake")
	a.AddConnector(c)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	c.Feed(agent.NewInbound("fake", "#ch", "alice", "!ping"))

	require.Eventually(t, func() bool { return len(c.Sent()) == 1 }, 1*time.Second, 10*time.Millisecond)

	sent := c.Sent()
	assert.Equal(t, "pong", sent[0].Text)
	assert.Equal(t, "#ch", sent[0].Channel)

	cancel()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("unexpected agent error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("agent did not shut down")
	}
	assert.True(t, c.Started())
	assert.True(t, c.Stopped())
}

func TestAgentPublishesLifecycleEvents(t *testing.T) {
	a := agent.New(nil)
	c := fakeconn.New("fake")
	a.AddConnector(c)

	var boot, ready, shutdown atomic.Int32
	a.Events.Subscribe(agent.EventBoot, func(context.Context, *agent.Event) { boot.Add(1) })
	a.Events.Subscribe(agent.EventReady, func(context.Context, *agent.Event) { ready.Add(1) })
	a.Events.Subscribe(agent.EventShutdown, func(context.Context, *agent.Event) { shutdown.Add(1) })

	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = a.Run(ctx) }()

	require.Eventually(t, func() bool { return ready.Load() == 1 }, 1*time.Second, 10*time.Millisecond)

	cancel()
	require.Eventually(t, func() bool { return shutdown.Load() == 1 }, 1*time.Second, 10*time.Millisecond)

	assert.Equal(t, int32(1), boot.Load())
	assert.Equal(t, int32(1), ready.Load())
	assert.Equal(t, int32(1), shutdown.Load())
}

func TestAgentPublishesMessageEventForNonCommandText(t *testing.T) {
	a := agent.New(nil)
	c := fakeconn.New("fake")
	a.AddConnector(c)

	var msgCount atomic.Int32
	a.Events.Subscribe(agent.EventMessage, func(_ context.Context, _ *agent.Event) {
		msgCount.Add(1)
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = a.Run(ctx) }()

	c.Feed(agent.NewInbound("fake", "#ch", "alice", "just chatting"))

	require.Eventually(t, func() bool { return msgCount.Load() == 1 }, 1*time.Second, 10*time.Millisecond)
	assert.Empty(t, c.Sent(), "non-command text must not produce an outbound reply")
}

func TestAgentStartErrorAbortsRun(t *testing.T) {
	a := agent.New(nil)
	a.AddConnector(&failingStart{})

	err := a.Run(context.Background())
	require.Error(t, err)
}

func TestAgentNewWithPrefix(t *testing.T) {
	a := agent.NewWithPrefix(nil, ".")
	assert.Equal(t, ".", a.Commands.Prefix())
	assert.Contains(t, a.Commands.Names(), "ping")
}

func TestAgentBuiltinHelp(t *testing.T) {
	a := agent.New(nil)
	c := fakeconn.New("fake")
	a.AddConnector(c)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = a.Run(ctx) }()

	c.Feed(agent.NewInbound("fake", "#ch", "alice", "!help"))
	require.Eventually(t, func() bool { return len(c.Sent()) == 1 }, 1*time.Second, 10*time.Millisecond)

	sent := c.Sent()[0]
	assert.Contains(t, sent.Text, "ping")
	assert.Contains(t, sent.Text, "help")
}

func TestAgentSwallowsHandlerError(t *testing.T) {
	a := agent.New(nil)
	a.Commands.Register("boom", func(context.Context, *agent.InboundEnvelope, []string) (*agent.OutboundEnvelope, error) {
		return nil, errors.New("kaboom")
	}, nil)
	c := fakeconn.New("fake")
	a.AddConnector(c)

	var msgs atomic.Int32
	a.Events.Subscribe(agent.EventMessage, func(context.Context, *agent.Event) { msgs.Add(1) })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = a.Run(ctx) }()

	c.Feed(agent.NewInbound("fake", "#ch", "alice", "!boom"))
	require.Eventually(t, func() bool { return msgs.Load() == 1 }, 1*time.Second, 10*time.Millisecond)

	assert.Empty(t, c.Sent(), "handler error must not produce an outbound message")
}

func TestAgentLogsSendError(t *testing.T) {
	a := agent.New(nil)
	c := &sendFails{Conn: fakeconn.New("sendfail")}
	a.AddConnector(c)

	var sent atomic.Int32
	a.Events.Subscribe(agent.EventMessageSent, func(context.Context, *agent.Event) { sent.Add(1) })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = a.Run(ctx) }()

	c.Feed(agent.NewInbound("sendfail", "#ch", "alice", "!ping"))

	// Send error should be logged; MESSAGE_SENT should not fire.
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, int32(0), sent.Load(),
		"MESSAGE_SENT must not fire when the connector rejects the outbound")
}

func TestAgentLogReturnsConfiguredLogger(t *testing.T) {
	a := agent.New(nil)
	require.NotNil(t, a.Log(), "Log() must return a non-nil logger even when New(nil) was called")
}

func TestAgentDrainReturnsOnInboundChannelClose(t *testing.T) {
	// drain's select races between two ready branches when CloseInbound
	// AND cancel() fire back-to-back: closed-channel-recv and ctx.Done
	// are both selectable, and Go picks pseudo-randomly. Earlier this
	// test cancelled before drain could observe the close, so coverage
	// of the `if !ok { return nil }` branch was non-deterministic — it
	// passed locally and on most CI runs, then failed on main after a
	// merge with unlucky scheduling.
	//
	// Sequence here forces drain through the !ok branch: close the
	// inbox while ctx is still live, give drain a tick to observe the
	// close and return, then cancel to unwind the connector's Run loop.
	a := agent.New(nil)
	c := fakeconn.New("closer")
	a.AddConnector(c)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	require.Eventually(t, func() bool { return c.Started() },
		time.Second, 10*time.Millisecond)

	// Step 1: close the inbox. ctx is still live, so drain's select
	// has exactly one ready case — the closed channel — and reads !ok
	// deterministically.
	c.CloseInbound()

	// Step 2: give drain a beat to observe the close. A closed-channel
	// select wakes up in microseconds; 50ms is overkill on any runner.
	time.Sleep(50 * time.Millisecond)

	// Step 3: unwind the connector's Run (which blocks on ctx.Done).
	cancel()

	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("unexpected error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Agent did not return after inbound close + ctx cancel")
	}
}

type failingStart struct{ fakeconn.Conn }

func (f *failingStart) Name() string                                { return "failing" }
func (f *failingStart) Start(_ context.Context) error               { return errors.New("nope") }
func (f *failingStart) Run(_ context.Context) error                 { return nil }
func (f *failingStart) Stop(_ context.Context) error                { return nil }
func (f *failingStart) Inbound() <-chan *agent.InboundEnvelope      { return nil }
func (f *failingStart) Send(_ *agent.OutboundEnvelope) error        { return nil }
func (f *failingStart) ClaimSupervision() bool                      { return false }

type sendFails struct {
	*fakeconn.Conn
}

func (s *sendFails) Send(_ *agent.OutboundEnvelope) error {
	return errors.New("send broken")
}
