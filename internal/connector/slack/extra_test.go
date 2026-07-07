package slack

import (
	"context"
	"testing"
	"time"

	"github.com/slack-go/slack/slackevents"
	"github.com/slack-go/slack/socketmode"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/agent"
)

// fakeSocket is an in-memory stand-in for *socketmode.Client so tests never
// open a real Socket Mode WebSocket. RunContext blocks until ctx cancel.
type fakeSocket struct {
	acks []socketmode.Request
}

func (f *fakeSocket) RunContext(ctx context.Context) error {
	<-ctx.Done()
	return nil
}

func (f *fakeSocket) Ack(req socketmode.Request, _ ...any) error {
	f.acks = append(f.acks, req)
	return nil
}

// messageEvent builds a Socket Mode EventsAPI message event for the given text.
func messageEvent(channel, user, botID, text, channelType string) socketmode.Event {
	req := socketmode.Request{}
	return socketmode.Event{
		Type:    socketmode.EventTypeEventsAPI,
		Request: &req,
		Data: slackevents.EventsAPIEvent{
			Type: slackevents.CallbackEvent,
			InnerEvent: slackevents.EventsAPIInnerEvent{
				Data: &slackevents.MessageEvent{
					Channel:     channel,
					User:        user,
					BotID:       botID,
					Text:        text,
					TimeStamp:   "1700000000.000100",
					ChannelType: channelType,
				},
			},
		},
	}
}

func TestHandleSocketEventDeliversMessage(t *testing.T) {
	c, _ := newTestConn(t, &Settings{})
	fs := &fakeSocket{}
	c.handleSocketEvent(fs, messageEvent("C1", "U9", "", "hi", "channel"))

	select {
	case env := <-c.Inbound():
		assert.Equal(t, "hi", env.Text)
		assert.Equal(t, "C1", env.Channel)
	case <-time.After(time.Second):
		t.Fatal("no inbound from a message event")
	}
	require.Len(t, fs.acks, 1, "the Socket Mode request must be acked")
}

func TestHandleSocketEventIgnoresNonMessage(t *testing.T) {
	c, _ := newTestConn(t, &Settings{})
	fs := &fakeSocket{}

	// Not an EventsAPI event at all.
	c.handleSocketEvent(fs, socketmode.Event{Type: socketmode.EventTypeConnected})
	// EventsAPI wrapper but Data isn't an EventsAPIEvent.
	c.handleSocketEvent(fs, socketmode.Event{Type: socketmode.EventTypeEventsAPI, Data: "nope"})
	// EventsAPI but not a callback.
	c.handleSocketEvent(fs, socketmode.Event{
		Type: socketmode.EventTypeEventsAPI,
		Data: slackevents.EventsAPIEvent{Type: slackevents.URLVerification},
	})

	select {
	case env := <-c.Inbound():
		t.Fatalf("unexpected inbound: %q", env.Text)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestHandleSocketEventSuspendedDrops(t *testing.T) {
	c, _ := newTestConn(t, &Settings{})
	c.Suspend()
	c.handleSocketEvent(&fakeSocket{}, messageEvent("C1", "U9", "", "hi", "channel"))
	select {
	case env := <-c.Inbound():
		t.Fatalf("suspended connector should drop inbound, got %q", env.Text)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestRunConsumesSocketEvents(t *testing.T) {
	c, _ := newTestConn(t, &Settings{})
	fs := &fakeSocket{}
	evCh := make(chan socketmode.Event, 4)
	c.mu.Lock()
	c.sm = fs
	c.events2 = evCh
	c.mu.Unlock()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	evCh <- messageEvent("C1", "U9", "", "hello", "channel")
	select {
	case env := <-c.Inbound():
		assert.Equal(t, "hello", env.Text)
	case <-time.After(time.Second):
		t.Fatal("consume loop did not deliver the event")
	}

	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return on ctx cancel")
	}
}

func TestIngestSkipsEmptyText(t *testing.T) {
	c, _ := newTestConn(t, &Settings{})
	c.ingest("C1", "U9", "", "   ", "1.1", false)
	c.ingest("C1", "U9", "", "real", "1.2", false)
	assert.Equal(t, "real", (<-c.Inbound()).Text)
}

func TestBotNameDefault(t *testing.T) {
	c := New(&Settings{}, nil, agent.NewEventBus(nil))
	t.Cleanup(func() { _ = c.Stop(context.Background()) })
	assert.Equal(t, "turborg", c.BotName())
}

func TestLifecycleToggles(t *testing.T) {
	c := New(&Settings{}, nil, agent.NewEventBus(nil))
	t.Cleanup(func() { _ = c.Stop(context.Background()) })
	assert.False(t, c.isSuspended())
	c.SetInitialSuspended(true)
	assert.True(t, c.isSuspended())
	c.Resume()
	assert.False(t, c.isSuspended())
	c.Suspend()
	assert.True(t, c.isSuspended())
}

func TestStartSuspendedIsNoop(t *testing.T) {
	c := New(&Settings{Suspended: true}, nil, agent.NewEventBus(nil))
	t.Cleanup(func() { _ = c.Stop(context.Background()) })
	require.NoError(t, c.Start(context.Background()))
	assert.Nil(t, c.getAPI())
}

func TestStartPreWiredIsNoop(t *testing.T) {
	c, _ := newTestConn(t, &Settings{}) // fake api injected
	require.NoError(t, c.Start(context.Background()))
}

func TestStopIsIdempotent(t *testing.T) {
	c, _ := newTestConn(t, &Settings{})
	require.NoError(t, c.Stop(context.Background()))
	require.NoError(t, c.Stop(context.Background()))
}

func TestRememberRefEvictsOldest(t *testing.T) {
	c, _ := newTestConn(t, &Settings{})
	for i := 0; i < replyRefCap+10; i++ {
		c.rememberRef(agent.NewInbound("slack", "c", "s", "t").ID, replyRef{ts: "1.1"})
	}
	c.refMu.Lock()
	n := len(c.refs)
	c.refMu.Unlock()
	assert.LessOrEqual(t, n, replyRefCap)
}

func TestPublishSentNoBusIsNoop(t *testing.T) {
	c := New(&Settings{}, nil, nil)
	c.api = &fakeAPI{}
	t.Cleanup(func() { _ = c.Stop(context.Background()) })
	require.NoError(t, NewActor(c).Say("C1", "hi"))
}

func TestActorNotConnected(t *testing.T) {
	c := New(&Settings{}, nil, agent.NewEventBus(nil))
	t.Cleanup(func() { _ = c.Stop(context.Background()) })
	a := NewActor(c)
	assert.ErrorIs(t, a.Say("C1", "x"), errNotConnected)
	assert.ErrorIs(t, a.Notice("C1", "x"), errNotConnected)
	assert.ErrorIs(t, a.Kick("C1", "U9", "r"), errNotConnected)
	assert.ErrorIs(t, a.Topic("C1", "t"), errNotConnected)
	assert.ErrorIs(t, a.Invite("C1", "U9"), errNotConnected)
}

func TestActorSayPropagatesPostError(t *testing.T) {
	c, fa := newTestConn(t, &Settings{})
	fa.failPost = true
	assert.Error(t, NewActor(c).Say("C1", "x"))
}

func TestConsumeReturnsOnClosedChannel(t *testing.T) {
	c, _ := newTestConn(t, &Settings{})
	evCh := make(chan socketmode.Event)
	close(evCh)
	done := make(chan struct{})
	go func() {
		c.consume(context.Background(), &fakeSocket{}, evCh)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("consume did not return when the event channel closed")
	}
}
