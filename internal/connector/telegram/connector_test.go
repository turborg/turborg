package telegram

import (
	"context"
	"errors"
	"testing"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/agent"
)

// fakeAPI is an in-memory stand-in for *tgbotapi.BotAPI so tests never hit the
// real Bot API.
type fakeAPI struct {
	sends    []tgbotapi.MessageConfig
	requests []tgbotapi.Chattable
	failSend bool
}

func (f *fakeAPI) Send(c tgbotapi.Chattable) (tgbotapi.Message, error) {
	if f.failSend {
		return tgbotapi.Message{}, errors.New("boom")
	}
	if m, ok := c.(tgbotapi.MessageConfig); ok {
		f.sends = append(f.sends, m)
	}
	return tgbotapi.Message{}, nil
}

func (f *fakeAPI) Request(c tgbotapi.Chattable) (*tgbotapi.APIResponse, error) {
	f.requests = append(f.requests, c)
	return &tgbotapi.APIResponse{Ok: true}, nil
}

func newTestConn(t *testing.T, s *Settings) (*Connector, *fakeAPI) {
	t.Helper()
	c := New(s, nil, agent.NewEventBus(nil))
	fa := &fakeAPI{}
	c.api = fa
	c.selfID = 42
	c.selfName = "turbobot"
	t.Cleanup(func() { _ = c.Stop(context.Background()) })
	return c, fa
}

func TestInboundNormalization(t *testing.T) {
	c, _ := newTestConn(t, &Settings{})
	c.ingest(1001, 9, "alice", "hello", 500, false)

	select {
	case env := <-c.Inbound():
		assert.Equal(t, "telegram", env.Connector)
		assert.Equal(t, "1001", env.Channel)
		assert.Equal(t, "alice", env.Sender)
		assert.Equal(t, "hello", env.Text)
		assert.False(t, env.IsDirect)
		assert.Equal(t, "500", env.Metadata["message_id"])
		assert.Equal(t, "1001", env.Metadata["chat_id"])
		assert.Equal(t, "9", env.Metadata["sender_id"])
	case <-time.After(time.Second):
		t.Fatal("no inbound envelope produced")
	}
}

func TestInboundPrivateChatIsDirect(t *testing.T) {
	c, _ := newTestConn(t, &Settings{})
	c.ingest(1001, 9, "alice", "hey", 1, true)
	env := <-c.Inbound()
	assert.True(t, env.IsDirect, "a private chat message is a DM")
}

func TestSelfMessageSkipped(t *testing.T) {
	c, _ := newTestConn(t, &Settings{})
	c.ingest(1001, 42, "turbobot", "self", 1, false)
	c.ingest(1001, 9, "alice", "real", 2, false)
	env := <-c.Inbound()
	assert.Equal(t, "real", env.Text)
}

func TestChatAllowListFilters(t *testing.T) {
	c, _ := newTestConn(t, &Settings{Chats: []string{"1001"}})
	c.ingest(2002, 9, "alice", "nope", 1, false)
	c.ingest(1001, 9, "alice", "yes", 2, false)
	env := <-c.Inbound()
	assert.Equal(t, "yes", env.Text)
}

func TestAllowListBypassedForDMs(t *testing.T) {
	c, _ := newTestConn(t, &Settings{Chats: []string{"1001"}})
	c.ingest(9999, 9, "alice", "dm", 1, true)
	env := <-c.Inbound()
	assert.Equal(t, "dm", env.Text)
}

func TestSendPlainMessage(t *testing.T) {
	c, fa := newTestConn(t, &Settings{})
	require.NoError(t, c.Send(&agent.OutboundEnvelope{Channel: "1001", Text: "pong"}))
	require.Len(t, fa.sends, 1)
	assert.Equal(t, int64(1001), fa.sends[0].ChatID)
	assert.Equal(t, "pong", fa.sends[0].Text)
	assert.Zero(t, fa.sends[0].ReplyToMessageID)
}

func TestSendReplyUsesStoredMessageID(t *testing.T) {
	c, fa := newTestConn(t, &Settings{})
	c.ingest(1001, 9, "alice", "question?", 777, false)
	env := <-c.Inbound()

	id := env.ID
	require.NoError(t, c.Send(&agent.OutboundEnvelope{Channel: "1001", Text: "answer", ReplyTo: &id}))
	require.Len(t, fa.sends, 1)
	assert.Equal(t, 777, fa.sends[0].ReplyToMessageID)
}

func TestSendInvalidChatID(t *testing.T) {
	c, _ := newTestConn(t, &Settings{})
	assert.Error(t, c.Send(&agent.OutboundEnvelope{Channel: "not-a-number", Text: "x"}))
}

func TestSendErrorsWhenNotConnected(t *testing.T) {
	c := New(&Settings{}, nil, agent.NewEventBus(nil))
	assert.Error(t, c.Send(&agent.OutboundEnvelope{Channel: "1", Text: "x"}))
}

func TestActorModeration(t *testing.T) {
	c, fa := newTestConn(t, &Settings{})
	a := NewActor(c)

	require.NoError(t, a.Say("1001", "hi"))
	require.Len(t, fa.sends, 1)

	require.NoError(t, a.Kick("1001", "9", "spam"))
	require.NoError(t, a.Ban("1001", "10"))
	require.Len(t, fa.requests, 2)
	kick, ok := fa.requests[0].(tgbotapi.BanChatMemberConfig)
	require.True(t, ok)
	assert.Equal(t, int64(1001), kick.ChatID)
	assert.Equal(t, int64(9), kick.UserID)
	assert.False(t, kick.RevokeMessages)
	ban := fa.requests[1].(tgbotapi.BanChatMemberConfig)
	assert.True(t, ban.RevokeMessages)

	assert.ErrorIs(t, a.SetMode("1001", "+m"), errUnsupported)
	assert.ErrorIs(t, a.Op("1001", "x"), errUnsupported)
	assert.ErrorIs(t, a.Voice("1001", "x"), errUnsupported)
	assert.ErrorIs(t, a.Topic("1001", "t"), errUnsupported)
	assert.ErrorIs(t, a.Invite("1001", "x"), errUnsupported)
}

func TestActorSayPublishesSent(t *testing.T) {
	c := New(&Settings{}, nil, agent.NewEventBus(nil))
	c.api = &fakeAPI{}
	t.Cleanup(func() { _ = c.Stop(context.Background()) })

	sent := make(chan *agent.Event, 1)
	c.events.Subscribe(agent.EventMessageSent, func(_ context.Context, ev *agent.Event) { sent <- ev })

	require.NoError(t, NewActor(c).Say("1001", "output"))
	select {
	case ev := <-sent:
		assert.Equal(t, "1001", ev.Fields["channel"])
		assert.Equal(t, "output", ev.Fields["text"])
	case <-time.After(time.Second):
		t.Fatal("MESSAGE_SENT not published")
	}
}

func TestRunReturnsOnCtxCancelWithoutBot(t *testing.T) {
	c, _ := newTestConn(t, &Settings{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()
	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return on ctx cancel")
	}
}

func TestNameAndSupervision(t *testing.T) {
	c, _ := newTestConn(t, &Settings{})
	assert.Equal(t, "telegram", c.Name())
	assert.True(t, c.ClaimSupervision())
	assert.Equal(t, "turbobot", c.BotName())
}

func TestSettingsValidate(t *testing.T) {
	assert.Error(t, (&Settings{}).Validate())
	assert.NoError(t, (&Settings{Token: "t"}).Validate())
}
