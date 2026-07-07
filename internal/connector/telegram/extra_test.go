package telegram

import (
	"context"
	"testing"
	"time"

	tgbotapi "github.com/go-telegram-bot-api/telegram-bot-api/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/agent"
)

func TestOnUpdateNormalizes(t *testing.T) {
	c, _ := newTestConn(t, &Settings{})
	c.onUpdate(tgbotapi.Update{Message: &tgbotapi.Message{
		MessageID: 7,
		From:      &tgbotapi.User{ID: 9, UserName: "alice"},
		Chat:      &tgbotapi.Chat{ID: 1001, Type: "group"},
		Text:      "hi",
	}})

	select {
	case env := <-c.Inbound():
		assert.Equal(t, "1001", env.Channel)
		assert.Equal(t, "alice", env.Sender)
		assert.Equal(t, "hi", env.Text)
		assert.False(t, env.IsDirect)
	case <-time.After(time.Second):
		t.Fatal("onUpdate produced no inbound")
	}
}

func TestOnUpdateSkipsIncomplete(t *testing.T) {
	c, _ := newTestConn(t, &Settings{})
	c.onUpdate(tgbotapi.Update{Message: nil})
	c.onUpdate(tgbotapi.Update{Message: &tgbotapi.Message{From: nil, Chat: &tgbotapi.Chat{ID: 1}}})
	c.onUpdate(tgbotapi.Update{Message: &tgbotapi.Message{From: &tgbotapi.User{ID: 1}, Chat: nil}})
	c.onUpdate(tgbotapi.Update{Message: &tgbotapi.Message{
		MessageID: 1, From: &tgbotapi.User{ID: 9, UserName: "alice"},
		Chat: &tgbotapi.Chat{ID: 1001, Type: "private"}, Text: "real",
	}})
	env := <-c.Inbound()
	assert.Equal(t, "real", env.Text)
	assert.True(t, env.IsDirect, "a private chat is a DM")
}

func TestSenderNamePrefers(t *testing.T) {
	assert.Equal(t, "", senderName(nil))
	assert.Equal(t, "handle", senderName(&tgbotapi.User{ID: 5, UserName: "handle", FirstName: "First"}))
	assert.Equal(t, "First", senderName(&tgbotapi.User{ID: 5, FirstName: "First"}))
	assert.Equal(t, "5", senderName(&tgbotapi.User{ID: 5}))
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
	require.NoError(t, c.Start(context.Background()), "a suspended connector starts without opening a client")
	assert.Nil(t, c.getAPI())
}

func TestStartPreWiredIsNoop(t *testing.T) {
	c, _ := newTestConn(t, &Settings{}) // newTestConn injects a fake api
	require.NoError(t, c.Start(context.Background()), "a pre-wired connector does not build a real client")
}

func TestStopIsIdempotent(t *testing.T) {
	c, _ := newTestConn(t, &Settings{})
	require.NoError(t, c.Stop(context.Background()))
	require.NoError(t, c.Stop(context.Background()))
}

func TestRememberRefEvictsOldest(t *testing.T) {
	c, _ := newTestConn(t, &Settings{})
	for i := 0; i < replyRefCap+10; i++ {
		c.rememberRef(agent.NewInbound("telegram", "c", "s", "t").ID, replyRef{messageID: 1})
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
	require.NoError(t, NewActor(c).Say("1001", "hi"))
}

func TestActorNotConnected(t *testing.T) {
	c := New(&Settings{}, nil, agent.NewEventBus(nil))
	t.Cleanup(func() { _ = c.Stop(context.Background()) })
	a := NewActor(c)
	assert.ErrorIs(t, a.Say("1", "x"), errNotConnected)
	assert.ErrorIs(t, a.Notice("1", "x"), errNotConnected)
	assert.ErrorIs(t, a.Kick("1", "9", "r"), errNotConnected)
	assert.ErrorIs(t, a.Ban("1", "9"), errNotConnected)
}

func TestActorInvalidIDs(t *testing.T) {
	c, _ := newTestConn(t, &Settings{})
	a := NewActor(c)
	assert.Error(t, a.Say("not-a-number", "x"))
	assert.Error(t, a.Kick("not-a-number", "9", "r"))
	assert.Error(t, a.Kick("1001", "not-a-number", "r"))
}

func TestActorSayPropagatesSendError(t *testing.T) {
	c, fa := newTestConn(t, &Settings{})
	fa.failSend = true
	assert.Error(t, NewActor(c).Say("1001", "x"), "a Send failure surfaces from the Actor")
}

func TestIngestSkipsEmptyText(t *testing.T) {
	c, _ := newTestConn(t, &Settings{})
	c.ingest(1001, 9, "alice", "   ", 1, false)
	c.ingest(1001, 9, "alice", "real", 2, false)
	assert.Equal(t, "real", (<-c.Inbound()).Text)
}
