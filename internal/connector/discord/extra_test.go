package discord

import (
	"context"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/agent"
)

func TestOnMessageCreateNormalizes(t *testing.T) {
	c, _ := newTestConn(t, &Settings{GuildID: "g1"})
	c.onMessageCreate(nil, &discordgo.MessageCreate{Message: &discordgo.Message{
		ID:        "m1",
		ChannelID: "c1",
		Content:   "hi",
		GuildID:   "g1",
		Author:    &discordgo.User{ID: "u9", Username: "alice"},
	}})

	select {
	case env := <-c.Inbound():
		assert.Equal(t, "c1", env.Channel)
		assert.Equal(t, "alice", env.Sender)
		assert.Equal(t, "hi", env.Text)
	case <-time.After(time.Second):
		t.Fatal("onMessageCreate produced no inbound")
	}
}

func TestOnMessageCreateSkipsNilMessages(t *testing.T) {
	c, _ := newTestConn(t, &Settings{GuildID: "g1"})
	c.onMessageCreate(nil, nil)
	c.onMessageCreate(nil, &discordgo.MessageCreate{Message: nil})
	c.onMessageCreate(nil, &discordgo.MessageCreate{Message: &discordgo.Message{Author: nil}})
	// A real one afterwards must still be the first thing delivered.
	c.onMessageCreate(nil, &discordgo.MessageCreate{Message: &discordgo.Message{
		ChannelID: "c1", Content: "real", GuildID: "g1", Author: &discordgo.User{ID: "u9", Username: "alice"},
	}})
	assert.Equal(t, "real", (<-c.Inbound()).Text)
}

func TestAuthorNameFallsBackToID(t *testing.T) {
	assert.Equal(t, "", authorName(nil))
	assert.Equal(t, "u5", authorName(&discordgo.User{ID: "u5"}))
	assert.Equal(t, "bob", authorName(&discordgo.User{ID: "u5", Username: "bob"}))
}

func TestBotNameDefaultBeforeConnect(t *testing.T) {
	c := New(&Settings{GuildID: "g1"}, nil, agent.NewEventBus(nil))
	t.Cleanup(func() { _ = c.Stop(context.Background()) })
	assert.Equal(t, "turborg", c.BotName(), "no self name yet → neutral default")
}

func TestSetInitialSuspendedThenStartSkipsConnect(t *testing.T) {
	c := New(&Settings{GuildID: "g1"}, nil, agent.NewEventBus(nil))
	fs := &fakeSession{}
	c.session = fs
	t.Cleanup(func() { _ = c.Stop(context.Background()) })
	c.SetInitialSuspended(true)
	require.NoError(t, c.Start(context.Background()))
	assert.False(t, fs.opened, "a connector set suspended before Start does not open")
}

func TestSuspendResumeIdempotent(t *testing.T) {
	c, _ := newTestConn(t, &Settings{GuildID: "g1"})
	c.Suspend()
	c.Suspend() // second call is a no-op (already suspended)
	assert.True(t, c.isSuspended())
	assert.Nil(t, c.getSession(), "Suspend drops the live session")

	c.Resume()
	c.Resume() // second call is a no-op (not suspended)
	assert.False(t, c.isSuspended())
}

func TestRunReconnectsOnLifecycleSignal(t *testing.T) {
	c, fs := newTestConn(t, &Settings{GuildID: "g1"})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	// A lifecycle wake while not suspended re-opens the (pre-injected) session.
	c.signalLifecycle()
	require.Eventually(t, func() bool { return fs.opened }, time.Second, 10*time.Millisecond)

	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return on ctx cancel")
	}
}

func TestRememberRefEvictsOldest(t *testing.T) {
	c, _ := newTestConn(t, &Settings{GuildID: "g1"})
	// Push more than the cap so the oldest entry is evicted; the map stays bounded.
	for i := 0; i < replyRefCap+10; i++ {
		c.rememberRef(agent.NewInbound("discord", "c", "s", "t").ID, replyRef{messageID: "x"})
	}
	c.refMu.Lock()
	n := len(c.refs)
	c.refMu.Unlock()
	assert.LessOrEqual(t, n, replyRefCap)
}

func TestPublishSentNoBusIsNoop(t *testing.T) {
	// A nil event bus must not panic when the Actor mirrors its output.
	c := New(&Settings{GuildID: "g1"}, nil, nil)
	fs := &fakeSession{}
	c.session = fs
	t.Cleanup(func() { _ = c.Stop(context.Background()) })
	require.NoError(t, NewActor(c).Say("c1", "hi"))
}

func TestActorNotConnected(t *testing.T) {
	c := New(&Settings{GuildID: "g1"}, nil, agent.NewEventBus(nil))
	t.Cleanup(func() { _ = c.Stop(context.Background()) })
	a := NewActor(c)
	assert.ErrorIs(t, a.Say("c", "x"), errNotConnected)
	assert.ErrorIs(t, a.Notice("c", "x"), errNotConnected)
	assert.ErrorIs(t, a.Kick("c", "u", "r"), errNotConnected)
	assert.ErrorIs(t, a.Ban("c", "u"), errNotConnected)
	assert.ErrorIs(t, a.Topic("c", "t"), errNotConnected)
}

func TestActorNoticeDelegatesToSay(t *testing.T) {
	c, fs := newTestConn(t, &Settings{GuildID: "g1"})
	require.NoError(t, NewActor(c).Notice("c1", "note"))
	require.Len(t, fs.sends, 1)
	assert.Equal(t, "note", fs.sends[0].content)
}

func TestActorSayPropagatesSendError(t *testing.T) {
	c, fs := newTestConn(t, &Settings{GuildID: "g1"})
	fs.failSend = true
	assert.Error(t, NewActor(c).Say("c1", "x"))
}

func TestStopIsIdempotent(t *testing.T) {
	c, _ := newTestConn(t, &Settings{GuildID: "g1"})
	require.NoError(t, c.Stop(context.Background()), "first Stop closes the live session")
	require.NoError(t, c.Stop(context.Background()), "second Stop is a no-op")
}
