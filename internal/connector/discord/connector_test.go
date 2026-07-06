package discord

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bwmarrin/discordgo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/agent"
)

// fakeSession is an in-memory stand-in for *discordgo.Session so tests never
// open a real Gateway socket. It records the last call of each moderation /
// send method. opened/closed are atomic because Open/Close are driven from the
// connector's Run goroutine (connect/disconnect) while a test asserts on them.
type fakeSession struct {
	opened, closed atomic.Bool
	sends          []sentMsg
	replies        []sentReply
	kicks          []kickCall
	bans           []banCall
	topics         []topicCall
	failSend       bool
}

type sentMsg struct{ channel, content string }
type sentReply struct {
	channel, content string
	ref              *discordgo.MessageReference
}
type kickCall struct{ guild, user, reason string }
type banCall struct{ guild, user, reason string }
type topicCall struct{ channel, topic string }

func (f *fakeSession) Open() error  { f.opened.Store(true); return nil }
func (f *fakeSession) Close() error { f.closed.Store(true); return nil }

// wasOpened / wasClosed are the race-free readers for the atomic lifecycle flags.
func (f *fakeSession) wasOpened() bool { return f.opened.Load() }
func (f *fakeSession) wasClosed() bool { return f.closed.Load() }

func (f *fakeSession) ChannelMessageSend(channelID, content string, _ ...discordgo.RequestOption) (*discordgo.Message, error) {
	if f.failSend {
		return nil, errors.New("boom")
	}
	f.sends = append(f.sends, sentMsg{channelID, content})
	return &discordgo.Message{ID: "sent"}, nil
}

func (f *fakeSession) ChannelMessageSendReply(channelID, content string, ref *discordgo.MessageReference, _ ...discordgo.RequestOption) (*discordgo.Message, error) {
	f.replies = append(f.replies, sentReply{channelID, content, ref})
	return &discordgo.Message{ID: "reply"}, nil
}

func (f *fakeSession) GuildMemberDeleteWithReason(guildID, userID, reason string, _ ...discordgo.RequestOption) error {
	f.kicks = append(f.kicks, kickCall{guildID, userID, reason})
	return nil
}

func (f *fakeSession) GuildBanCreateWithReason(guildID, userID, reason string, _ int, _ ...discordgo.RequestOption) error {
	f.bans = append(f.bans, banCall{guildID, userID, reason})
	return nil
}

func (f *fakeSession) ChannelEditComplex(channelID string, data *discordgo.ChannelEdit, _ ...discordgo.RequestOption) (*discordgo.Channel, error) {
	f.topics = append(f.topics, topicCall{channelID, data.Topic})
	return &discordgo.Channel{}, nil
}

// newTestConn builds a connector wired to a fake session, with a known self id.
func newTestConn(t *testing.T, s *Settings) (*Connector, *fakeSession) {
	t.Helper()
	c := New(s, nil, agent.NewEventBus(nil))
	fs := &fakeSession{}
	c.session = fs
	c.selfID = "bot-self"
	c.selfName = "turbobot"
	t.Cleanup(func() { _ = c.Stop(context.Background()) })
	return c, fs
}

func TestInboundNormalization(t *testing.T) {
	c, _ := newTestConn(t, &Settings{GuildID: "g1"})
	c.ingest("chan-1", "user-9", "alice", "hello", "msg-100", "g1", false)

	select {
	case env := <-c.Inbound():
		assert.Equal(t, "discord", env.Connector)
		assert.Equal(t, "chan-1", env.Channel)
		assert.Equal(t, "alice", env.Sender)
		assert.Equal(t, "hello", env.Text)
		assert.False(t, env.IsDirect)
		assert.Equal(t, "msg-100", env.Metadata["message_id"])
		assert.Equal(t, "g1", env.Metadata["guild_id"])
		assert.Equal(t, "user-9", env.Metadata["author_id"])
	case <-time.After(time.Second):
		t.Fatal("no inbound envelope produced")
	}
}

func TestInboundDMIsDirect(t *testing.T) {
	c, _ := newTestConn(t, &Settings{GuildID: "g1"})
	c.ingest("dm-chan", "user-9", "alice", "hey", "m1", "", true)
	env := <-c.Inbound()
	assert.True(t, env.IsDirect, "a message with no guild is a DM")
}

func TestSelfMessageSkipped(t *testing.T) {
	c, _ := newTestConn(t, &Settings{GuildID: "g1"})
	c.ingest("chan-1", "bot-self", "turbobot", "my own message", "m1", "g1", false)
	c.ingest("chan-1", "user-9", "alice", "real", "m2", "g1", false)
	env := <-c.Inbound()
	assert.Equal(t, "real", env.Text, "the bot's own message must be skipped")
}

func TestChannelAllowListFilters(t *testing.T) {
	c, _ := newTestConn(t, &Settings{GuildID: "g1", Channels: []string{"allowed"}})
	c.ingest("blocked", "user-9", "alice", "nope", "m1", "g1", false)
	c.ingest("allowed", "user-9", "alice", "yes", "m2", "g1", false)
	env := <-c.Inbound()
	assert.Equal(t, "yes", env.Text, "only the allow-listed channel is delivered")
}

func TestAllowListBypassedForDMs(t *testing.T) {
	c, _ := newTestConn(t, &Settings{GuildID: "g1", Channels: []string{"allowed"}})
	c.ingest("dm-chan", "user-9", "alice", "dm", "m1", "", true)
	env := <-c.Inbound()
	assert.Equal(t, "dm", env.Text, "DMs bypass the channel allow-list")
}

func TestEmptyTextIgnored(t *testing.T) {
	c, _ := newTestConn(t, &Settings{GuildID: "g1"})
	c.ingest("chan-1", "user-9", "alice", "   ", "m1", "g1", false)
	c.ingest("chan-1", "user-9", "alice", "real", "m2", "g1", false)
	env := <-c.Inbound()
	assert.Equal(t, "real", env.Text)
}

func TestSendPlainMessage(t *testing.T) {
	c, fs := newTestConn(t, &Settings{GuildID: "g1"})
	require.NoError(t, c.Send(&agent.OutboundEnvelope{Channel: "chan-1", Text: "pong"}))
	require.Len(t, fs.sends, 1)
	assert.Equal(t, "chan-1", fs.sends[0].channel)
	assert.Equal(t, "pong", fs.sends[0].content)
	assert.Empty(t, fs.replies)
}

func TestSendReplyUsesStoredMessageID(t *testing.T) {
	c, fs := newTestConn(t, &Settings{GuildID: "g1"})
	// Ingest a message so its native id is remembered under the envelope id.
	c.ingest("chan-1", "user-9", "alice", "question?", "msg-777", "g1", false)
	env := <-c.Inbound()

	id := env.ID
	require.NoError(t, c.Send(&agent.OutboundEnvelope{Channel: "chan-1", Text: "answer", ReplyTo: &id}))
	require.Len(t, fs.replies, 1)
	assert.Equal(t, "msg-777", fs.replies[0].ref.MessageID)
	assert.Equal(t, "chan-1", fs.replies[0].ref.ChannelID)
	assert.Empty(t, fs.sends, "a resolvable ReplyTo threads the reply")
}

func TestSendReplyFallsBackWhenRefUnknown(t *testing.T) {
	c, fs := newTestConn(t, &Settings{GuildID: "g1"})
	unknown := agent.NewInbound("discord", "c", "s", "t").ID
	require.NoError(t, c.Send(&agent.OutboundEnvelope{Channel: "chan-1", Text: "hi", ReplyTo: &unknown}))
	assert.Len(t, fs.sends, 1, "an unresolvable ReplyTo falls back to a plain send")
	assert.Empty(t, fs.replies)
}

func TestSendErrorsWhenNotConnected(t *testing.T) {
	c := New(&Settings{GuildID: "g1"}, nil, agent.NewEventBus(nil))
	err := c.Send(&agent.OutboundEnvelope{Channel: "c", Text: "x"})
	assert.Error(t, err)
}

func TestActorModeration(t *testing.T) {
	c, fs := newTestConn(t, &Settings{GuildID: "g1"})
	a := NewActor(c)

	require.NoError(t, a.Say("chan-1", "hi"))
	require.Len(t, fs.sends, 1)

	require.NoError(t, a.Kick("chan-1", "user-42", "spam"))
	require.Len(t, fs.kicks, 1)
	assert.Equal(t, "g1", fs.kicks[0].guild)
	assert.Equal(t, "user-42", fs.kicks[0].user)
	assert.Equal(t, "spam", fs.kicks[0].reason)

	require.NoError(t, a.Ban("chan-1", "user-99"))
	require.Len(t, fs.bans, 1)
	assert.Equal(t, "user-99", fs.bans[0].user)

	require.NoError(t, a.Topic("chan-1", "new topic"))
	require.Len(t, fs.topics, 1)
	assert.Equal(t, "new topic", fs.topics[0].topic)

	assert.ErrorIs(t, a.SetMode("chan-1", "+m"), errUnsupported)
	assert.ErrorIs(t, a.Op("chan-1", "x"), errUnsupported)
	assert.ErrorIs(t, a.Voice("chan-1", "x"), errUnsupported)
	assert.ErrorIs(t, a.Invite("chan-1", "x"), errUnsupported)
}

func TestActorSayPublishesSent(t *testing.T) {
	c := New(&Settings{GuildID: "g1"}, nil, agent.NewEventBus(nil))
	fs := &fakeSession{}
	c.session = fs
	t.Cleanup(func() { _ = c.Stop(context.Background()) })

	sent := make(chan *agent.Event, 1)
	c.events.Subscribe(agent.EventMessageSent, func(_ context.Context, ev *agent.Event) { sent <- ev })

	require.NoError(t, NewActor(c).Say("chan-1", "output"))
	select {
	case ev := <-sent:
		assert.Equal(t, "chan-1", ev.Fields["channel"])
		assert.Equal(t, "output", ev.Fields["text"])
	case <-time.After(time.Second):
		t.Fatal("MESSAGE_SENT not published")
	}
}

func TestSuspendResumeLifecycle(t *testing.T) {
	c, fs := newTestConn(t, &Settings{GuildID: "g1"})
	require.NoError(t, c.Start(context.Background()))
	assert.True(t, fs.wasOpened(), "Start opens the (pre-injected) session")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	c.Suspend()
	require.Eventually(t, func() bool { return c.getSession() == nil }, time.Second, 10*time.Millisecond)

	cancel()
	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return on ctx cancel")
	}
}

func TestStartSuspendedSkipsConnect(t *testing.T) {
	c := New(&Settings{GuildID: "g1", Suspended: true}, nil, agent.NewEventBus(nil))
	fs := &fakeSession{}
	c.session = fs
	t.Cleanup(func() { _ = c.Stop(context.Background()) })
	require.NoError(t, c.Start(context.Background()))
	assert.False(t, fs.wasOpened(), "a suspended connector does not open on Start")
}

func TestNameAndSupervision(t *testing.T) {
	c, _ := newTestConn(t, &Settings{GuildID: "g1"})
	assert.Equal(t, "discord", c.Name())
	assert.True(t, c.ClaimSupervision())
	assert.Equal(t, "turbobot", c.BotName())
}

func TestSettingsValidate(t *testing.T) {
	assert.Error(t, (&Settings{}).Validate())
	assert.Error(t, (&Settings{Token: "t"}).Validate())
	assert.NoError(t, (&Settings{Token: "t", GuildID: "g"}).Validate())
}
