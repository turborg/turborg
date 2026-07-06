package slack

import (
	"context"
	"errors"
	"testing"
	"time"

	slackapi "github.com/slack-go/slack"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/agent"
)

// fakeAPI is an in-memory stand-in for *slackapi.Client so tests never hit the
// real Web API.
type fakeAPI struct {
	posts    []postCall
	kicks    []kickCall
	topics   []topicCall
	invites  []inviteCall
	failPost bool
}

type postCall struct {
	channel string
	ts      string // captured from MsgOptionTS when present
	text    string
}
type kickCall struct{ channel, user string }
type topicCall struct{ channel, topic string }
type inviteCall struct {
	channel string
	users   []string
}

func (f *fakeAPI) PostMessage(channelID string, options ...slackapi.MsgOption) (string, string, error) {
	if f.failPost {
		return "", "", errors.New("boom")
	}
	// Reconstruct the composed message so the test can assert text + thread ts.
	_, values, _ := slackapi.UnsafeApplyMsgOptions("token", channelID, "https://slack.test/", options...)
	f.posts = append(f.posts, postCall{channel: channelID, ts: values.Get("thread_ts"), text: values.Get("text")})
	return channelID, "1700000000.000100", nil
}

func (f *fakeAPI) KickUserFromConversation(channelID, user string) error {
	f.kicks = append(f.kicks, kickCall{channelID, user})
	return nil
}

func (f *fakeAPI) SetTopicOfConversation(channelID, topic string) (*slackapi.Channel, error) {
	f.topics = append(f.topics, topicCall{channelID, topic})
	return &slackapi.Channel{}, nil
}

func (f *fakeAPI) InviteUsersToConversation(channelID string, users ...string) (*slackapi.Channel, error) {
	f.invites = append(f.invites, inviteCall{channelID, users})
	return &slackapi.Channel{}, nil
}

func newTestConn(t *testing.T, s *Settings) (*Connector, *fakeAPI) {
	t.Helper()
	c := New(s, nil, agent.NewEventBus(nil))
	fa := &fakeAPI{}
	c.api = fa
	c.selfID = "UBOT"
	c.selfName = "turbobot"
	t.Cleanup(func() { _ = c.Stop(context.Background()) })
	return c, fa
}

func TestInboundNormalization(t *testing.T) {
	c, _ := newTestConn(t, &Settings{})
	c.ingest("C123", "U9", "", "hello", "1700000000.000200", false)

	select {
	case env := <-c.Inbound():
		assert.Equal(t, "slack", env.Connector)
		assert.Equal(t, "C123", env.Channel)
		assert.Equal(t, "U9", env.Sender)
		assert.Equal(t, "hello", env.Text)
		assert.False(t, env.IsDirect)
		assert.Equal(t, "1700000000.000200", env.Metadata["message_ts"])
		assert.Equal(t, "U9", env.Metadata["user_id"])
	case <-time.After(time.Second):
		t.Fatal("no inbound envelope produced")
	}
}

func TestInboundDMIsDirect(t *testing.T) {
	c, _ := newTestConn(t, &Settings{})
	c.ingest("D123", "U9", "", "hey", "1.1", true)
	env := <-c.Inbound()
	assert.True(t, env.IsDirect)
}

func TestBotMessageSkipped(t *testing.T) {
	c, _ := newTestConn(t, &Settings{})
	c.ingest("C123", "U9", "B999", "from a bot", "1.1", false) // BotID set → skip
	c.ingest("C123", "U9", "", "real", "1.2", false)
	env := <-c.Inbound()
	assert.Equal(t, "real", env.Text)
}

func TestSelfMessageSkipped(t *testing.T) {
	c, _ := newTestConn(t, &Settings{})
	c.ingest("C123", "UBOT", "", "my own", "1.1", false)
	c.ingest("C123", "U9", "", "real", "1.2", false)
	env := <-c.Inbound()
	assert.Equal(t, "real", env.Text)
}

func TestChannelAllowListFilters(t *testing.T) {
	c, _ := newTestConn(t, &Settings{Channels: []string{"C-allowed"}})
	c.ingest("C-blocked", "U9", "", "nope", "1.1", false)
	c.ingest("C-allowed", "U9", "", "yes", "1.2", false)
	env := <-c.Inbound()
	assert.Equal(t, "yes", env.Text)
}

func TestAllowListBypassedForDMs(t *testing.T) {
	c, _ := newTestConn(t, &Settings{Channels: []string{"C-allowed"}})
	c.ingest("D999", "U9", "", "dm", "1.1", true)
	env := <-c.Inbound()
	assert.Equal(t, "dm", env.Text)
}

func TestSendPlainMessage(t *testing.T) {
	c, fa := newTestConn(t, &Settings{})
	require.NoError(t, c.Send(&agent.OutboundEnvelope{Channel: "C123", Text: "pong"}))
	require.Len(t, fa.posts, 1)
	assert.Equal(t, "C123", fa.posts[0].channel)
	assert.Equal(t, "pong", fa.posts[0].text)
	assert.Empty(t, fa.posts[0].ts, "a plain send carries no thread ts")
}

func TestSendReplyUsesStoredTS(t *testing.T) {
	c, fa := newTestConn(t, &Settings{})
	c.ingest("C123", "U9", "", "question?", "1700000000.000777", false)
	env := <-c.Inbound()

	id := env.ID
	require.NoError(t, c.Send(&agent.OutboundEnvelope{Channel: "C123", Text: "answer", ReplyTo: &id}))
	require.Len(t, fa.posts, 1)
	assert.Equal(t, "1700000000.000777", fa.posts[0].ts, "a resolvable ReplyTo threads on the source ts")
}

func TestSendErrorsWhenNotConnected(t *testing.T) {
	c := New(&Settings{}, nil, agent.NewEventBus(nil))
	assert.Error(t, c.Send(&agent.OutboundEnvelope{Channel: "C1", Text: "x"}))
}

func TestActorModeration(t *testing.T) {
	c, fa := newTestConn(t, &Settings{})
	a := NewActor(c)

	require.NoError(t, a.Say("C123", "hi"))
	require.Len(t, fa.posts, 1)

	require.NoError(t, a.Kick("C123", "U42", "spam"))
	require.Len(t, fa.kicks, 1)
	assert.Equal(t, "C123", fa.kicks[0].channel)
	assert.Equal(t, "U42", fa.kicks[0].user)

	require.NoError(t, a.Topic("C123", "new topic"))
	require.Len(t, fa.topics, 1)
	assert.Equal(t, "new topic", fa.topics[0].topic)

	require.NoError(t, a.Invite("C123", "U50"))
	require.Len(t, fa.invites, 1)
	assert.Equal(t, []string{"U50"}, fa.invites[0].users)

	assert.ErrorIs(t, a.Ban("C123", "x"), errUnsupported)
	assert.ErrorIs(t, a.SetMode("C123", "+m"), errUnsupported)
	assert.ErrorIs(t, a.Op("C123", "x"), errUnsupported)
	assert.ErrorIs(t, a.Voice("C123", "x"), errUnsupported)
}

func TestActorSayPublishesSent(t *testing.T) {
	c := New(&Settings{}, nil, agent.NewEventBus(nil))
	c.api = &fakeAPI{}
	t.Cleanup(func() { _ = c.Stop(context.Background()) })

	sent := make(chan *agent.Event, 1)
	c.events.Subscribe(agent.EventMessageSent, func(_ context.Context, ev *agent.Event) { sent <- ev })

	require.NoError(t, NewActor(c).Say("C123", "output"))
	select {
	case ev := <-sent:
		assert.Equal(t, "C123", ev.Fields["channel"])
		assert.Equal(t, "output", ev.Fields["text"])
	case <-time.After(time.Second):
		t.Fatal("MESSAGE_SENT not published")
	}
}

func TestRunReturnsOnCtxCancelWithoutSocket(t *testing.T) {
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
	assert.Equal(t, "slack", c.Name())
	assert.True(t, c.ClaimSupervision())
	assert.Equal(t, "turbobot", c.BotName())
}

func TestSettingsValidate(t *testing.T) {
	assert.Error(t, (&Settings{}).Validate())
	assert.Error(t, (&Settings{BotToken: "xoxb-1"}).Validate())
	assert.NoError(t, (&Settings{BotToken: "xoxb-1", AppToken: "xapp-1"}).Validate())
}
