package irc

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// recordingSender captures every raw line the Actor emits.
type recordingSender struct {
	lines []string
	err   error
}

func (r *recordingSender) SendRaw(line string) error {
	r.lines = append(r.lines, line)
	return r.err
}

func newTestActor() (*Actor, *recordingSender) {
	rs := &recordingSender{}
	return &Actor{send: rs}, rs
}

func TestActorTranslatesToIRCWire(t *testing.T) {
	a, rs := newTestActor()

	require.NoError(t, a.Say("#room", "hello"))
	require.NoError(t, a.Notice("alice", "psst"))
	require.NoError(t, a.Kick("#room", "bob", "spam"))
	require.NoError(t, a.Kick("#room", "carol", "")) // no reason → no trailing
	require.NoError(t, a.Op("#room", "dave"))
	require.NoError(t, a.Voice("#room", "erin"))
	require.NoError(t, a.SetMode("#room", "+m"))
	require.NoError(t, a.Topic("#room", "new topic"))
	require.NoError(t, a.Invite("#room", "frank"))

	assert.Equal(t, []string{
		"PRIVMSG #room :hello",
		"NOTICE alice :psst",
		"KICK #room bob :spam",
		"KICK #room carol",
		"MODE #room +o dave",
		"MODE #room +v erin",
		"MODE #room +m",
		"TOPIC #room :new topic",
		"INVITE frank #room",
	}, rs.lines)
}

func TestActorBanResolvesMask(t *testing.T) {
	a, rs := newTestActor()
	require.NoError(t, a.Ban("#room", "troll"))             // bare nick → widened
	require.NoError(t, a.Ban("#room", "troll!*@evil.host")) // full mask → as-is
	assert.Equal(t, []string{
		"MODE #room +b troll!*@*",
		"MODE #room +b troll!*@evil.host",
	}, rs.lines)
}

func TestActorSetModeWithArgs(t *testing.T) {
	a, rs := newTestActor()
	require.NoError(t, a.SetMode("#room", "+qo", "spammer", "helper"))
	assert.Equal(t, "MODE #room +qo spammer helper", rs.lines[0])
}

func TestActorPropagatesSendError(t *testing.T) {
	rs := &recordingSender{err: errors.New("not connected")}
	a := &Actor{send: rs}
	require.Error(t, a.Say("#room", "hi"))
}

func TestResolveBanMask(t *testing.T) {
	assert.Equal(t, "nick!*@*", resolveBanMask("nick"))
	assert.Equal(t, "nick!user@host", resolveBanMask("nick!user@host"))
	assert.Equal(t, "*@host", resolveBanMask("*@host"))
}

func TestNewActorBindsConnector(t *testing.T) {
	cfg := &Settings{Hostname: "irc.example", Nick: "bot"}
	cfg.ApplyDefaults()
	conn := New(cfg, nil, nil)
	a := NewActor(conn)
	// Not connected → SendRaw surfaces the connector's "not connected" error.
	require.Error(t, a.Say("#room", "hi"))
}

func TestActorSaySplitsMultilineText(t *testing.T) {
	a, rs := newTestActor()
	require.NoError(t, a.Say("#room", "line one\nline two\nline three"))
	assert.Equal(t, []string{
		"PRIVMSG #room :line one",
		"PRIVMSG #room :line two",
		"PRIVMSG #room :line three",
	}, rs.lines)
}

func TestActorSaySkipsBlankLinesAndNormalizesCRLF(t *testing.T) {
	a, rs := newTestActor()
	require.NoError(t, a.Say("#room", "Top 10 Crypto Prices:\r\n\r\nBTC $65000\nETH $3400"))
	assert.Equal(t, []string{
		"PRIVMSG #room :Top 10 Crypto Prices:",
		"PRIVMSG #room :BTC $65000",
		"PRIVMSG #room :ETH $3400",
	}, rs.lines)
}

func TestActorSayNeutralizesCRLFInjection(t *testing.T) {
	a, rs := newTestActor()
	// A newline in user/LLM text must not smuggle a second raw IRC command.
	require.NoError(t, a.Say("#room", "hi\nQUIT :bye"))
	assert.Equal(t, []string{
		"PRIVMSG #room :hi",
		"PRIVMSG #room :QUIT :bye", // sent as message text, never a bare QUIT
	}, rs.lines)
}

func TestActorNoticeSplitsMultilineText(t *testing.T) {
	a, rs := newTestActor()
	require.NoError(t, a.Notice("alice", "a\nb"))
	assert.Equal(t, []string{"NOTICE alice :a", "NOTICE alice :b"}, rs.lines)
}
