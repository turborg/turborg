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
