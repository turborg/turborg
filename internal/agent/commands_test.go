package agent_test

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/agent"
)

func TestCommandRegistryParse(t *testing.T) {
	r := agent.NewCommandRegistry("!")

	cases := []struct {
		text     string
		wantName string
		wantArgs []string
		wantOK   bool
	}{
		{"!ping", "ping", []string{}, true},
		{"!ping foo bar", "ping", []string{"foo", "bar"}, true},
		{"!PING", "ping", []string{}, true},
		{"!  spaces", "spaces", []string{}, true},
		{"hello", "", nil, false},
		{"!", "", nil, false},
		{"!   ", "", nil, false},
	}
	for _, tc := range cases {
		t.Run(tc.text, func(t *testing.T) {
			name, args, ok := r.Parse(tc.text)
			assert.Equal(t, tc.wantOK, ok)
			assert.Equal(t, tc.wantName, name)
			assert.Equal(t, tc.wantArgs, args)
		})
	}
}

func TestCommandRegistryCustomPrefix(t *testing.T) {
	r := agent.NewCommandRegistry(".")
	name, _, ok := r.Parse(".ping")
	assert.True(t, ok)
	assert.Equal(t, "ping", name)

	_, _, ok = r.Parse("!ping")
	assert.False(t, ok, "wrong prefix must not match")
}

func TestCommandRegistryDispatch(t *testing.T) {
	r := agent.NewCommandRegistry("!")
	called := false
	r.Register("echo", func(_ context.Context, env *agent.InboundEnvelope, args []string) (*agent.OutboundEnvelope, error) {
		called = true
		assert.Equal(t, []string{"hello"}, args)
		assert.Equal(t, "echo", env.Command)
		return agent.ReplyTo(env, "ok"), nil
	}, nil)

	env := agent.NewInbound("test", "#ch", "alice", "!echo hello")
	out, err := r.Dispatch(context.Background(), env)

	require.NoError(t, err)
	require.NotNil(t, out)
	assert.True(t, called)
	assert.Equal(t, "ok", out.Text)
}

func TestCommandRegistryUnknownCommand(t *testing.T) {
	r := agent.NewCommandRegistry("!")
	env := agent.NewInbound("test", "#ch", "alice", "!nope")
	out, err := r.Dispatch(context.Background(), env)

	assert.NoError(t, err)
	assert.Nil(t, out, "unknown commands return (nil, nil), not an error")
}

func TestCommandRegistryNonCommand(t *testing.T) {
	r := agent.NewCommandRegistry("!")
	r.Register("ping", func(_ context.Context, env *agent.InboundEnvelope, _ []string) (*agent.OutboundEnvelope, error) {
		return agent.ReplyTo(env, "pong"), nil
	}, nil)

	env := agent.NewInbound("test", "#ch", "alice", "just chatting")
	out, err := r.Dispatch(context.Background(), env)

	assert.NoError(t, err)
	assert.Nil(t, out)
}

func TestCommandRegistryPerCommandGuard(t *testing.T) {
	r := agent.NewCommandRegistry("!")
	r.Register("op", func(_ context.Context, env *agent.InboundEnvelope, _ []string) (*agent.OutboundEnvelope, error) {
		return agent.ReplyTo(env, "ok"), nil
	}, func(env *agent.InboundEnvelope) bool {
		return env.Sender == "owner"
	})

	allowed := agent.NewInbound("test", "#ch", "owner", "!op")
	out, err := r.Dispatch(context.Background(), allowed)
	assert.NoError(t, err)
	assert.NotNil(t, out)

	denied := agent.NewInbound("test", "#ch", "alice", "!op")
	out, err = r.Dispatch(context.Background(), denied)
	assert.NoError(t, err)
	assert.Nil(t, out, "guard rejection returns (nil, nil), not an error")
}

func TestCommandRegistryGlobalGuard(t *testing.T) {
	r := agent.NewCommandRegistry("!")
	r.Register("ping", func(_ context.Context, env *agent.InboundEnvelope, _ []string) (*agent.OutboundEnvelope, error) {
		return agent.ReplyTo(env, "pong"), nil
	}, nil)
	r.SetGuard(func(env *agent.InboundEnvelope) bool {
		return env.Sender != "banned"
	})

	ok := agent.NewInbound("test", "#ch", "alice", "!ping")
	out, err := r.Dispatch(context.Background(), ok)
	assert.NoError(t, err)
	assert.NotNil(t, out)

	banned := agent.NewInbound("test", "#ch", "banned", "!ping")
	out, err = r.Dispatch(context.Background(), banned)
	assert.NoError(t, err)
	assert.Nil(t, out)
}

func TestCommandRegistryHandlerError(t *testing.T) {
	r := agent.NewCommandRegistry("!")
	wantErr := errors.New("kaboom")
	r.Register("fail", func(_ context.Context, _ *agent.InboundEnvelope, _ []string) (*agent.OutboundEnvelope, error) {
		return nil, wantErr
	}, nil)

	env := agent.NewInbound("test", "#ch", "alice", "!fail")
	out, err := r.Dispatch(context.Background(), env)
	assert.ErrorIs(t, err, wantErr)
	assert.Nil(t, out)
}

func TestCommandRegistryNames(t *testing.T) {
	r := agent.NewCommandRegistry("!")
	r.Register("a", func(context.Context, *agent.InboundEnvelope, []string) (*agent.OutboundEnvelope, error) { return nil, nil }, nil)
	r.Register("b", func(context.Context, *agent.InboundEnvelope, []string) (*agent.OutboundEnvelope, error) { return nil, nil }, nil)
	assert.ElementsMatch(t, []string{"a", "b"}, r.Names())
}

func TestCommandRegistryEmptyPrefixDefaultsToBang(t *testing.T) {
	r := agent.NewCommandRegistry("")
	assert.Equal(t, "!", r.Prefix())
}
