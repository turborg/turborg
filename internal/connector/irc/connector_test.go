package irc_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/agent"
	"github.com/turborg/turborg/internal/connector/irc"
	"github.com/turborg/turborg/tests/fixtures/fakeirc"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestEchoPing(t *testing.T) {
	fs := fakeirc.New(t)
	defer fs.Close()

	conn := irc.New(irc.Config{
		Host:    "127.0.0.1",
		Port:    fs.Port(),
		TLS:     false,
		Nick:    "turborg",
		User:    "turborg",
		Real:    "turborg PoC",
		Channel: "#test",
	}, nil)

	a := agent.New(nil)
	a.AddConnector(conn)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	require.True(t,
		fs.WaitFor(containsPrefix("JOIN #test"), 2*time.Second),
		"connector did not JOIN #test; received: %v", fs.Received(),
	)

	require.NoError(t, fs.SendLine(":alice!~a@host PRIVMSG #test :!ping"))

	require.True(t,
		fs.WaitFor(containsLine("PRIVMSG #test :pong"), 2*time.Second),
		"connector did not reply with pong; received: %v", fs.Received(),
	)

	shutdownStart := time.Now()
	cancel()

	select {
	case err := <-done:
		elapsed := time.Since(shutdownStart)
		assert.Less(t, elapsed, 500*time.Millisecond, "SIGTERM unwind exceeded 500ms budget (took %v)", elapsed)
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("unexpected agent error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("agent did not shut down within 2s of ctx cancel")
	}

	assert.True(t,
		fs.WaitFor(containsPrefix("QUIT "), 500*time.Millisecond),
		"expected clean QUIT on shutdown; received: %v", fs.Received(),
	)
}

func containsPrefix(prefix string) func([]string) bool {
	return func(lines []string) bool {
		for _, l := range lines {
			if strings.HasPrefix(l, prefix) {
				return true
			}
		}
		return false
	}
}

func containsLine(want string) func([]string) bool {
	return func(lines []string) bool {
		for _, l := range lines {
			if l == want {
				return true
			}
		}
		return false
	}
}
