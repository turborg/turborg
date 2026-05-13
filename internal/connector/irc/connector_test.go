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

	conn := irc.New(&irc.Settings{
		Hostname: "127.0.0.1",
		Port:     fs.Port(),
		UseTLS:   false,
		Nick:     "turborg",
		Username: "turborg",
		RealName: "turborg PoC",
		Channels: []string{"#test"},
	}, nil, nil)

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

func TestSASLSuccess(t *testing.T) {
	fs := fakeirc.New(t, fakeirc.WithSASL(fakeirc.SASLSuccess))
	defer fs.Close()

	conn := irc.New(&irc.Settings{
		Hostname:     "127.0.0.1",
		Port:         fs.Port(),
		Nick:         "turborg",
		Username:     "turborg",
		RealName:     "turborg",
		Channels:     []string{"#test"},
		SASLUser:     "alice",
		SASLPassword: "secret",
	}, nil, nil)

	a := agent.New(nil)
	a.AddConnector(conn)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	require.True(t,
		fs.WaitFor(containsPrefix("JOIN #test"), 2*time.Second),
		"connector did not JOIN after SASL success; received: %v", fs.Received(),
	)

	// Verify the connector sent AUTHENTICATE PLAIN then base64 creds.
	got := fs.Received()
	var sawAuthPlain, sawAuthCreds bool
	for _, l := range got {
		if l == "AUTHENTICATE PLAIN" {
			sawAuthPlain = true
		} else if strings.HasPrefix(l, "AUTHENTICATE ") && l != "AUTHENTICATE PLAIN" && l != "AUTHENTICATE +" {
			sawAuthCreds = true
		}
	}
	assert.True(t, sawAuthPlain, "missing AUTHENTICATE PLAIN; received: %v", got)
	assert.True(t, sawAuthCreds, "missing AUTHENTICATE <base64>; received: %v", got)

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("agent did not shut down")
	}
}

func TestSASLFailureSurfaces(t *testing.T) {
	fs := fakeirc.New(t, fakeirc.WithSASL(fakeirc.SASLFail))
	defer fs.Close()

	conn := irc.New(&irc.Settings{
		Hostname:     "127.0.0.1",
		Port:         fs.Port(),
		Nick:         "turborg",
		Channels:     []string{"#test"},
		SASLUser:     "alice",
		SASLPassword: "wrong",
	}, nil, nil)

	a := agent.New(nil)
	a.AddConnector(conn)

	err := a.Run(context.Background())
	require.Error(t, err, "SASL 904 must surface as a Run() error")
	assert.Contains(t, err.Error(), "SASL failed")
}

func TestSASLUnsupportedFallsBack(t *testing.T) {
	// SASLDisabled means the server NAKs :sasl in CAP REQ.
	fs := fakeirc.New(t, fakeirc.WithSASL(fakeirc.SASLDisabled))
	defer fs.Close()

	conn := irc.New(&irc.Settings{
		Hostname:     "127.0.0.1",
		Port:         fs.Port(),
		Nick:         "turborg",
		Channels:     []string{"#test"},
		SASLUser:     "alice",
		SASLPassword: "secret",
	}, nil, nil)

	a := agent.New(nil)
	a.AddConnector(conn)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	require.True(t,
		fs.WaitFor(containsPrefix("JOIN #test"), 2*time.Second),
		"connector did not JOIN after SASL fallback; received: %v", fs.Received(),
	)

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("agent did not shut down")
	}
}
