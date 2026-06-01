package irc_test

import (
	"bufio"
	"context"
	"errors"
	"net"
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
	// Register a simple everyone-access ping command (the agent ships with
	// none) so the end-to-end echo path has something to dispatch.
	a.Commands.Register("ping", func(_ context.Context, env *agent.InboundEnvelope, _ []string) (*agent.OutboundEnvelope, error) {
		return agent.ReplyTo(env, "pong"), nil
	}, nil)
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

// --- Server password + NickServ identify + CTCP throttle paths ---------

func TestConnectorSendsServerPasswordAndNickServIdentify(t *testing.T) {
	fs := fakeirc.New(t)
	defer fs.Close()

	conn := irc.New(&irc.Settings{
		Hostname:          "127.0.0.1",
		Port:              fs.Port(),
		Nick:              "turborg",
		Channels:          []string{"#test"},
		ServerPassword:    "sekrit",
		NickServPassword:  "nspass",
		CTCPAutoReply:     true,
		CTCPMaxPerWindow:  3,
		CTCPWindowSeconds: 30,
	}, nil, nil)

	a := agent.New(nil)
	a.AddConnector(conn)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	require.True(t,
		fs.WaitFor(containsLine("PASS sekrit"), 2*time.Second),
		"server PASS must be sent during register; received: %v", fs.Received(),
	)
	require.True(t,
		fs.WaitFor(containsPrefix("PRIVMSG NickServ :IDENTIFY"), 2*time.Second),
		"NickServ IDENTIFY must follow JOIN; received: %v", fs.Received(),
	)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("agent did not shut down")
	}
}

func TestConnectorCTCPAutoReplyVersion(t *testing.T) {
	fs := fakeirc.New(t)
	defer fs.Close()

	conn := irc.New(&irc.Settings{
		Hostname:          "127.0.0.1",
		Port:              fs.Port(),
		Nick:              "turborg",
		Channels:          []string{"#test"},
		CTCPAutoReply:     true,
		CTCPMaxPerWindow:  3,
		CTCPWindowSeconds: 30,
	}, nil, nil)

	a := agent.New(nil)
	a.AddConnector(conn)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	require.True(t,
		fs.WaitFor(containsPrefix("JOIN #test"), 2*time.Second),
		"connector did not JOIN; received: %v", fs.Received(),
	)
	// Simulate an inbound CTCP VERSION from alice; expect a NOTICE reply.
	require.NoError(t, fs.SendLine(":alice!~a@host PRIVMSG turborg :\x01VERSION\x01"))
	require.True(t,
		fs.WaitFor(func(lines []string) bool {
			for _, l := range lines {
				if strings.HasPrefix(l, "NOTICE alice :\x01VERSION turborg") {
					return true
				}
			}
			return false
		}, 2*time.Second),
		"missing CTCP VERSION reply NOTICE; received: %v", fs.Received(),
	)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("agent did not shut down")
	}
}

func TestConnectorClientPingFiresPeriodically(t *testing.T) {
	// ClientPingInterval=50ms — within a 2s test window we should see
	// at least one client-initiated PING from the connector reach the
	// upstream socket.
	fs := fakeirc.New(t)
	defer fs.Close()

	conn := irc.New(&irc.Settings{
		Hostname:           "127.0.0.1",
		Port:               fs.Port(),
		Nick:               "turborg",
		Channels:           []string{"#test"},
		ClientPingInterval: 50 * time.Millisecond,
		ReadIdleTimeout:    5 * time.Second,
	}, nil, nil)

	a := agent.New(nil)
	a.AddConnector(conn)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	require.True(t,
		fs.WaitFor(containsPrefix("PING "), 2*time.Second),
		"client-initiated PING must reach the server; received: %v", fs.Received(),
	)
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("agent did not shut down")
	}
}

func TestConnectorReadIdleTimeoutSurfacesAsTransientState(t *testing.T) {
	// ReadIdleTimeout=200ms, no ClientPingInterval — the read loop must
	// classify the silent-death as transient and the reconnect
	// supervisor must publish that transition. Pre-supervisor this test
	// asserted Run() exited with the read-idle error; the supervisor
	// now treats read-idle as recoverable and keeps retrying, so the
	// observable signal moved from Run's return value to the state
	// machine.
	fs := fakeirc.New(t)
	defer fs.Close()

	conn := irc.New(&irc.Settings{
		Hostname:           "127.0.0.1",
		Port:               fs.Port(),
		Nick:               "turborg",
		Channels:           []string{"#test"},
		ReadIdleTimeout:    200 * time.Millisecond,
		ClientPingInterval: 0,
	}, nil, nil)

	sawTransient := make(chan struct{}, 1)
	sub := conn.UpstreamState().Subscribe(func(c irc.UpstreamStateChange) {
		if c.To == irc.UpstreamStateDisconnectedTransient {
			select {
			case sawTransient <- struct{}{}:
			default:
			}
		}
	})
	defer sub.Unsubscribe()

	a := agent.New(nil)
	a.AddConnector(conn)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	select {
	case <-sawTransient:
	case <-time.After(3 * time.Second):
		cancel()
		<-done
		t.Fatal("read-idle timeout did not transition state to disconnected_transient")
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("agent did not shut down within 2s of ctx cancel")
	}
}

func TestConnectorIgnoresUnknownNickInUse433DuringHandshake(t *testing.T) {
	// Manually drive a fake server that responds to USER with a 433
	// (NickNameInUse) and then keeps the connection open. The connector
	// sends USER → NICK → CAP END one after the other; if the fake
	// closes its side immediately after writing 433 the connector's
	// next write loses the race and surfaces a "connection reset by
	// peer" error instead of the parsed 433. Keep reading until the
	// connector closes its side so awaitHandshake reliably reads the
	// 433 and returns the expected error.
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer l.Close()

	go func() {
		conn, err := l.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		reader := bufio.NewReader(conn)
		sentReject := false
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				return
			}
			if !sentReject && strings.HasPrefix(line, "USER ") {
				_, _ = conn.Write([]byte(":fake 433 * turborg :Nickname is already in use\r\n"))
				sentReject = true
			}
			// Keep draining client writes (NICK, CAP END, QUIT) until
			// the connector closes — the read loop above will exit on
			// EOF naturally.
		}
	}()

	c := irc.New(&irc.Settings{
		Hostname:         "127.0.0.1",
		Port:             l.Addr().(*net.TCPAddr).Port,
		Nick:             "turborg",
		HandshakeTimeout: 2 * time.Second,
	}, nil, nil)

	err = c.Start(context.Background())
	require.NoError(t, err,
		"a 433 nick-in-use is recoverable — Start degrades gracefully so the supervisor can retry")
	assert.True(t, c.UpstreamState().State().IsRecoverable(),
		"433 during registration must leave the connector in a recoverable state, not crash it")
}

func TestConnectorClientLimitsAccessors(t *testing.T) {
	c := irc.New(&irc.Settings{Hostname: "h", Nick: "n"}, nil, nil)

	// Default: zero value (unrestricted).
	assert.Equal(t, irc.ClientLimits{}, c.ClientLimits())

	limits := irc.ClientLimits{NickLocked: true, MaxChannels: 5}
	c.SetClientLimits(limits)
	assert.Equal(t, limits, c.ClientLimits())
}

func TestConnectorOutboundThrottleAccessors(t *testing.T) {
	c := irc.New(&irc.Settings{Hostname: "h", Nick: "n"}, nil, nil)

	// Default: nil.
	assert.Nil(t, c.OutboundThrottle())

	tr, err := irc.NewThrottle(5, time.Second, nil)
	require.NoError(t, err)
	c.SetOutboundThrottle(tr)
	assert.Same(t, tr, c.OutboundThrottle(),
		"OutboundThrottle should return the exact pointer we set")
}
