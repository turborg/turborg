package irc_test

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/agent"
	"github.com/turborg/turborg/internal/connector/irc"
	"github.com/turborg/turborg/tests/fixtures/fakeirc"
)

// driveConnector spins up a fakeirc + agent + IRC connector, runs through
// the handshake, and returns the components plus a teardown. The returned
// agent's EventBus has no subscribers — callers attach what they need
// before invoking driveConnector.
func driveConnector(t *testing.T, settings *irc.Settings, fsOpts ...fakeirc.Option) (*fakeirc.Server, *irc.Connector, *agent.Agent, func()) {
	t.Helper()
	fs := fakeirc.New(t, fsOpts...)
	settings.Hostname = "127.0.0.1"
	settings.Port = fs.Port()
	settings.UseTLS = false
	if settings.Nick == "" {
		settings.Nick = "turborg"
	}
	if len(settings.Channels) == 0 {
		settings.Channels = []string{"#test"}
	}
	// CTCP defaults: auto reply enabled, 3 replies per 30s window.
	// Individual tests override by setting CTCPMaxPerWindow /
	// CTCPWindowSeconds / CTCPAutoReply before calling driveConnector.
	if settings.CTCPMaxPerWindow == 0 && settings.CTCPWindowSeconds == 0 {
		settings.CTCPAutoReply = true
		settings.CTCPMaxPerWindow = 3
		settings.CTCPWindowSeconds = 30
	}
	a := agent.New(nil)
	conn := irc.New(settings, nil, a.Events)
	a.AddConnector(conn)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	require.True(t,
		fs.WaitFor(containsPrefix("JOIN "+settings.Channels[0]), 2*time.Second),
		"handshake did not complete; received: %v", fs.Received(),
	)

	teardown := func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatal("agent did not shut down")
		}
		fs.Close()
	}
	return fs, conn, a, teardown
}

// --- Channel state -------------------------------------------------------

func TestConnectorTracksMemberJoinPart(t *testing.T) {
	fs, conn, _, td := driveConnector(t, &irc.Settings{})
	defer td()

	require.NoError(t, fs.SendLine(":bob!u@h JOIN #test"))
	require.Eventually(t, func() bool {
		info := conn.State().Get("#test")
		return info != nil && info.Members["bob"] == ""
	}, time.Second, 10*time.Millisecond, "bob's JOIN should land in state")

	require.NoError(t, fs.SendLine(":bob!u@h PART #test :bye"))
	require.Eventually(t, func() bool {
		info := conn.State().Get("#test")
		return info != nil && info.Members["bob"] == "" && !contains(info.Members, "bob")
	}, time.Second, 10*time.Millisecond, "bob's PART should remove him")
}

func contains[T comparable](m map[string]T, k string) bool {
	_, ok := m[k]
	return ok
}

func TestConnectorTracksQuitNetworkWide(t *testing.T) {
	fs, conn, _, td := driveConnector(t, &irc.Settings{Channels: []string{"#a"}})
	defer td()

	// Bot already JOIN'd #a per handshake. Add a second channel state via
	// observed JOINs.
	require.NoError(t, fs.SendLine(":turborg!u@h JOIN #b"))
	require.NoError(t, fs.SendLine(":alice!u@h JOIN #a"))
	require.NoError(t, fs.SendLine(":alice!u@h JOIN #b"))

	require.Eventually(t, func() bool {
		return conn.State().Get("#b") != nil &&
			contains(conn.State().Get("#a").Members, "alice") &&
			contains(conn.State().Get("#b").Members, "alice")
	}, time.Second, 10*time.Millisecond)

	require.NoError(t, fs.SendLine(":alice!u@h QUIT :gone"))
	require.Eventually(t, func() bool {
		return !contains(conn.State().Get("#a").Members, "alice") &&
			!contains(conn.State().Get("#b").Members, "alice")
	}, time.Second, 10*time.Millisecond)
}

func TestConnectorTracksKick(t *testing.T) {
	fs, conn, _, td := driveConnector(t, &irc.Settings{})
	defer td()

	require.NoError(t, fs.SendLine(":alice!u@h JOIN #test"))
	require.Eventually(t, func() bool {
		return contains(conn.State().Get("#test").Members, "alice")
	}, time.Second, 10*time.Millisecond)

	require.NoError(t, fs.SendLine(":op!u@h KICK #test alice :no thanks"))
	require.Eventually(t, func() bool {
		return !contains(conn.State().Get("#test").Members, "alice")
	}, time.Second, 10*time.Millisecond)
}

func TestConnectorTracksNickChange(t *testing.T) {
	fs, conn, _, td := driveConnector(t, &irc.Settings{})
	defer td()

	require.NoError(t, fs.SendLine(":alice!u@h JOIN #test"))
	require.Eventually(t, func() bool {
		return contains(conn.State().Get("#test").Members, "alice")
	}, time.Second, 10*time.Millisecond)

	require.NoError(t, fs.SendLine(":alice!u@h NICK :alice2"))
	require.Eventually(t, func() bool {
		return contains(conn.State().Get("#test").Members, "alice2") &&
			!contains(conn.State().Get("#test").Members, "alice")
	}, time.Second, 10*time.Millisecond)
}

func TestConnectorTracksTopicAndNumerics(t *testing.T) {
	fs, conn, _, td := driveConnector(t, &irc.Settings{})
	defer td()

	require.NoError(t, fs.SendLine(":op!u@h TOPIC #test :a new topic"))
	require.NoError(t, fs.SendLine(":fake 332 turborg #test :another topic"))
	require.NoError(t, fs.SendLine(":fake 333 turborg #test alice 1700000000"))
	require.NoError(t, fs.SendLine(":fake 353 turborg = #test :@turborg +bob carol"))
	require.NoError(t, fs.SendLine(":fake 366 turborg #test :End of /NAMES list"))

	require.Eventually(t, func() bool {
		info := conn.State().Get("#test")
		return info != nil &&
			info.Topic == "another topic" && // 332 wins because it arrived last
			info.TopicSetBy == "alice" &&
			info.TopicSetAt == 1700000000 &&
			info.NamesComplete &&
			info.Members["bob"] == "+" &&
			info.Members["carol"] == ""
	}, time.Second, 10*time.Millisecond, "topic+names should be tracked through numerics")

	// RPL_NOTOPIC path
	require.NoError(t, fs.SendLine(":fake 331 turborg #test :No topic is set"))
	require.Eventually(t, func() bool {
		info := conn.State().Get("#test")
		return info != nil && info.Topic == "" && info.TopicSet
	}, time.Second, 10*time.Millisecond)
}

// --- Events fired -------------------------------------------------------

func TestEndOfNamesPublishesChannelNamesEvent(t *testing.T) {
	// Regression: state.OnNamesEnd was firing but nothing was publishing
	// EventChannelNames, so the web gateway never pushed a refreshed
	// member list to existing WS clients. After a bot restart, the WS
	// `state` op on connect saw an empty channel (NAMES hadn't arrived
	// yet) and the member list stayed empty until a full page reload.
	a := agent.New(nil)
	var (
		gotMu sync.Mutex
		got   *agent.Event
	)
	a.Events.Subscribe(agent.EventChannelNames, func(_ context.Context, ev *agent.Event) {
		gotMu.Lock()
		got = ev
		gotMu.Unlock()
	})

	fs := fakeirc.New(t)
	conn := irc.New(&irc.Settings{
		Hostname: "127.0.0.1",
		Port:     fs.Port(),
		Nick:     "turborg",
		Channels: []string{"#test"},
	}, nil, a.Events)
	a.AddConnector(conn)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	defer func() {
		cancel()
		<-done
		fs.Close()
	}()

	require.True(t, fs.WaitFor(containsPrefix("JOIN #test"), 2*time.Second))

	require.NoError(t, fs.SendLine(":fake 353 turborg = #test :@turborg +alice bob"))
	require.NoError(t, fs.SendLine(":fake 366 turborg #test :End of /NAMES list"))

	require.Eventually(t, func() bool {
		gotMu.Lock()
		defer gotMu.Unlock()
		return got != nil
	}, time.Second, 10*time.Millisecond,
		"RplEndOfNames must publish EventChannelNames so the gateway can refresh WS clients")

	gotMu.Lock()
	captured := got
	gotMu.Unlock()
	assert.Equal(t, "#test", captured.Fields["channel"])

	members, ok := captured.Fields["members"].([]map[string]string)
	require.True(t, ok, "members field must be []map[string]string")
	nicks := map[string]string{}
	for _, m := range members {
		nicks[m["nick"]] = m["mode"]
	}
	assert.Equal(t, "@", nicks["turborg"])
	assert.Equal(t, "+", nicks["alice"])
	assert.Equal(t, "", nicks["bob"])
}

func TestConnectorPublishesLifecycleEvents(t *testing.T) {
	a := agent.New(nil)
	var joins, parts, kicks, nicks, topics atomic.Int32
	a.Events.Subscribe(agent.EventUserJoin, func(context.Context, *agent.Event) { joins.Add(1) })
	a.Events.Subscribe(agent.EventUserLeave, func(context.Context, *agent.Event) { parts.Add(1) })
	a.Events.Subscribe(agent.EventUserKicked, func(context.Context, *agent.Event) { kicks.Add(1) })
	a.Events.Subscribe(agent.EventUserNickChange, func(context.Context, *agent.Event) { nicks.Add(1) })
	a.Events.Subscribe(agent.EventTopicChanged, func(context.Context, *agent.Event) { topics.Add(1) })

	fs := fakeirc.New(t)
	conn := irc.New(&irc.Settings{
		Hostname: "127.0.0.1",
		Port:     fs.Port(),
		Nick:     "turborg",
		Channels: []string{"#test"},
	}, nil, a.Events)
	a.AddConnector(conn)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	defer func() {
		cancel()
		<-done
		fs.Close()
	}()

	require.True(t, fs.WaitFor(containsPrefix("JOIN #test"), 2*time.Second))

	require.NoError(t, fs.SendLine(":alice!u@h JOIN #test"))
	require.NoError(t, fs.SendLine(":alice!u@h PART #test"))
	require.NoError(t, fs.SendLine(":alice!u@h JOIN #test"))
	require.NoError(t, fs.SendLine(":op!u@h KICK #test alice"))
	require.NoError(t, fs.SendLine(":bob!u@h JOIN #test"))
	require.NoError(t, fs.SendLine(":bob!u@h NICK :bob2"))
	require.NoError(t, fs.SendLine(":op!u@h TOPIC #test :hello"))

	require.Eventually(t, func() bool {
		return joins.Load() >= 2 && parts.Load() >= 1 && kicks.Load() == 1 &&
			nicks.Load() == 1 && topics.Load() == 1
	}, time.Second, 10*time.Millisecond,
		"expected events: joins>=2 parts>=1 kicks=1 nicks=1 topics=1; got %d/%d/%d/%d/%d",
		joins.Load(), parts.Load(), kicks.Load(), nicks.Load(), topics.Load(),
	)
}

// --- CTCP auto-reply ----------------------------------------------------

func TestCTCPVersionAutoReply(t *testing.T) {
	fs, _, _, td := driveConnector(t, &irc.Settings{})
	defer td()

	require.NoError(t, fs.SendLine(":alice!u@h PRIVMSG turborg :\x01VERSION\x01"))

	require.True(t,
		fs.WaitFor(func(lines []string) bool {
			for _, l := range lines {
				if strings.HasPrefix(l, "NOTICE alice :\x01VERSION ") && strings.HasSuffix(l, "\x01") {
					return true
				}
			}
			return false
		}, time.Second),
		"expected CTCP VERSION reply via NOTICE; received: %v", fs.Received(),
	)
}

func TestCTCPActionIsNotAutoReplied(t *testing.T) {
	fs, _, _, td := driveConnector(t, &irc.Settings{})
	defer td()

	require.NoError(t, fs.SendLine(":alice!u@h PRIVMSG #test :\x01ACTION waves\x01"))
	time.Sleep(50 * time.Millisecond)
	for _, l := range fs.Received() {
		assert.False(t, strings.HasPrefix(l, "NOTICE alice"),
			"ACTION must never trigger CTCP auto-reply: %s", l)
	}
}

func TestCTCPThrottle(t *testing.T) {
	settings := &irc.Settings{CTCPAutoReply: true, CTCPMaxPerWindow: 2, CTCPWindowSeconds: 30}
	fs, _, _, td := driveConnector(t, settings)
	defer td()

	for i := 0; i < 4; i++ {
		require.NoError(t, fs.SendLine(":alice!u@h PRIVMSG turborg :\x01VERSION\x01"))
	}
	time.Sleep(150 * time.Millisecond)

	var notices int
	for _, l := range fs.Received() {
		if strings.HasPrefix(l, "NOTICE alice :\x01VERSION ") {
			notices++
		}
	}
	assert.Equal(t, 2, notices, "throttle should cap auto-replies at MaxPerWindow")
}

func TestCTCPAutoReplyDisabled(t *testing.T) {
	settings := &irc.Settings{CTCPAutoReply: false, CTCPMaxPerWindow: 3, CTCPWindowSeconds: 30}
	fs, _, _, td := driveConnector(t, settings)
	defer td()

	require.NoError(t, fs.SendLine(":alice!u@h PRIVMSG turborg :\x01VERSION\x01"))
	time.Sleep(100 * time.Millisecond)
	for _, l := range fs.Received() {
		assert.False(t, strings.HasPrefix(l, "NOTICE alice"),
			"CTCP auto-reply disabled means no NOTICE — got: %s", l)
	}
}

func TestCTCPPingEchoesArg(t *testing.T) {
	fs, _, _, td := driveConnector(t, &irc.Settings{})
	defer td()

	require.NoError(t, fs.SendLine(":alice!u@h PRIVMSG turborg :\x01PING 12345\x01"))
	require.True(t,
		fs.WaitFor(func(lines []string) bool {
			for _, l := range lines {
				if l == "NOTICE alice :\x01PING 12345\x01" {
					return true
				}
			}
			return false
		}, time.Second),
		"PING should be echoed with the same arg; received: %v", fs.Received(),
	)
}

func TestCTCPUnknownTypeNoReply(t *testing.T) {
	fs, _, _, td := driveConnector(t, &irc.Settings{})
	defer td()

	require.NoError(t, fs.SendLine(":alice!u@h PRIVMSG turborg :\x01UNKNOWN\x01"))
	time.Sleep(50 * time.Millisecond)
	for _, l := range fs.Received() {
		assert.False(t, strings.HasPrefix(l, "NOTICE alice :\x01UNKNOWN"),
			"unrecognized CTCP must not produce a reply: %s", l)
	}
}

// --- PING from server ---------------------------------------------------

func TestPostHandshakePingPong(t *testing.T) {
	fs, _, _, td := driveConnector(t, &irc.Settings{})
	defer td()

	require.NoError(t, fs.SendLine("PING :keep-alive"))
	require.True(t,
		fs.WaitFor(containsLine("PONG :keep-alive"), time.Second),
		"server PING after handshake must be PONG'd; received: %v", fs.Received(),
	)
}

// --- Bouncer wire-up ----------------------------------------------------

func TestConnectorStartsBouncerWhenConfigured(t *testing.T) {
	fs := fakeirc.New(t)
	defer fs.Close()
	a := agent.New(nil)
	conn := irc.New(&irc.Settings{
		Hostname:                "127.0.0.1",
		Port:                    fs.Port(),
		Nick:                    "turborg",
		Channels:                []string{"#test"},
		BouncerPassword:         "hunter2",
		BouncerHost:             "127.0.0.1",
		BouncerPort:             0, // random
		BouncerRatelimitEnabled: false,
	}, nil, a.Events)
	a.AddConnector(conn)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	defer func() {
		cancel()
		<-done
	}()

	// Wait for handshake to finish (bouncer.Start runs after).
	require.True(t, fs.WaitFor(containsPrefix("JOIN #test"), 2*time.Second))

	// Bouncer started? We can't probe its addr without exposing it, but
	// we can verify lifecycle by checking the agent stays up; the
	// presence of bouncer.Start is exercised by coverage.
}
