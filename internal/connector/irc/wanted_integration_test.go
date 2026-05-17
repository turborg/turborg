package irc_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/agent"
	"github.com/turborg/turborg/internal/connector/irc"
)

// TestWantedChannelsSeededFromSettings confirms the connector's
// wanted-set starts populated from the operator-configured channel
// list — the supervisor's first JOIN replay must cover the same
// channels Settings.Channels names.
func TestWantedChannelsSeededFromSettings(t *testing.T) {
	c := irc.New(&irc.Settings{
		Hostname: "127.0.0.1",
		Nick:     "turborg",
		Channels: []string{"#a", "#b"},
	}, nil, nil)
	snap := c.WantedChannels().Snapshot()
	require.Len(t, snap, 2)
	assert.Equal(t, "#a", snap[0].Name)
	assert.Equal(t, "#b", snap[1].Name)
}

// TestBouncerRecordsKeyOnClientJoin runs a stand-alone bouncer (no
// upstream) and confirms its forward path captures channel keys from
// client-originated JOIN frames into the attached wanted-set. The
// supervisor's reconnect replay (covered separately below) is what
// then makes use of that key on the next session.
func TestBouncerRecordsKeyOnClientJoin(t *testing.T) {
	wanted := irc.NewWantedChannels(nil)
	b, addr := freshBouncer(t, "hunter2", func(b *irc.Bouncer) {
		b.AttachWantedChannels(wanted)
		b.AttachState(irc.NewChannelState(), "turborg", "ident", "host")
	})
	trackForwarded(b)

	conn, _ := authBouncerClient(t, addr)
	writeLine(t, conn, "JOIN #private hunter2")

	require.Eventually(t, func() bool {
		entry, ok := wanted.Get("#private")
		return ok && entry.Key == "hunter2"
	}, time.Second, 10*time.Millisecond,
		"client-originated keyed JOIN must populate the wanted-set")
}

// TestBouncerRecordsMultiChannelJoinIntoWanted covers the comma-list
// JOIN syntax — a single JOIN frame can carry multiple channels +
// matching keys. The wanted-set must pick up each pairing.
func TestBouncerRecordsMultiChannelJoinIntoWanted(t *testing.T) {
	wanted := irc.NewWantedChannels(nil)
	b, addr := freshBouncer(t, "hunter2", func(b *irc.Bouncer) {
		b.AttachWantedChannels(wanted)
		b.AttachState(irc.NewChannelState(), "turborg", "ident", "host")
	})
	trackForwarded(b)

	conn, _ := authBouncerClient(t, addr)
	writeLine(t, conn, "JOIN #a,#b,#c k1,,k3")

	require.Eventually(t, func() bool {
		a, aok := wanted.Get("#a")
		b, bok := wanted.Get("#b")
		c, cok := wanted.Get("#c")
		return aok && bok && cok &&
			a.Key == "k1" && b.Key == "" && c.Key == "k3"
	}, time.Second, 10*time.Millisecond,
		"comma-list JOIN must pair channels with their keys positionally")
}

// TestBouncerRecordsPartIntoWanted confirms a client-originated PART
// removes the channel from the wanted-set so the supervisor doesn't
// silently rejoin it on the next reconnect.
func TestBouncerRecordsPartIntoWanted(t *testing.T) {
	wanted := irc.NewWantedChannels([]string{"#a", "#b"})
	b, addr := freshBouncer(t, "hunter2", func(b *irc.Bouncer) {
		b.AttachWantedChannels(wanted)
		b.AttachState(irc.NewChannelState(), "turborg", "ident", "host")
	})
	trackForwarded(b)

	conn, _ := authBouncerClient(t, addr)
	writeLine(t, conn, "PART #b")

	require.Eventually(t, func() bool {
		_, ok := wanted.Get("#b")
		return !ok
	}, time.Second, 10*time.Millisecond,
		"client PART must remove the channel from the wanted-set")

	_, stillThere := wanted.Get("#a")
	assert.True(t, stillThere, "#a must survive")
}

// TestSupervisorReplaysKeyedJoinOnReconnect is the load-bearing
// scenario for channel-key memory: the user joined a +k channel
// during the first session, upstream drops, the supervisor reconnects,
// and the JOIN replay carries the stored key. Asserts against the
// fake server's recorded wire traffic.
func TestSupervisorReplaysKeyedJoinOnReconnect(t *testing.T) {
	fs := newReconnectTestServer(t)

	conn := irc.New(&irc.Settings{
		Hostname: "127.0.0.1",
		Port:     fs.Port(),
		Nick:     "turborg",
		Channels: []string{"#a"},
	}, nil, nil)
	// Pre-seed the wanted-set with a keyed channel — same outcome as
	// if a client had JOINed it during the first session, but doesn't
	// need a bouncer in the loop to drive.
	conn.WantedChannels().Add("#private", "hunter2")

	a := agent.New(nil)
	a.AddConnector(conn)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()

	require.Eventually(t, func() bool {
		return conn.UpstreamState().State() == irc.UpstreamStateRegistered &&
			fs.Sessions() == 1
	}, 2*time.Second, 10*time.Millisecond)

	// First-session JOINs include both #a (seed) and #private (keyed).
	require.Eventually(t, func() bool {
		for _, l := range fs.Received() {
			if l == "JOIN #private hunter2" {
				return true
			}
		}
		return false
	}, time.Second, 10*time.Millisecond,
		"initial JOIN replay must carry the stored key")

	// Bounce upstream + wait for the supervisor to reconnect.
	fs.Kill()
	require.Eventually(t, func() bool {
		return conn.UpstreamState().State() == irc.UpstreamStateRegistered &&
			fs.Sessions() == 2
	}, 5*time.Second, 50*time.Millisecond)

	// Second-session JOINs must also include the keyed line.
	var keyedCount int
	for _, l := range fs.Received() {
		if l == "JOIN #private hunter2" {
			keyedCount++
		}
	}
	assert.GreaterOrEqual(t, keyedCount, 2,
		"keyed JOIN must replay on every reconnect cycle, not just the initial one")

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("agent did not shut down")
	}
}

// TestUpstreamSelfJoinEchoDoesNotClobberKey covers the "Add semantics"
// invariant from the unit tests at the connector level: when upstream
// echoes a JOIN for a +k channel (server omits the key from echo), the
// wanted-set entry must retain the key the client originally supplied.
func TestUpstreamSelfJoinEchoDoesNotClobberKey(t *testing.T) {
	conn := irc.New(&irc.Settings{
		Hostname: "127.0.0.1",
		Nick:     "turborg",
	}, nil, nil)
	conn.WantedChannels().Add("#private", "hunter2")
	// Simulate the dispatchLine path observing an upstream self-JOIN —
	// it Add()s with empty key. Direct call here because the dispatch
	// path is exercised by other tests; this one is targeted at the
	// invariant.
	conn.WantedChannels().Add("#private", "")
	entry, ok := conn.WantedChannels().Get("#private")
	require.True(t, ok)
	assert.Equal(t, "hunter2", entry.Key,
		"upstream JOIN echo must not clobber a stored channel key")
}
