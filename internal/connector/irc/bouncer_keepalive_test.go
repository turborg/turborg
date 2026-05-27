package irc_test

import (
	"io"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/connector/irc"
)

// TestBouncerPingsAttachedClient: the bouncer must proactively PING an
// attached client so the client↔bouncer leg keeps bytes flowing and a
// NAT/proxy idle timeout (e.g. the HAProxy SNI router) never reaps a
// live-but-quiet attachment. Pre-auth is fine — real IRCds ping during
// registration too.
func TestBouncerPingsAttachedClient(t *testing.T) {
	_, addr := freshBouncer(t, "hunter2", func(b *irc.Bouncer) {
		// Fast ping, generous grace so the client isn't reaped mid-test.
		b.AttachClientKeepalive(40*time.Millisecond, time.Second)
	})
	conn, r := bouncerClient(t, addr)

	conn.SetReadDeadline(time.Now().Add(time.Second))
	sawPing := false
	for i := 0; i < 6; i++ {
		line, err := r.ReadString('\n')
		require.NoError(t, err)
		if strings.HasPrefix(line, "PING :") {
			sawPing = true
			break
		}
	}
	assert.True(t, sawPing, "bouncer must send a server PING to keep the client leg warm")
}

// TestBouncerReapsClientThatNeverPongs: a client that answers nothing —
// not even our keepalive PING — is dead. The read loop's deadline
// (pingInterval+pongGrace) must close it instead of leaving a phantom
// attachment occupying the conn_cur flood-guard slot.
func TestBouncerReapsClientThatNeverPongs(t *testing.T) {
	_, addr := freshBouncer(t, "hunter2", func(b *irc.Bouncer) {
		b.AttachClientKeepalive(40*time.Millisecond, 80*time.Millisecond)
	})
	conn, r := bouncerClient(t, addr)

	// Never write back. Read with a deadline well past the expected reap so
	// an EOF here means the *bouncer* closed us, not our own deadline.
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	start := time.Now()
	var err error
	for err == nil {
		_, err = r.ReadString('\n')
	}
	elapsed := time.Since(start)

	require.Error(t, err, "bouncer must close a silent client")
	assert.ErrorIs(t, err, io.EOF, "expected a clean peer close, not our read timeout")
	assert.Less(t, elapsed, 600*time.Millisecond,
		"dead client should be reaped within ~pingInterval+pongGrace")
}

// TestBouncerKeepsLiveClientAttached: a client that answers PINGs with
// PONGs (and nothing else) must stay attached well past pingInterval —
// the PONG resets the read deadline each cycle. Guards against the reaper
// firing on a perfectly healthy idle client.
func TestBouncerKeepsLiveClientAttached(t *testing.T) {
	_, addr := freshBouncer(t, "hunter2", func(b *irc.Bouncer) {
		b.AttachClientKeepalive(30*time.Millisecond, 60*time.Millisecond)
	})
	conn, r := bouncerClient(t, addr)

	// Pong every PING for ~5 intervals; the connection must survive throughout.
	deadline := time.Now().Add(250 * time.Millisecond)
	for time.Now().Before(deadline) {
		conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
		line, err := r.ReadString('\n')
		require.NoError(t, err, "live client must not be reaped while it pongs")
		if strings.HasPrefix(line, "PING :") {
			token := strings.TrimPrefix(strings.TrimSpace(line), "PING :")
			writeLine(t, conn, "PONG :"+token)
		}
	}
}

// TestBouncerConsumesClientPongWithoutForwarding: the PONG a client sends
// in reply to our keepalive PING must be consumed locally, never tunnelled
// upstream.
func TestBouncerConsumesClientPongWithoutForwarding(t *testing.T) {
	b, addr := freshBouncer(t, "hunter2", func(b *irc.Bouncer) {
		// Disable keepalive so the read loop blocks and this test controls
		// exactly what the client sends.
		b.AttachClientKeepalive(0, 0)
		b.AttachState(irc.NewChannelState(), "turborg", "ident", "host")
	})
	forwarded, mu := trackForwarded(b)

	conn, _ := authBouncerClient(t, addr)
	writeLine(t, conn, "PONG :tb-12345")
	// Give the bouncer a beat to (not) forward it.
	time.Sleep(50 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	for _, line := range *forwarded {
		assert.NotContains(t, line, "PONG", "client PONG must not be forwarded upstream")
	}
}
