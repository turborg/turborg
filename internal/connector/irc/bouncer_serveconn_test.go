package irc_test

import (
	"bufio"
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/connector/irc"
)

// TestBouncerListenerlessServeConn is the pooled-runtime seam: a bouncer
// brought up with StartListenerless binds no port, and a connection handed to
// ServeConn gets a fully-functional bouncer session — same pre-auth hint and
// PASS→001 handshake a listened connection would. This is what lets the
// pooled SNI/PROXY-v2 router front per-tenant bouncers without per-tenant
// ports, while the bouncer logic stays one source of truth across modes.
func TestBouncerListenerlessServeConn(t *testing.T) {
	b, err := irc.NewBouncer("hunter2", "127.0.0.1", 0, nil, nil)
	require.NoError(t, err)
	require.NoError(t, b.StartListenerless(context.Background()))
	t.Cleanup(func() { _ = b.Stop() })

	assert.Empty(t, b.Addr(), "listenerless bouncer must not bind a TCP listener")

	srv, cli := net.Pipe()
	t.Cleanup(func() { _ = cli.Close() })
	go b.ServeConn(srv)

	r := bufio.NewReader(cli)
	_ = cli.SetReadDeadline(time.Now().Add(2 * time.Second))
	notice, err := r.ReadString('\n')
	require.NoError(t, err)
	assert.Contains(t, notice, "NOTICE AUTH",
		"a served connection gets the same pre-auth hint as a listened one")

	_, err = cli.Write([]byte("PASS hunter2\r\n"))
	require.NoError(t, err)

	got001 := false
	for i := 0; i < 50; i++ {
		_ = cli.SetReadDeadline(time.Now().Add(2 * time.Second))
		line, err := r.ReadString('\n')
		if err != nil {
			break
		}
		if strings.Contains(line, " 001 ") {
			got001 = true
			break
		}
	}
	assert.True(t, got001, "PASS over a served connection must reach the 001 welcome")
}

// TestBouncerServeConnAfterStopClosesConn: a connection handed to a stopped
// (or never-started) bouncer is closed, not leaked — runCtx is nil so there's
// no lifecycle to attach it to.
func TestBouncerServeConnAfterStopClosesConn(t *testing.T) {
	b, err := irc.NewBouncer("hunter2", "127.0.0.1", 0, nil, nil)
	require.NoError(t, err)
	require.NoError(t, b.StartListenerless(context.Background()))
	require.NoError(t, b.Stop())

	srv, cli := net.Pipe()
	defer func() { _ = cli.Close() }()
	done := make(chan struct{})
	go func() { b.ServeConn(srv); close(done) }()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("ServeConn on a stopped bouncer must return promptly")
	}

	// The server end is closed, so a read from the client end ends in error
	// rather than hanging.
	_ = cli.SetReadDeadline(time.Now().Add(time.Second))
	buf := make([]byte, 1)
	_, err = cli.Read(buf)
	require.Error(t, err, "connection should be closed by ServeConn")
}
