package irc_test

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/connector/irc"
)

// TestBringUpDialTimeoutDoesNotHang covers the libera-throttle failure mode:
// a node accepts the TCP connection but never completes the TLS handshake
// (it zero-windows us mid-ClientHello). Without a dial deadline the connect
// blocks indefinitely on DialContext; with DialTimeout it must error promptly
// so the supervisor's backoff/re-resolve loop can rotate to a healthy node.
func TestBringUpDialTimeoutDoesNotHang(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer func() { _ = ln.Close() }()

	// Accept and hold connections open, never responding — the stall.
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			defer func() { _ = c.Close() }()
		}
	}()

	conn := irc.New(&irc.Settings{
		Hostname:    "127.0.0.1",
		Port:        ln.Addr().(*net.TCPAddr).Port,
		UseTLS:      true,
		Nick:        "turborg",
		DialTimeout: 200 * time.Millisecond,
	}, slog.New(slog.NewTextHandler(io.Discard, nil)), nil)

	start := time.Now()
	err = conn.Start(context.Background())
	elapsed := time.Since(start)

	require.Error(t, err, "a stalled TLS dial must error, not hang")
	require.Less(t, elapsed, 3*time.Second,
		"dial must abort at the ~200ms deadline, not block indefinitely (took %s)", elapsed)
}
