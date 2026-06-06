package irc_test

import (
	"context"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/connector/irc"
)

func listenLocal(t *testing.T) (int, <-chan string) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	remote := make(chan string, 1)
	go func() {
		c, err := ln.Accept()
		if err != nil {
			remote <- ""
			return
		}
		defer func() { _ = c.Close() }()
		host, _, _ := net.SplitHostPort(c.RemoteAddr().String())
		remote <- host
	}()
	_, portStr, _ := net.SplitHostPort(ln.Addr().String())
	port, _ := strconv.Atoi(portStr)
	return port, remote
}

func TestDialBindsSourceIP(t *testing.T) {
	port, remote := listenLocal(t)

	cli, err := irc.Dial(context.Background(), "127.0.0.1", port, false, "127.0.0.1")
	require.NoError(t, err)
	require.NotNil(t, cli)

	select {
	case host := <-remote:
		require.Equal(t, "127.0.0.1", host, "server must see the bound source IP")
	case <-time.After(2 * time.Second):
		t.Fatal("no connection accepted")
	}
}

func TestDialIgnoresInvalidSourceIP(t *testing.T) {
	port, _ := listenLocal(t)
	// An unparseable source IP must be ignored (no bind), not fail the dial.
	cli, err := irc.Dial(context.Background(), "127.0.0.1", port, false, "not-an-ip")
	require.NoError(t, err)
	require.NotNil(t, cli)
}
