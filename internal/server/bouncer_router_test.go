package server

import (
	"context"
	"log/slog"
	"net"
	"testing"
	"time"

	proxyproto "github.com/pires/go-proxyproto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func authorityHeader(authority string) *proxyproto.Header {
	h := &proxyproto.Header{
		Version:           2,
		Command:           proxyproto.PROXY,
		TransportProtocol: proxyproto.TCPv4,
		SourceAddr:        &net.TCPAddr{IP: net.IPv4(1, 2, 3, 4), Port: 12345},
		DestinationAddr:   &net.TCPAddr{IP: net.IPv4(5, 6, 7, 8), Port: 7900},
	}
	if authority != "" {
		_ = h.SetTLVs([]proxyproto.TLV{{Type: proxyproto.PP2_TYPE_AUTHORITY, Value: []byte(authority)}})
	}
	return h
}

func TestTurborgIDFromAuthority(t *testing.T) {
	id, err := turborgIDFromAuthority(authorityHeader("alice-7f3b.bouncer.irc.example.test"))
	require.NoError(t, err)
	assert.Equal(t, "alice-7f3b", id, "turborg_id is the first DNS label of the SNI authority")

	id, err = turborgIDFromAuthority(authorityHeader("bareid"))
	require.NoError(t, err)
	assert.Equal(t, "bareid", id, "an authority with no dot is the whole label")

	_, err = turborgIDFromAuthority(authorityHeader(""))
	require.Error(t, err, "a header with no AUTHORITY TLV is an error, not a silent empty id")

	_, err = turborgIDFromAuthority(authorityHeader(".bouncer.irc.example.test"))
	require.Error(t, err, "an authority with an empty leading label has no turborg_id")
}

// TestRouteBouncerConnNonProxyConnCloses: a connection that isn't a
// *proxyproto.Conn (no header to read) is closed rather than leaked.
func TestRouteBouncerConnNonProxyConnCloses(t *testing.T) {
	s := New(nil, slog.Default())
	srv, cli := net.Pipe()
	t.Cleanup(func() { _ = cli.Close() })

	s.routeBouncerConn(srv)

	_ = cli.SetReadDeadline(time.Now().Add(time.Second))
	_, err := cli.Read(make([]byte, 1))
	assert.Error(t, err, "a non-PROXY connection must be closed")
}

// TestBouncerRouterNoAuthorityClosesConn drives the accept path with a valid
// PROXY v2 header that carries no AUTHORITY TLV: the router can't resolve a
// tenant and closes the connection.
func TestBouncerRouterNoAuthorityClosesConn(t *testing.T) {
	s := New(nil, slog.Default())
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = s.serveBouncerRouter(ctx, ln) }()

	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	_, err = authorityHeader("").WriteTo(conn) // valid v2 header, no AUTHORITY TLV
	require.NoError(t, err)

	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err = conn.Read(make([]byte, 1))
	assert.Error(t, err, "a header with no authority resolves to no tenant and is closed")
}

// TestServeBouncerRouterBindsAndStops covers the bind+delegate wrapper and a
// clean shutdown on context cancel.
func TestServeBouncerRouterBindsAndStops(t *testing.T) {
	s := New(nil, slog.Default())
	ctx, cancel := context.WithCancel(context.Background())

	errc := make(chan error, 1)
	go func() { errc <- s.ServeBouncerRouter(ctx, "127.0.0.1:0") }()

	time.Sleep(50 * time.Millisecond) // let the listener bind
	cancel()

	select {
	case err := <-errc:
		assert.ErrorIs(t, err, context.Canceled, "router returns ctx.Err() on cancel")
	case <-time.After(2 * time.Second):
		t.Fatal("router did not stop on context cancel")
	}
}

func TestRouteBouncerConnLookup(t *testing.T) {
	s := New(nil, slog.Default())
	s.tenants["known"] = &Tenant{ID: "known", log: slog.Default()} // no live ircConn

	// Unknown tenant: false, and the conn is left for the caller to close.
	c1, c1peer := net.Pipe()
	t.Cleanup(func() { _ = c1.Close(); _ = c1peer.Close() })
	assert.False(t, s.RouteBouncerConn("missing", c1), "missing tenant routes false")

	// Known tenant with no running connector: true, and the tenant closes the
	// routed conn (nothing to attach to between runs).
	srv, cli := net.Pipe()
	t.Cleanup(func() { _ = cli.Close() })
	assert.True(t, s.RouteBouncerConn("known", srv), "attached tenant routes true")
	_ = cli.SetReadDeadline(time.Now().Add(time.Second))
	_, err := cli.Read(make([]byte, 1))
	assert.Error(t, err, "tenant with no live connector closes the routed conn")
}

// TestBouncerRouterUnknownTenantClosesConn drives the full accept path: a real
// PROXY v2 header is written to the router, which parses the authority,
// resolves no tenant, and closes the connection.
func TestBouncerRouterUnknownTenantClosesConn(t *testing.T) {
	s := New(nil, slog.Default())
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = s.serveBouncerRouter(ctx, ln) }()

	conn, err := net.Dial("tcp", addr)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	_, err = authorityHeader("ghost-tenant.bouncer.irc.example.test").WriteTo(conn)
	require.NoError(t, err)

	_ = conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	_, err = conn.Read(make([]byte, 1))
	assert.Error(t, err, "router must close a conn whose authority resolves to no tenant")
}
