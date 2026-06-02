package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	proxyproto "github.com/pires/go-proxyproto"

	"github.com/turborg/turborg/internal/safe"
)

// proxyHeaderReadTimeout bounds how long the router waits for the PROXY v2
// header on a freshly accepted connection. A slow or non-HAProxy peer must not
// pin a goroutine waiting for bytes that never come.
const proxyHeaderReadTimeout = 5 * time.Second

// ServeBouncerRouter accepts bouncer connections the edge HAProxy forwards to
// the pool and routes each to the tenant it belongs to. HAProxy terminates the
// wildcard TLS and sends a PROXY v2 header whose AUTHORITY TLV carries the
// original TLS SNI (`<turborg_id>.bouncer.<domain>`); the connection payload is
// plaintext IRC from there on. This is the pooled counterpart to a single-instance
// container's own bouncer port — one listener fronts every pooled tenant, so
// the runtime doesn't reintroduce a per-tenant port pool.
//
// Blocks until ctx is cancelled or the listener fails.
func (s *Server) ServeBouncerRouter(ctx context.Context, addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("bouncer router listen %s: %w", addr, err)
	}
	return s.serveBouncerRouter(ctx, ln)
}

// serveBouncerRouter wraps an already-bound listener in the PROXY-protocol
// reader and runs the accept loop. Split from ServeBouncerRouter so tests can
// drive it with a listener on an ephemeral port.
func (s *Server) serveBouncerRouter(ctx context.Context, ln net.Listener) error {
	pl := &proxyproto.Listener{
		Listener: ln,
		// REQUIRE: only the edge proxy (which always sends a PROXY header) may
		// reach the router. A direct connection has no header and is rejected,
		// so nobody can bypass tenant resolution by dialing the pool port.
		ConnPolicy: func(proxyproto.ConnPolicyOptions) (proxyproto.Policy, error) {
			return proxyproto.REQUIRE, nil
		},
	}
	defer func() { _ = pl.Close() }()

	go func() {
		<-ctx.Done()
		_ = pl.Close()
	}()

	s.log.Info("bouncer router listening", "addr", ln.Addr().String())
	for {
		conn, err := pl.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			s.log.Debug("bouncer router accept", "err", err)
			continue
		}
		safe.Go("bouncer-route", func() { s.routeBouncerConn(conn) })
	}
}

// routeBouncerConn resolves the tenant from the connection's PROXY v2 authority
// and hands it off. Any failure — missing/!proxy header, no authority TLV,
// unknown tenant — closes the connection so nothing leaks.
func (s *Server) routeBouncerConn(conn net.Conn) {
	pc, ok := conn.(*proxyproto.Conn)
	if !ok {
		s.log.Warn("bouncer router: non-proxy connection", "remote", conn.RemoteAddr())
		_ = conn.Close()
		return
	}

	// Bound the header read; clear the deadline before handoff so the bouncer
	// manages its own (keepalive) deadlines on the same conn.
	_ = pc.SetReadDeadline(time.Now().Add(proxyHeaderReadTimeout))
	header := pc.ProxyHeader()
	_ = pc.SetReadDeadline(time.Time{})
	if header == nil {
		s.log.Warn("bouncer router: missing proxy header", "remote", conn.RemoteAddr())
		_ = conn.Close()
		return
	}

	id, err := turborgIDFromAuthority(header)
	if err != nil {
		s.log.Warn("bouncer router: cannot resolve tenant", "err", err, "remote", conn.RemoteAddr())
		_ = conn.Close()
		return
	}

	if !s.RouteBouncerConn(id, conn) {
		s.log.Info("bouncer router: no such tenant", "turborg_id", id)
		_ = conn.Close()
	}
}

// turborgIDFromAuthority reads the PROXY v2 AUTHORITY TLV (the TLS SNI HAProxy
// forwarded) and returns its first DNS label — the turborg_id in
// `<turborg_id>.bouncer.<domain>`.
func turborgIDFromAuthority(header *proxyproto.Header) (string, error) {
	tlvs, err := header.TLVs()
	if err != nil {
		return "", fmt.Errorf("parse tlvs: %w", err)
	}
	for _, tlv := range tlvs {
		if tlv.Type != proxyproto.PP2_TYPE_AUTHORITY {
			continue
		}
		authority := string(tlv.Value)
		label, _, _ := strings.Cut(authority, ".")
		if label == "" {
			return "", fmt.Errorf("empty turborg_id label in authority %q", authority)
		}
		return label, nil
	}
	return "", errors.New("no authority tlv in proxy header")
}
