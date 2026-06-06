package irc

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"net"
	"strings"
	"sync"
	"time"
)

type Client struct {
	conn   net.Conn
	reader *bufio.Reader
	wmu    sync.Mutex

	// tlsCfg non-nil means the TLS handshake is deferred: Dial returned right
	// after the bare TCP connect — so the local source port is already known —
	// and Handshake completes the negotiation. nil means plaintext, or the
	// handshake already ran. ServerName is filled in by dial so the deferred
	// Handshake still verifies the cert hostname.
	tlsCfg *tls.Config

	logMu sync.RWMutex
	log   *slog.Logger
}

// SetLog wires a logger so every outbound line is traced at DEBUG.
// Safe to call after Dial; the connector calls this once at Start.
func (c *Client) SetLog(log *slog.Logger) {
	c.logMu.Lock()
	defer c.logMu.Unlock()
	c.log = log
}

func (c *Client) currentLog() *slog.Logger {
	c.logMu.RLock()
	defer c.logMu.RUnlock()
	return c.log
}

// LocalPort returns the local TCP source port of the upstream connection, or 0
// when unavailable. The pooled ident responder keys on this: SNAT preserves the
// source port for the pool process's egress, so the port an IRC server queries
// on :113 is exactly this one, letting an external responder map it back to the
// tenant's ident.
func (c *Client) LocalPort() int {
	if c.conn == nil {
		return 0
	}
	if a, ok := c.conn.LocalAddr().(*net.TCPAddr); ok {
		return a.Port
	}
	return 0
}

func Dial(ctx context.Context, host string, port int, useTLS bool, sourceIP string) (*Client, error) {
	return dial(ctx, host, port, useTLS, sourceIP, nil)
}

// dial is the internal Dial that accepts a custom *tls.Config. Tests
// supply one with InsecureSkipVerify against a self-signed listener so
// the TLS path can be exercised without setting up a real cert chain.
// Production callers go through Dial, which passes nil → defaults.
//
// sourceIP, when a valid IP, is bound as the connection's local address so the
// host SNAT egresses on the tenant's assigned public IP (pooled per-tenant
// egress). Empty / unparseable → default route.
func dial(ctx context.Context, host string, port int, useTLS bool, sourceIP string, tlsCfg *tls.Config) (*Client, error) {
	addr := fmt.Sprintf("%s:%d", host, port)
	nd := net.Dialer{}
	if ip := net.ParseIP(sourceIP); ip != nil {
		nd.LocalAddr = &net.TCPAddr{IP: ip}
	}
	// Always establish the bare TCP connection first, even for TLS. The local
	// source port is assigned at connect, and an IRC server probes our :113
	// ident the moment it accepts the TCP connection — i.e. before our TLS
	// handshake finishes. Returning here lets the caller register that port with
	// the ident responder, then complete the handshake via Handshake(). Folding
	// the handshake into the dial (the old tls.Dialer path) registered the port
	// only after the handshake and lost the race, yielding a ~-prefixed ident.
	conn, err := nd.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("irc dial %s: %w", addr, err)
	}
	c := &Client{
		conn:   conn,
		reader: bufio.NewReaderSize(conn, 4096),
	}
	if useTLS {
		cfg := tlsCfg
		if cfg == nil {
			cfg = &tls.Config{MinVersion: tls.VersionTLS12}
		}
		// tls.Dialer auto-filled ServerName from the dial host; tls.Client does
		// not, so set it ourselves or cert verification fails. Clone first to
		// avoid mutating a caller-supplied config.
		if cfg.ServerName == "" {
			cfg = cfg.Clone()
			cfg.ServerName = host
		}
		c.tlsCfg = cfg
	}
	return c, nil
}

// Handshake completes a deferred TLS handshake on a connection returned by Dial,
// and is a no-op on a plaintext connection. Separating it from the TCP connect
// is what lets the caller register the (already-assigned) local source port with
// the ident responder before the IRC server's :113 probe — which arrives on
// TCP-accept, ahead of this handshake — has to be answered.
func (c *Client) Handshake(ctx context.Context) error {
	if c.tlsCfg == nil {
		return nil
	}
	tlsConn := tls.Client(c.conn, c.tlsCfg)
	if err := tlsConn.HandshakeContext(ctx); err != nil {
		return fmt.Errorf("irc tls handshake %s: %w", c.tlsCfg.ServerName, err)
	}
	c.conn = tlsConn
	c.reader = bufio.NewReaderSize(tlsConn, 4096)
	c.tlsCfg = nil
	return nil
}

func (c *Client) WriteLine(line string) error {
	if log := c.currentLog(); log != nil {
		log.Debug("irc >>", "line", maskSecrets(line))
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	_, err := c.conn.Write([]byte(line + "\r\n"))
	return err
}

// maskSecrets redacts payloads that contain credentials so a DEBUG
// log stream stays safe to forward to log aggregators. Only the
// payload is masked; operators still see which command went out.
func maskSecrets(line string) string {
	upper := strings.ToUpper(line)
	switch {
	case strings.HasPrefix(upper, "PASS "):
		return "PASS <redacted>"
	case strings.HasPrefix(upper, "AUTHENTICATE "):
		// AUTHENTICATE PLAIN starts the exchange (no secret). The
		// follow-up (base64 creds) carries the secret — redact it.
		rest := strings.TrimSpace(line[len("AUTHENTICATE "):])
		if strings.EqualFold(rest, "PLAIN") {
			return line
		}
		return "AUTHENTICATE <redacted>"
	case strings.HasPrefix(upper, "PRIVMSG NICKSERV "):
		// `PRIVMSG NickServ :IDENTIFY <password>` and friends. Keep
		// the verb visible so the operator sees what we're doing;
		// mask the rest.
		idx := strings.Index(line, " :")
		if idx < 0 {
			return line
		}
		payload := line[idx+2:]
		head := strings.SplitN(payload, " ", 2)
		verb := strings.ToUpper(head[0])
		return line[:idx+2] + verb + " <redacted>"
	}
	return line
}

func (c *Client) ReadLine() (string, error) {
	line, err := c.reader.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimRight(line, "\r\n"), nil
}

// Unblock forces the next pending or in-flight read to return immediately by
// setting a past read deadline. Used by the connector's Run() loop to
// unwind the reader goroutine on shutdown without closing the conn yet
// (Stop still needs to send QUIT cleanly).
func (c *Client) Unblock() error {
	return c.conn.SetReadDeadline(time.Now())
}

// SetReadDeadline forwards to the underlying conn. Pass the zero
// time.Time to clear an existing deadline.
func (c *Client) SetReadDeadline(t time.Time) error {
	return c.conn.SetReadDeadline(t)
}

func (c *Client) Close() error {
	return c.conn.Close()
}
