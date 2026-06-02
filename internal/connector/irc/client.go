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

func Dial(ctx context.Context, host string, port int, useTLS bool) (*Client, error) {
	return dial(ctx, host, port, useTLS, nil)
}

// dial is the internal Dial that accepts a custom *tls.Config. Tests
// supply one with InsecureSkipVerify against a self-signed listener so
// the TLS path can be exercised without setting up a real cert chain.
// Production callers go through Dial, which passes nil → defaults.
func dial(ctx context.Context, host string, port int, useTLS bool, tlsCfg *tls.Config) (*Client, error) {
	addr := fmt.Sprintf("%s:%d", host, port)
	var conn net.Conn
	var err error
	if useTLS {
		cfg := tlsCfg
		if cfg == nil {
			cfg = &tls.Config{MinVersion: tls.VersionTLS12}
		}
		d := &tls.Dialer{Config: cfg}
		conn, err = d.DialContext(ctx, "tcp", addr)
	} else {
		var nd net.Dialer
		conn, err = nd.DialContext(ctx, "tcp", addr)
	}
	if err != nil {
		return nil, fmt.Errorf("irc dial %s: %w", addr, err)
	}
	return &Client{
		conn:   conn,
		reader: bufio.NewReaderSize(conn, 4096),
	}, nil
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
