package irc

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"strings"
	"sync"
	"time"
)

type Client struct {
	conn   net.Conn
	reader *bufio.Reader
	wmu    sync.Mutex
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
	c.wmu.Lock()
	defer c.wmu.Unlock()
	_, err := c.conn.Write([]byte(line + "\r\n"))
	return err
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
