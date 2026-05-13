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
	addr := fmt.Sprintf("%s:%d", host, port)
	var conn net.Conn
	var err error
	if useTLS {
		d := &tls.Dialer{Config: &tls.Config{MinVersion: tls.VersionTLS12}}
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
// setting a past read deadline. Used by the connector's ctx watchdog to
// unwind the reader goroutine on shutdown without closing the conn yet
// (Stop still needs to send QUIT cleanly).
func (c *Client) Unblock() error {
	return c.conn.SetReadDeadline(time.Now())
}

func (c *Client) Close() error {
	return c.conn.Close()
}
