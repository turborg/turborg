// Package fakeirc provides an in-process IRC server stub used by tests.
// It auto-completes the IRCv3 handshake (CAP REQ ACK → optional SASL flow
// → 001 + 376 after USER), records every received line, and exposes a
// predicate-based wait so tests can synchronize without sleeps.
package fakeirc

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"testing"
	"time"
)

// SASLResult drives how the fake server replies during the SASL exchange.
type SASLResult int

const (
	// SASLDisabled: server does NOT ACK :sasl in CAP REQ — equivalent to
	// a network that doesn't support SASL. Connector should log a warning
	// and fall back to unauthenticated.
	SASLDisabled SASLResult = iota
	// SASLSuccess: ACK :sasl in CAP REQ, send AUTHENTICATE +, then 903.
	SASLSuccess
	// SASLFail: ACK :sasl, send AUTHENTICATE +, then 904.
	SASLFail
)

type Server struct {
	tb       testing.TB
	listener net.Listener
	sasl     SASLResult

	wmu  sync.Mutex
	conn net.Conn

	mu       sync.Mutex
	received []string

	closed chan struct{}
}

type Option func(*Server)

func WithSASL(r SASLResult) Option {
	return func(s *Server) { s.sasl = r }
}

func New(tb testing.TB, opts ...Option) *Server {
	tb.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("fakeirc listen: %v", err)
	}
	s := &Server{tb: tb, listener: l, closed: make(chan struct{})}
	for _, o := range opts {
		o(s)
	}
	go s.accept()
	return s
}

func (s *Server) Port() int { return s.listener.Addr().(*net.TCPAddr).Port }

func (s *Server) Received() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, len(s.received))
	copy(out, s.received)
	return out
}

// SendLine writes a server-originated line. Returns an error if no
// client is connected yet (callers should WaitFor the handshake first).
func (s *Server) SendLine(line string) error {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	if s.conn == nil {
		return errors.New("fakeirc: no client")
	}
	_, err := s.conn.Write([]byte(line + "\r\n"))
	return err
}

// WaitFor polls Received() every 10ms until pred returns true or the
// timeout expires.
func (s *Server) WaitFor(pred func([]string) bool, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if pred(s.Received()) {
			return true
		}
		time.Sleep(10 * time.Millisecond)
	}
	return false
}

func (s *Server) Close() {
	select {
	case <-s.closed:
		return
	default:
	}
	close(s.closed)
	_ = s.listener.Close()
	s.wmu.Lock()
	if s.conn != nil {
		_ = s.conn.Close()
	}
	s.wmu.Unlock()
}

func (s *Server) accept() {
	conn, err := s.listener.Accept()
	if err != nil {
		return
	}
	s.wmu.Lock()
	s.conn = conn
	s.wmu.Unlock()

	reader := bufio.NewReader(conn)
	for {
		select {
		case <-s.closed:
			return
		default:
		}
		line, err := reader.ReadString('\n')
		if err != nil {
			return
		}
		line = strings.TrimRight(line, "\r\n")
		s.mu.Lock()
		s.received = append(s.received, line)
		s.mu.Unlock()
		s.handleLine(line)
	}
}

func (s *Server) handleLine(line string) {
	switch {
	case strings.HasPrefix(line, "CAP REQ"):
		s.handleCapReq(line)
	case line == "AUTHENTICATE PLAIN":
		if s.sasl == SASLSuccess || s.sasl == SASLFail {
			_ = s.SendLine("AUTHENTICATE +")
		}
	case strings.HasPrefix(line, "AUTHENTICATE "):
		switch s.sasl {
		case SASLSuccess:
			_ = s.SendLine(fmt.Sprintf(":fake 903 %s :SASL successful", s.firstNick()))
		case SASLFail:
			_ = s.SendLine(fmt.Sprintf(":fake 904 %s :SASL failed", s.firstNick()))
		}
	case strings.HasPrefix(line, "USER "):
		nick := s.firstNick()
		_ = s.SendLine(fmt.Sprintf(":fake 001 %s :Welcome", nick))
		_ = s.SendLine(fmt.Sprintf(":fake 376 %s :End of MOTD", nick))
	case strings.HasPrefix(line, "JOIN "):
		// Real servers echo the client's own JOIN back so it can update
		// its membership state. fakeirc mirrors that — without it, the
		// connector never records its self-join, and subsequent member
		// JOINs to that channel hit a missing-channel no-op.
		target := strings.TrimSpace(strings.TrimPrefix(line, "JOIN "))
		nick := s.firstNick()
		_ = s.SendLine(fmt.Sprintf(":%s!~u@host JOIN %s", nick, target))
	}
}

func (s *Server) handleCapReq(line string) {
	idx := strings.Index(line, " :")
	if idx < 0 {
		return
	}
	caps := strings.Fields(line[idx+2:])
	ack := make([]string, 0, len(caps))
	nak := make([]string, 0, len(caps))
	for _, c := range caps {
		if c == "sasl" && s.sasl == SASLDisabled {
			nak = append(nak, c)
		} else {
			ack = append(ack, c)
		}
	}
	if len(ack) > 0 {
		_ = s.SendLine(":fake CAP * ACK :" + strings.Join(ack, " "))
	}
	if len(nak) > 0 {
		_ = s.SendLine(":fake CAP * NAK :" + strings.Join(nak, " "))
	}
}

func (s *Server) firstNick() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, l := range s.received {
		if strings.HasPrefix(l, "NICK ") {
			return strings.TrimSpace(strings.TrimPrefix(l, "NICK "))
		}
	}
	return "*"
}
