// Package fakeirc provides an in-process IRC server stub used by integration
// tests. It auto-completes the handshake (NICK/USER → 001 + 376), records
// every line it receives, and exposes a predicate-based wait so tests can
// synchronize without sleeps.
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

type Server struct {
	tb       testing.TB
	listener net.Listener
	conn     net.Conn

	mu       sync.Mutex
	received []string
	wmu      sync.Mutex

	closed chan struct{}
}

func New(tb testing.TB) *Server {
	tb.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("fakeirc listen: %v", err)
	}
	s := &Server{tb: tb, listener: l, closed: make(chan struct{})}
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

// SendLine writes a server-originated line to the connected client. It is a
// no-op if no client is connected yet — callers should WaitFor handshake
// completion first.
func (s *Server) SendLine(line string) error {
	s.wmu.Lock()
	defer s.wmu.Unlock()
	if s.conn == nil {
		return errors.New("fakeirc: no client")
	}
	_, err := s.conn.Write([]byte(line + "\r\n"))
	return err
}

// WaitFor polls the received-lines slice every 10ms until pred returns true
// or the timeout expires.
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

		if strings.HasPrefix(line, "USER ") {
			_ = s.SendLine(fmt.Sprintf(":fake 001 %s :Welcome", s.firstNick()))
			_ = s.SendLine(fmt.Sprintf(":fake 376 %s :End of MOTD", s.firstNick()))
		}
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
