package irc

import (
	"net"
	"sync"
	"testing"
)

// recordingReporter captures Set/Clear calls for assertions.
type recordingReporter struct {
	mu    sync.Mutex
	set   map[int]string
	clear []int
}

func newRecordingReporter() *recordingReporter {
	return &recordingReporter{set: map[int]string{}}
}

func (r *recordingReporter) Set(port int, ident string) {
	r.mu.Lock()
	r.set[port] = ident
	r.mu.Unlock()
}

func (r *recordingReporter) Clear(port int) {
	r.mu.Lock()
	r.clear = append(r.clear, port)
	r.mu.Unlock()
}

// fakeConn is a net.Conn whose only meaningful method is LocalAddr; the embedded
// nil interface panics on any other call, which keeps the test honest (setClient
// must touch nothing but the local port).
type fakeConn struct {
	net.Conn
	port int
}

func (f fakeConn) LocalAddr() net.Addr {
	return &net.TCPAddr{IP: net.IPv4(10, 201, 1, 2), Port: f.port}
}

func TestSetClientReportsAndClearsIdent(t *testing.T) {
	rep := newRecordingReporter()
	c := New(&Settings{Nick: "testthisx", Username: "tf1dcv74w"}, nil, nil)
	c.SetIdentReporter(rep)

	// Connect: a client on source port 51440 → report (51440 → ident).
	c.setClient(&Client{conn: fakeConn{port: 51440}})
	if got := rep.set[51440]; got != "tf1dcv74w" {
		t.Fatalf("Set(51440) = %q; want tf1dcv74w", got)
	}

	// Reconnect onto a new port: clears the old, reports the new.
	c.setClient(&Client{conn: fakeConn{port: 52000}})
	if got := rep.set[52000]; got != "tf1dcv74w" {
		t.Fatalf("Set(52000) = %q; want tf1dcv74w", got)
	}
	if len(rep.clear) != 1 || rep.clear[0] != 51440 {
		t.Fatalf("clear = %v; want [51440]", rep.clear)
	}

	// Teardown: nil client clears the live port.
	c.setClient(nil)
	if len(rep.clear) != 2 || rep.clear[1] != 52000 {
		t.Fatalf("clear = %v; want [51440 52000]", rep.clear)
	}
}

func TestSetClientNilReporterDoesNotPanic(t *testing.T) {
	c := New(&Settings{Nick: "x", Username: "tfx"}, nil, nil)
	// No reporter installed — must be a no-op, not a nil-interface panic.
	c.setClient(&Client{conn: fakeConn{port: 51440}})
	c.setClient(nil)
}
