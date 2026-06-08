package irc

import (
	"net"
	"testing"
)

// TestClientLocalPort_EdgeCases covers the two non-happy LocalPort paths: a
// client with no connection yet, and a connection whose local address is not a
// *net.TCPAddr. Both must report 0 rather than panic — the pooled ident
// responder treats 0 as "unavailable".
func TestClientLocalPort_EdgeCases(t *testing.T) {
	// No connection: LocalPort is 0.
	if got := (&Client{}).LocalPort(); got != 0 {
		t.Fatalf("LocalPort on a nil-conn client = %d, want 0", got)
	}

	// Non-TCP connection (net.Pipe's addr is not *net.TCPAddr): LocalPort is 0.
	p1, p2 := net.Pipe()
	t.Cleanup(func() { _ = p1.Close(); _ = p2.Close() })
	if got := (&Client{conn: p1}).LocalPort(); got != 0 {
		t.Fatalf("LocalPort on a non-TCP conn = %d, want 0", got)
	}
}
