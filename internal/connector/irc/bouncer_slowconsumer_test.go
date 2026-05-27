package irc

import (
	"errors"
	"net"
	"os"
	"testing"
	"time"
)

// TestWriteWithTimeoutErrorsOnStalledConsumer: a client that never reads must
// not pin the writing goroutine. On net.Pipe a write blocks until the peer
// reads, so with no reader the bounded deadline fires and the write returns a
// timeout error — the signal the bouncer uses to drop a slow consumer instead
// of stalling the shared pooled fan-out.
func TestWriteWithTimeoutErrorsOnStalledConsumer(t *testing.T) {
	srv, cli := net.Pipe()
	defer func() { _ = srv.Close(); _ = cli.Close() }() // cli never reads

	err := writeWithTimeout(srv, []byte("PING :x\r\n"), 50*time.Millisecond)
	if err == nil {
		t.Fatal("write to a non-reading peer must time out, not block forever")
	}
	if !errors.Is(err, os.ErrDeadlineExceeded) {
		t.Fatalf("expected a deadline-exceeded error, got %v", err)
	}
}

// TestWriteWithTimeoutSucceedsWhenConsumerReads: the happy path — a reading
// peer gets the bytes and the write returns nil.
func TestWriteWithTimeoutSucceedsWhenConsumerReads(t *testing.T) {
	srv, cli := net.Pipe()
	defer func() { _ = srv.Close(); _ = cli.Close() }()

	go func() {
		buf := make([]byte, 64)
		_, _ = cli.Read(buf)
	}()

	if err := writeWithTimeout(srv, []byte("PING :x\r\n"), 2*time.Second); err != nil {
		t.Fatalf("write to a reading peer should succeed, got %v", err)
	}
}
