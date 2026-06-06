package ident

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"testing"
	"time"
)

// startTestRouter binds an ephemeral listener and serves the router against reg,
// returning the base URL. It captures serveRouter's return and waits for it on
// cleanup, so the graceful-shutdown path is covered deterministically rather
// than left to goroutine scheduling at process exit (the flaky pattern).
func startTestRouter(t *testing.T, reg *Registry) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- serveRouter(ctx, ln, reg) }()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("serveRouter did not return within 2s of cancel")
		}
	})
	return "http://" + ln.Addr().String()
}

// get retries briefly to absorb the gap between Listen and Serve accepting.
func get(t *testing.T, url string) (int, string) {
	t.Helper()
	var lastErr error
	for range 20 {
		resp, err := http.Get(url) //nolint:noctx // test
		if err != nil {
			lastErr = err
			time.Sleep(10 * time.Millisecond)
			continue
		}
		defer func() { _ = resp.Body.Close() }()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}
	t.Fatalf("GET %s: %v", url, lastErr)
	return 0, ""
}

func TestRouterReturnsIdentForKnownPort(t *testing.T) {
	reg := NewRegistry()
	reg.Set(51440, "tf1dcv74w")
	base := startTestRouter(t, reg)

	code, body := get(t, base+"/ident?port=51440")
	if code != http.StatusOK || body != "tf1dcv74w" {
		t.Fatalf("known port = %d,%q; want 200,tf1dcv74w", code, body)
	}
}

func TestRouterNotFoundForUnknownPort(t *testing.T) {
	base := startTestRouter(t, NewRegistry())
	if code, _ := get(t, base+"/ident?port=51440"); code != http.StatusNotFound {
		t.Fatalf("unknown port = %d; want 404", code)
	}
}

func TestRouterBadPort(t *testing.T) {
	base := startTestRouter(t, NewRegistry())
	for _, p := range []string{"abc", "0", "70000", ""} {
		if code, _ := get(t, base+"/ident?port="+p); code != http.StatusBadRequest {
			t.Fatalf("port=%q = %d; want 400", p, code)
		}
	}
}

// TestServeRouterBindsAndStops covers the exported ServeRouter wrapper (it binds
// its own listener) and its clean return on context cancel.
func TestServeRouterBindsAndStops(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- ServeRouter(ctx, "127.0.0.1:0", NewRegistry()) }()

	time.Sleep(20 * time.Millisecond) // let it bind + start serving
	cancel()
	select {
	case err := <-done:
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("ServeRouter returned %v; want nil or context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("ServeRouter did not return within 2s of cancel")
	}
}

// TestServeRouterBindError covers the listen-failure branch of ServeRouter.
func TestServeRouterBindError(t *testing.T) {
	if err := ServeRouter(context.Background(), "bad-addr-no-port", NewRegistry()); err == nil {
		t.Fatal("expected a bind error for a malformed address")
	}
}
