package ident

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"time"
)

// routerReadHeaderTimeout bounds the header read on an accepted request so a
// slow caller can't pin a connection. The only caller is the sidecar on the
// same host, so this is generous.
const routerReadHeaderTimeout = 5 * time.Second

// ServeRouter answers the sidecar's ident-backing lookups:
//
//	GET /ident?port=<localSourcePort>  → 200 text/plain <ident> | 404
//
// The sidecar resolves an inbound RFC-1413 query to (containerIP, sourcePort)
// via conntrack, then asks the container that owns the connection — this router
// — for the ident. Bound to TURBORG_IDENT_ROUTER_ADDR; the sidecar (host
// network) reaches it on the container's bridge IP. Mirrors web_router.go.
//
// Blocks until ctx is cancelled or the listener fails.
func ServeRouter(ctx context.Context, addr string, reg *Registry) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("ident router listen %s: %w", addr, err)
	}
	return serveRouter(ctx, ln, reg)
}

// serveRouter runs the HTTP server on an already-bound listener. Split from
// ServeRouter so tests can drive it on an ephemeral port (mirrors
// serveWebGatewayRouter).
func serveRouter(ctx context.Context, ln net.Listener, reg *Registry) error {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /ident", func(w http.ResponseWriter, r *http.Request) {
		port, err := strconv.Atoi(r.URL.Query().Get("port"))
		if err != nil || port <= 0 || port > 65535 {
			http.Error(w, "bad port", http.StatusBadRequest)
			return
		}
		id, ok := reg.Lookup(port)
		if !ok {
			http.Error(w, "no such port", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write([]byte(id))
	})

	srv := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: routerReadHeaderTimeout,
	}

	// Shutdown gets its own bounded background context — ctx is already
	// cancelled by the time this fires (same pattern as serveWebGatewayRouter).
	go func() { //nolint:gosec // G118
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	err := srv.Serve(ln)
	if errors.Is(err, http.ErrServerClosed) {
		return ctx.Err()
	}
	return err
}
