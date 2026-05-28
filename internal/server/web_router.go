package server

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"
)

// webRouterReadHeaderTimeout bounds the header read on an accepted request so a
// slow client can't pin a connection before the WS upgrade.
const webRouterReadHeaderTimeout = 10 * time.Second

// ServeWebGatewayRouter accepts the appui web-shell requests the sidecar proxy
// forwards to the pool and routes each to the tenant it addresses. The path
// carries the tenant id (`/c/<turborg_id>`) and the `?token=` query authorizes
// the upgrade — unlike the bouncer router there's no PROXY-v2 / SNI to parse,
// so this is a plain HTTP server. This is the pooled counterpart to a dedicated
// container's own gateway port: one listener fronts every pooled tenant's web
// shell, so the runtime doesn't reintroduce a per-tenant port pool.
//
// Blocks until ctx is cancelled or the listener fails.
func (s *Server) ServeWebGatewayRouter(ctx context.Context, addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("web gateway router listen %s: %w", addr, err)
	}
	return s.serveWebGatewayRouter(ctx, ln)
}

// serveWebGatewayRouter runs the HTTP server on an already-bound listener. Split
// from ServeWebGatewayRouter so tests can drive it with a listener on an
// ephemeral port (mirrors serveBouncerRouter).
func (s *Server) serveWebGatewayRouter(ctx context.Context, ln net.Listener) error {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /c/{turborg_id}", func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("turborg_id")
		if id == "" || !s.RouteWS(id, w, r) {
			http.Error(w, "no such tenant", http.StatusNotFound)
		}
	})

	srv := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: webRouterReadHeaderTimeout,
	}

	// Shutdown gets its own bounded background context — ctx is already
	// cancelled by the time this fires, so deriving from it would short-circuit
	// the graceful close. Same intentional pattern as web.Gateway.Serve.
	go func() { //nolint:gosec // G118
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(shutdownCtx)
	}()

	s.log.Info("web gateway router listening", "addr", ln.Addr().String())
	err := srv.Serve(ln)
	if errors.Is(err, http.ErrServerClosed) {
		return ctx.Err()
	}
	return err
}
