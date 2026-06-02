// Command turborg-server runs the multi-tenant ("pooled") turborg runtime:
// one process that holds N tenants, each an isolated agent. It runs alongside
// the single-tenant `turborg` binary, which stays the default for env-driven
// hobbyist/OSS deployments.
//
// M1 is lifecycle-only — tenants attach/detach from a file source but run no
// connector behaviour yet. See accounts-api/dev/PLAN-multi-tenancy.md (WS2).
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"runtime/debug"
	"strconv"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/spf13/cobra"
	"github.com/turborg/turborg/internal/ident"
	"github.com/turborg/turborg/internal/llm"
	"github.com/turborg/turborg/internal/logging"
	"github.com/turborg/turborg/internal/runtime"
	"github.com/turborg/turborg/internal/safe"
	"github.com/turborg/turborg/internal/server"
	"github.com/turborg/turborg/internal/version"
)

// defaultAnthropicModel mirrors config.Settings' ANTHROPIC_MODEL envDefault so
// the pooled process picks the same model the dedicated runtime does when the
// operator doesn't override it.
const defaultAnthropicModel = "claude-sonnet-4-6"

const defaultTenantsFile = "/etc/turborg/tenants.json"

func main() {
	if err := newRootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}

func newRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:           "turborg-server",
		Short:         "turborg-server — pooled multi-tenant turborg runtime",
		SilenceUsage:  true,
		SilenceErrors: true,
		Version:       version.Version,
	}
	root.SetVersionTemplate("{{.Version}}\n")
	root.AddCommand(newRunCmd())
	return root
}

func newRunCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "run",
		Short: "Run the pooled runtime, sourcing tenants from a JSON file.",
		Long: `Run the pooled multi-tenant runtime.

Tenants are loaded from the file at TURBORG_TENANTS_FILE (default
` + defaultTenantsFile + `), a JSON document of the shape:

  {"tenants": [{"turborg_id": "...", "runtime_mode": "pooled", ...}]}

M1 is lifecycle-only: tenants attach and detach but run no connector
behaviour yet.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runE(cmd.OutOrStderr())
		},
	}
}

// startOptionalRouter launches a pool router (bouncer / web gateway / ident) in
// a supervised goroutine when addr is non-empty; a non-graceful exit cancels the
// process so we fail fast rather than serve tenants nobody can reach. Empty addr
// is a no-op — the router is disabled (e.g. a file-source self-host).
func startOptionalRouter(ctx context.Context, name, addr string, cancel context.CancelFunc, log *slog.Logger, serve func(string) error) {
	if addr == "" {
		return
	}
	safe.Go(name, func() {
		if err := serve(addr); err != nil && !errors.Is(err, context.Canceled) {
			log.Error(name+" stopped", "err", err)
			cancel()
		}
	})
}

func runE(stderr interface{ Write(p []byte) (int, error) }) error {
	// Best-effort .env load — real env vars take precedence.
	_ = godotenv.Load()

	log, err := logging.New(stderr, envOr("TURBORG_LOG_LEVEL", "info"), envOr("TURBORG_LOG_FORMAT", "json"))
	if err != nil {
		return err
	}

	// Pooled runtime: a panicking goroutine must not take down the process
	// (and every other tenant in it). Recover + log instead of exiting.
	safe.SetPanicPolicy(safe.RecoverPolicy)

	// Soft memory limit so GC defends the pool under pressure rather than
	// letting one tenant push the shared process to an OOM kill that drops
	// every tenant. The watchdog (server) observes; this bounds.
	applyMemoryLimit(log)

	source, desc := selectSource(log)
	log.Info("turborg-server starting", "version", version.Version, "source", desc)

	sigCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithCancel(sigCtx)
	defer cancel()

	srv := server.New(source, log)
	// Point each tenant's connector-state emitter at the control plane (the same
	// accounts-api base the HTTP feed uses). Empty on the file-source/OSS path →
	// state-sync stays off. The receiver authorizes via the host token + host-
	// owns check, so the control-plane token suffices.
	srv.SetControlPlane(os.Getenv("TURBORG_CONTROL_PLANE_URL"), os.Getenv("TURBORG_CONTROL_PLANE_TOKEN"))

	// Shared LLM provider for LLM-type commands, built once from the pool
	// process's own env (one key per host, shared across all tenants — a
	// stateless HTTP client). The unified router (TURBORG_LLM_*) takes
	// precedence; otherwise the legacy TURBORG_ANTHROPIC_* envs are used.
	// Absent key → provider nil → LLM-type commands are skipped. A build error
	// is logged but never fatal: the agent must still serve.
	buildPoolLLM := func() (llm.Provider, error) {
		if kind := os.Getenv("TURBORG_LLM_PROVIDER"); kind != "" || os.Getenv("TURBORG_LLM_API_KEY") != "" {
			return runtime.BuildLLMProvider(kind, os.Getenv("TURBORG_LLM_BASE_URL"), os.Getenv("TURBORG_LLM_API_KEY"), os.Getenv("TURBORG_LLM_MODEL"))
		}
		model := os.Getenv("TURBORG_ANTHROPIC_MODEL")
		if model == "" {
			model = defaultAnthropicModel
		}
		return runtime.NewAnthropicProvider(os.Getenv("TURBORG_ANTHROPIC_API_KEY"), model)
	}
	switch provider, err := buildPoolLLM(); {
	case err != nil:
		log.Error("llm provider disabled", "err", err)
	case provider != nil:
		srv.SetLLM(provider)
		log.Info("llm provider enabled", "model", provider.Model())
	}

	// Pooled bouncer ingress: one PROXY-v2 router fronts every tenant (HAProxy
	// terminates TLS and forwards the SNI as the PROXY authority). Runs
	// alongside the tenant supervisor; a bind failure cancels the process so we
	// fail fast rather than serve tenants nobody can attach to. Empty addr
	// disables it (e.g. a file-source self-host that doesn't expose a bouncer).
	startOptionalRouter(ctx, "bouncer-router", os.Getenv("TURBORG_BOUNCER_ROUTER_ADDR"), cancel, log,
		func(a string) error { return srv.ServeBouncerRouter(ctx, a) })

	// Pooled web-shell ingress: one HTTP router fronts every tenant's `/ws`
	// gateway (the sidecar proxy forwards `/c/<turborg_id>` here when the tenant
	// has no dedicated container). Same fail-fast contract as the bouncer router.
	startOptionalRouter(ctx, "web-gateway-router", os.Getenv("TURBORG_GATEWAY_ROUTER_ADDR"), cancel, log,
		func(a string) error { return srv.ServeWebGatewayRouter(ctx, a) })

	// Ident router: the sidecar's RFC-1413 responder resolves an inbound ident
	// query to (this container's IP, source port) via conntrack, then asks here
	// for the tenant's ident. Empty addr disables it (no sidecar / OSS self-host).
	startOptionalRouter(ctx, "ident-router", os.Getenv("TURBORG_IDENT_ROUTER_ADDR"), cancel, log,
		func(a string) error { return ident.ServeRouter(ctx, a, srv.Idents()) })

	// Pool watchdog: periodic heap/goroutine/tenant sampling for observability
	// and early warning (escalates to Warn above TURBORG_HEAP_WARN_BYTES). 0
	// interval disables it.
	if interval := envIntOr("TURBORG_WATCHDOG_INTERVAL_SECONDS", 60); interval > 0 {
		heapWarn := envUint64Or("TURBORG_HEAP_WARN_BYTES", 0)
		safe.Go("watchdog", func() {
			srv.RunWatchdog(ctx, time.Duration(interval)*time.Second, heapWarn)
		})
	}

	if err := srv.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("pooled server: %w", err)
	}
	log.Info("turborg-server exited cleanly")
	return nil
}

// applyMemoryLimit sets the Go soft memory limit (GOMEMLIMIT) from
// TURBORG_GOMEMLIMIT_BYTES when present. The sidecar derives it from the pool
// container's memory allocation so GC works harder near the ceiling instead of
// the kernel OOM-killing a process that holds every pooled tenant. Unset keeps
// Go's default (the runtime still honours a natively-set GOMEMLIMIT env).
func applyMemoryLimit(log *slog.Logger) {
	v := os.Getenv("TURBORG_GOMEMLIMIT_BYTES")
	if v == "" {
		return
	}
	n, err := strconv.ParseInt(v, 10, 64)
	if err != nil || n <= 0 {
		log.Warn("ignoring invalid TURBORG_GOMEMLIMIT_BYTES", "value", v)
		return
	}
	debug.SetMemoryLimit(n)
	log.Info("GOMEMLIMIT applied", "bytes", n)
}

// selectSource picks the tenant source from the environment. When
// TURBORG_CONTROL_PLANE_URL is set, the hosted HTTP feed (accounts-api) is
// used; otherwise tenants come from the local JSON file (OSS / self-host).
func selectSource(log *slog.Logger) (server.TenantSource, string) {
	if base := os.Getenv("TURBORG_CONTROL_PLANE_URL"); base != "" {
		return &server.HTTPSource{
			BaseURL: base,
			Bearer:  os.Getenv("TURBORG_CONTROL_PLANE_TOKEN"),
			HostID:  os.Getenv("TURBORG_HOST_ID"),
			Log:     log,
		}, "http:" + base
	}
	path := envOr("TURBORG_TENANTS_FILE", defaultTenantsFile)
	return &server.FileSource{Path: path}, "file:" + path
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

// envIntOr reads a non-negative integer env var, falling back to def when
// unset, empty, or unparseable.
func envIntOr(key string, def int) int {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return def
	}
	return n
}

// envUint64Or reads an unsigned integer env var (e.g. a byte count), falling
// back to def when unset, empty, or unparseable.
func envUint64Or(key string, def uint64) uint64 {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	n, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return def
	}
	return n
}
