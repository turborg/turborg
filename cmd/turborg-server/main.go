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
	"syscall"

	"github.com/joho/godotenv"
	"github.com/spf13/cobra"
	"github.com/turborg/turborg/internal/logging"
	"github.com/turborg/turborg/internal/server"
	"github.com/turborg/turborg/internal/version"
)

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

func runE(stderr interface{ Write(p []byte) (int, error) }) error {
	// Best-effort .env load — real env vars take precedence.
	_ = godotenv.Load()

	log, err := logging.New(stderr, envOr("TURBORG_LOG_LEVEL", "info"), envOr("TURBORG_LOG_FORMAT", "json"))
	if err != nil {
		return err
	}

	source, desc := selectSource(log)
	log.Info("turborg-server starting", "version", version.Version, "source", desc)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	srv := server.New(source, log)
	if err := srv.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		return fmt.Errorf("pooled server: %w", err)
	}
	log.Info("turborg-server exited cleanly")
	return nil
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
