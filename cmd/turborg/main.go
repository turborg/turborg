// Command turborg runs a turborg agent from environment-derived
// settings. See README.md for the env-var contract.
package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/joho/godotenv"
	"github.com/spf13/cobra"
	"github.com/turborg/turborg/internal/config"
	"github.com/turborg/turborg/internal/connector/irc"
	"github.com/turborg/turborg/internal/ident"
	"github.com/turborg/turborg/internal/logging"
	"github.com/turborg/turborg/internal/runtime"
	"github.com/turborg/turborg/internal/version"
)

func main() {
	if err := newRootCmd().Execute(); err != nil {
		os.Exit(1)
	}
}

func newRootCmd() *cobra.Command {
	root := &cobra.Command{
		Use:           "turborg",
		Short:         "turborg — modular AI agent framework",
		SilenceUsage:  true,
		SilenceErrors: true,
		Version:       version.Version,
	}
	root.SetVersionTemplate("{{.Version}}\n")
	root.AddCommand(newRunCmd())
	root.AddCommand(newHealthcheckCmd())
	return root
}

func newHealthcheckCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "healthcheck",
		Short: "Probe the local gateway health endpoint.",
		RunE: func(_ *cobra.Command, _ []string) error {
			return healthcheckE()
		},
	}
}

func newRunCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "run",
		Short: "Run a turborg agent from environment-derived settings.",
		Long: `Run a turborg agent from environment-derived settings.

Default path: a single IRC connector. Required env:
  TURBORG_IRC_HOSTNAME, TURBORG_IRC_NICK, TURBORG_IRC_CHANNELS

Commands are data-driven: define them in TURBORG_COMMANDS (a JSON array).
Optional: TURBORG_LLM_API_KEY (or TURBORG_ANTHROPIC_API_KEY) backs llm-type commands.

Multi-connector: set TURBORG_CONNECTORS=irc[,...] to register every listed
connector under one agent process. Each connector reads its own
configuration from its prefixed env.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runE(cmd.OutOrStderr())
		},
	}
}

func runE(stderr interface{ Write(p []byte) (int, error) }) error {
	// Best-effort .env load. A missing file is fine — real env vars
	// still apply. Real env vars take precedence over .env entries
	// (godotenv.Load only sets vars that aren't already set in the
	// process env).
	_ = godotenv.Load()

	settings, err := config.Load()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	ircSettings, err := runtime.LoadIRCSettings()
	if err != nil {
		return err
	}

	log, err := logging.New(stderr, settings.LogLevel, settings.LogFormat)
	if err != nil {
		return err
	}

	built, err := runtime.Build(settings, ircSettings, log)
	if err != nil {
		return err
	}

	// Bound this process's connect rate per (egress IP, host). For a
	// dedicated container this mainly spaces its own reconnects; combined
	// with the reconnect floor it keeps a single agent from flooding a
	// network. Shares the same env knob as the pooled runtime.
	irc.EnableConnectGateFromEnv()

	mode := "standalone"
	if built.LLM != nil {
		mode = "llm"
	}
	bouncerState := "off"
	if ircSettings.BouncerEnabled() {
		bouncerState = fmt.Sprintf("on@%s:%d", ircSettings.BouncerHost, ircSettings.BouncerPort)
	}
	gatewayState := "off"
	if built.Gateway != nil {
		gatewayState = fmt.Sprintf("on@%s:%d", settings.GatewayHost, settings.GatewayPort)
	}
	log.Info("turborg starting",
		"version", version.Version,
		"mode", mode,
		"connectors", connectorNames(settings),
		"bouncer", bouncerState,
		"gateway", gatewayState,
	)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Ident reporting for an external RFC-1413 (ident) responder — same
	// mechanism as the pooled runtime, so the bot names itself on
	// "identd required" networks and drops the ~ prefix. The connector reports
	// its source port → ident; the router (this host's IP) serves it to the
	// responder, which resolved the inbound query to us. Set before runtime.Run
	// starts the connector. Disabled when the addr is unset; a bind failure is
	// non-fatal — the bot still connects, just unverified.
	if built.IRC != nil {
		idents := ident.NewRegistry()
		built.IRC.SetIdentReporter(idents)
		if identAddr := os.Getenv("TURBORG_IDENT_ROUTER_ADDR"); identAddr != "" {
			go func() {
				if err := ident.ServeRouter(ctx, identAddr, idents); err != nil && !errors.Is(err, context.Canceled) {
					log.Error("ident router stopped", "err", err)
				}
			}()
		}
	}

	if err := runtime.Run(ctx, built); err != nil && !errors.Is(err, context.Canceled) {
		return err
	}
	log.Info("turborg exited cleanly")
	return nil
}

func connectorNames(s *config.Settings) []string {
	if len(s.Connectors) > 0 {
		return s.Connectors
	}
	return []string{"irc"}
}

func healthcheckE() error {
	settings, err := config.Load()
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}

	url := fmt.Sprintf(
		"http://%s:%d/health",
		settings.GatewayHost,
		settings.GatewayPort,
	)

	return healthcheckURL(url)
}

func healthcheckURL(url string) error {
	client := &http.Client{
		Timeout: 5 * time.Second,
	}

	resp, err := client.Get(url) //nolint:gosec
	if err != nil {
		return err
	}

	defer func() {
		_ = resp.Body.Close()
	}()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("gateway unhealthy: status %d", resp.StatusCode)
	}

	return nil
}
