// Command turborg runs a turborg agent from environment-derived
// settings. See README.md for the env-var contract.
package main

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/joho/godotenv"
	"github.com/spf13/cobra"
	"github.com/turborg/turborg/internal/config"
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
	return root
}

func newRunCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "run",
		Short: "Run a turborg agent from environment-derived settings.",
		Long: `Run a turborg agent from environment-derived settings.

Default path: a single IRC connector. Required env:
  TURBORG_IRC_HOSTNAME, TURBORG_IRC_NICK, TURBORG_IRC_CHANNELS

Optional: TURBORG_ANTHROPIC_API_KEY enables the !ask command.

Multi-connector: set TURBORG_CONNECTORS=irc[,...] to register every listed
connector under one agent process. Each connector reads its own
configuration from its prefixed env.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runE(cmd.OutOrStderr())
		},
	}
}

func runE(stderr interface{ Write(p []byte) (int, error) }) error {
	// Best-effort .env load matching the Python pydantic-settings
	// behavior. A missing file is fine — real env vars still apply.
	// Real env vars take precedence over .env entries (godotenv.Load
	// only sets vars that aren't already set in the process env).
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

	mode := "standalone"
	if built.LLM != nil {
		mode = "llm"
	}
	bouncerState := "off"
	if ircSettings.BouncerEnabled() {
		bouncerState = fmt.Sprintf("on@%s:%d", ircSettings.BouncerHost, ircSettings.BouncerPort)
	}
	webState := "off"
	if built.Gateway != nil {
		webState = fmt.Sprintf("on@%s:%d", settings.WebHost, settings.WebPort)
	}
	log.Info("turborg starting",
		"version", version.Version,
		"mode", mode,
		"connectors", connectorNames(settings),
		"bouncer", bouncerState,
		"web", webState,
	)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

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
