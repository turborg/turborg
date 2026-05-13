package main

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/turborg/turborg/internal/agent"
	"github.com/turborg/turborg/internal/connector/irc"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	settings, err := irc.LoadSettings()
	if err != nil {
		log.Error("config", "err", err)
		os.Exit(2)
	}
	if err := settings.Validate(); err != nil {
		log.Error("config", "err", err)
		os.Exit(2)
	}

	a := agent.New(log)
	a.AddConnector(irc.New(settings, log))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Info("turborg starting",
		"host", settings.Hostname, "port", settings.Port, "tls", settings.UseTLS,
		"nick", settings.Nick, "channels", settings.NormalizedChannels(),
	)

	if err := a.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("agent run", "err", err)
		os.Exit(1)
	}
	log.Info("turborg exited cleanly")
}
