package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/turborg/turborg/internal/agent"
	"github.com/turborg/turborg/internal/connector/irc"
)

func main() {
	cfg := parseFlags()

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	a := agent.New(log)
	a.AddConnector(irc.New(cfg, log))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Info("turborg starting", "host", cfg.Host, "port", cfg.Port, "channel", cfg.Channel)

	if err := a.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Error("agent run", "err", err)
		os.Exit(1)
	}
	log.Info("turborg exited cleanly")
}

func parseFlags() irc.Config {
	var cfg irc.Config
	flag.StringVar(&cfg.Host, "host", env("TURBORG_IRC_HOST", "irc.libera.chat"), "IRC server host")
	flag.IntVar(&cfg.Port, "port", envInt("TURBORG_IRC_PORT", 6697), "IRC server port")
	flag.BoolVar(&cfg.TLS, "tls", envBool("TURBORG_IRC_TLS", true), "Use TLS")
	flag.StringVar(&cfg.Nick, "nick", env("TURBORG_IRC_NICK", "turborg-poc"), "IRC nickname")
	flag.StringVar(&cfg.User, "user", env("TURBORG_IRC_USERNAME", "turborg"), "IRC username")
	flag.StringVar(&cfg.Real, "real", env("TURBORG_IRC_REALNAME", "turborg Go port PoC"), "IRC realname")
	flag.StringVar(&cfg.Channel, "channel", env("TURBORG_IRC_CHANNELS", "#turborg-test"), "IRC channel to join")
	flag.Parse()
	return cfg
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func envInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		var n int
		if _, err := fmt.Sscanf(v, "%d", &n); err == nil {
			return n
		}
	}
	return fallback
}

func envBool(key string, fallback bool) bool {
	switch os.Getenv(key) {
	case "1", "true", "TRUE", "True", "yes", "YES":
		return true
	case "0", "false", "FALSE", "False", "no", "NO":
		return false
	}
	return fallback
}
