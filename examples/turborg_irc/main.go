// A minimal turborg IRC bot in ~40 lines.
//
// Run it:
//
//	export TURBORG_IRC_HOSTNAME=irc.libera.chat
//	export TURBORG_IRC_NICK=myturborg
//	export TURBORG_IRC_CHANNELS=#turborg-test
//	go run ./examples/turborg_irc
//
// In #turborg-test, type `!ping` or `!echo hello` — the bot replies.
// Ctrl-C unwinds cleanly within ~500ms.
//
// Same wiring the production `turborg` binary uses internally (see
// cmd/turborg/main.go); this file just trims it down to the smallest
// thing you can read end-to-end.
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/turborg/turborg/internal/agent"
	"github.com/turborg/turborg/internal/connector/irc"
)

func main() {
	ircCfg, err := irc.LoadSettings()
	if err != nil {
		log.Fatalf("config: %v (set TURBORG_IRC_HOSTNAME and TURBORG_IRC_NICK)", err)
	}
	if err := ircCfg.Validate(); err != nil {
		log.Fatalf("config: %v", err)
	}

	a := agent.New(nil)
	a.AddConnector(irc.New(ircCfg, nil, a.Events))

	// Built-in commands (ping, help, version) are already registered.
	// Add your own — the handler returns an outbound envelope or nil.
	a.Commands.Register("echo", func(_ context.Context, env *agent.InboundEnvelope, args []string) (*agent.OutboundEnvelope, error) {
		return agent.ReplyTo(env, strings.Join(args, " ")), nil
	}, nil)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := a.Run(ctx); err != nil {
		log.Printf("agent: %v", err)
		os.Exit(1)
	}
}
