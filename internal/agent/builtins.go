package agent

import (
	"context"
	"sort"
	"strings"
)

// RegisterBuiltins installs commands that ship with every agent: ping
// (liveness probe), help (list known commands). Phase 1 keeps this list
// small; Phase 5 (runtime composition) adds owner-scoped admin commands.
func RegisterBuiltins(r *CommandRegistry) {
	r.Register("ping", pingCmd, nil)
	r.Register("help", helpCmd(r), nil)
}

func pingCmd(_ context.Context, env *InboundEnvelope, _ []string) (*OutboundEnvelope, error) {
	return ReplyTo(env, "pong"), nil
}

func helpCmd(r *CommandRegistry) CommandHandler {
	return func(_ context.Context, env *InboundEnvelope, _ []string) (*OutboundEnvelope, error) {
		names := r.Names()
		sort.Strings(names)
		text := "commands: " + strings.Join(names, ", ")
		return ReplyTo(env, text), nil
	}
}
