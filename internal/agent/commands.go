package agent

import (
	"context"
	"strings"
	"sync"
)

type CommandHandler func(ctx context.Context, env *InboundEnvelope, args []string) (*OutboundEnvelope, error)

// CommandGuard is a synchronous predicate run before dispatch. Returning
// false silently drops the command (no reply, no error). Mirrors Python's
// owner-only + throttle guard composition.
type CommandGuard func(env *InboundEnvelope) bool

type commandEntry struct {
	name    string
	handler CommandHandler
	guard   CommandGuard
}

// CommandRegistry parses text that starts with Prefix and dispatches to a
// registered handler. Names are case-insensitive (Python convention).
type CommandRegistry struct {
	prefix   string
	mu       sync.RWMutex
	commands map[string]commandEntry
	guard    CommandGuard
}

func NewCommandRegistry(prefix string) *CommandRegistry {
	if prefix == "" {
		prefix = "!"
	}
	return &CommandRegistry{
		prefix:   prefix,
		commands: map[string]commandEntry{},
	}
}

func (r *CommandRegistry) Prefix() string { return r.prefix }

// SetGuard installs a registry-wide guard run before every per-command
// guard. Used for global throttle / owner-only policies.
func (r *CommandRegistry) SetGuard(g CommandGuard) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.guard = g
}

func (r *CommandRegistry) Register(name string, handler CommandHandler, perCommandGuard CommandGuard) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := strings.ToLower(name)
	r.commands[key] = commandEntry{name: key, handler: handler, guard: perCommandGuard}
}

func (r *CommandRegistry) Names() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.commands))
	for name := range r.commands {
		out = append(out, name)
	}
	return out
}

// Parse returns the command name and whitespace-split args if text begins
// with the configured prefix, or ok=false otherwise. The result is
// populated on the envelope's Command/Args fields by callers that want to
// route through the registry.
func (r *CommandRegistry) Parse(text string) (name string, args []string, ok bool) {
	if !strings.HasPrefix(text, r.prefix) {
		return "", nil, false
	}
	rest := strings.TrimPrefix(text, r.prefix)
	rest = strings.TrimLeft(rest, " \t")
	if rest == "" {
		return "", nil, false
	}
	parts := strings.Fields(rest)
	return strings.ToLower(parts[0]), parts[1:], true
}

// Dispatch parses env.Text, locates the handler, runs guards, and returns
// the handler's OutboundEnvelope (or nil if the message wasn't a command,
// the command wasn't registered, or a guard rejected it). Errors from the
// handler propagate to the caller.
func (r *CommandRegistry) Dispatch(ctx context.Context, env *InboundEnvelope) (*OutboundEnvelope, error) {
	name, args, ok := r.Parse(env.Text)
	if !ok {
		return nil, nil
	}

	r.mu.RLock()
	entry, exists := r.commands[name]
	globalGuard := r.guard
	r.mu.RUnlock()
	if !exists {
		return nil, nil
	}

	env.Command = name
	env.Args = args

	if globalGuard != nil && !globalGuard(env) {
		return nil, nil
	}
	if entry.guard != nil && !entry.guard(env) {
		return nil, nil
	}
	return entry.handler(ctx, env, args)
}
