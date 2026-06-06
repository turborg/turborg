package agent

import (
	"context"
	"errors"
	"strings"
	"sync"
)

type CommandHandler func(ctx context.Context, env *InboundEnvelope, args []string) (*OutboundEnvelope, error)

// CommandGuard is a synchronous predicate run before dispatch. Returning
// false silently drops the command (no reply, no error). The runtime
// composes owner-only + per-sender throttle into a single guard.
type CommandGuard func(env *InboundEnvelope) bool

type commandEntry struct {
	name    string
	handler CommandHandler
	guard   CommandGuard
	// dynamic marks entries installed via the user-defined path
	// (RegisterDynamic / ReplaceDynamic) as opposed to Register. Only
	// dynamic entries are governed by the MaxDynamic cap and swapped out
	// by ReplaceDynamic; Register entries are left untouched.
	dynamic bool
}

// DynamicCommand is one user-defined command in a ReplaceDynamic batch:
// a trigger name plus its handler and optional per-command guard.
type DynamicCommand struct {
	Name    string
	Handler CommandHandler
	Guard   CommandGuard
}

// CommandRegistry parses text that starts with Prefix and dispatches to a
// registered handler. Names are case-insensitive.
type CommandRegistry struct {
	prefix       string
	mu           sync.RWMutex
	commands     map[string]commandEntry
	guard        CommandGuard
	maxDynamic   int
	dynamicCount int
}

func NewCommandRegistry(prefix string) *CommandRegistry {
	if prefix == "" {
		prefix = "!"
	}
	return &CommandRegistry{
		prefix:   prefix,
		commands: map[string]commandEntry{},
		// Default: dynamic registrations not allowed. Operators that
		// want to let users register their own commands set
		// SetMaxDynamic at startup. -1 = unrestricted.
		maxDynamic: 0,
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

// SetMaxDynamic caps how many user-defined (dynamic) commands the
// registry will accept via RegisterDynamic. 0 = none allowed (default);
// -1 = unrestricted; positive values cap at N. Builtin commands
// registered via Register are uncapped — this only governs the dynamic
// path. Called once at startup; not safe for concurrent reassignment.
func (r *CommandRegistry) SetMaxDynamic(n int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.maxDynamic = n
}

// Register installs a command handler unconditionally. Used for
// builtins wired at startup and other internally-trusted callers. See
// RegisterDynamic for the capped path open to user-defined commands.
func (r *CommandRegistry) Register(name string, handler CommandHandler, perCommandGuard CommandGuard) {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := strings.ToLower(name)
	r.commands[key] = commandEntry{name: key, handler: handler, guard: perCommandGuard}
}

// ErrDynamicCommandLimit signals that RegisterDynamic refused to install
// the handler because the configured cap is already reached. The caller
// surfaces this back to whoever asked (HTTP API, IRC command, etc).
var ErrDynamicCommandLimit = errors.New("agent: dynamic command limit reached")

// RegisterDynamic is the capped registration path used by anything that
// installs user-defined commands at runtime. Returns
// ErrDynamicCommandLimit when the registry's MaxDynamic cap (0 by
// default — see SetMaxDynamic) would be exceeded. Re-registering an
// existing name overwrites without consuming a new slot.
func (r *CommandRegistry) RegisterDynamic(name string, handler CommandHandler, perCommandGuard CommandGuard) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	key := strings.ToLower(name)
	_, alreadyExists := r.commands[key]
	if !alreadyExists && r.maxDynamic != -1 && r.dynamicCount >= r.maxDynamic {
		return ErrDynamicCommandLimit
	}
	r.commands[key] = commandEntry{name: key, handler: handler, guard: perCommandGuard, dynamic: true}
	if !alreadyExists {
		r.dynamicCount++
	}
	return nil
}

// ReplaceDynamic atomically swaps the entire set of dynamic (user-defined)
// commands for the given batch, leaving Register-installed entries
// untouched. It is the hot-reload primitive: the pooled runtime calls it
// when a tenant's attached commands change, and the single-instance control
// endpoint calls it on a push — neither drops the IRC connection.
//
// The swap happens under the write lock; Dispatch only ever RLocks, so a
// concurrent command sees the complete old set or the complete new set,
// never a torn mix. MaxDynamic is enforced as a safety net (the control
// plane caps attachments upstream); entries beyond the cap are dropped.
// A dynamic command may not shadow a Register-installed (built-in) name.
func (r *CommandRegistry) ReplaceDynamic(cmds []DynamicCommand) {
	r.mu.Lock()
	defer r.mu.Unlock()

	for key, e := range r.commands {
		if e.dynamic {
			delete(r.commands, key)
		}
	}
	r.dynamicCount = 0

	for _, c := range cmds {
		key := strings.ToLower(c.Name)
		if existing, ok := r.commands[key]; ok {
			if !existing.dynamic {
				continue // never shadow a Register-installed command
			}
			// Duplicate name within the batch — overwrite, no new slot.
			r.commands[key] = commandEntry{name: key, handler: c.Handler, guard: c.Guard, dynamic: true}
			continue
		}
		if r.maxDynamic != -1 && r.dynamicCount >= r.maxDynamic {
			continue // safety net; the control plane caps attachments
		}
		r.commands[key] = commandEntry{name: key, handler: c.Handler, guard: c.Guard, dynamic: true}
		r.dynamicCount++
	}
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
