// Package safe provides panic-recovering goroutine launches. Every long-lived
// goroutine in turborg should start via safe.Go so a panic becomes a logged,
// policy-controlled event instead of an unrecovered crash with a raw stack.
//
// The post-recovery action is a process-wide policy (multi-tenancy plan E4):
//
//   - single-tenant turborg: the default Exit policy — log + os.Exit(1),
//     preserving today's crash-then-container-restart semantics but with a
//     structured log + stack first.
//   - pooled turborg-server: Recover policy — log and keep the process alive,
//     so one tenant's panicking goroutine can't take down the whole pool.
//
// Set the policy once at process boot via SetPanicPolicy.
package safe

import (
	"log/slog"
	"os"
	"runtime/debug"
	"sync/atomic"
)

// PanicPolicy decides what happens after a recovered goroutine panic. name
// labels the goroutine; recovered is the panic value; stack is the captured
// debug stack.
type PanicPolicy func(name string, recovered any, stack []byte)

// exitFunc is the process-exit hook, swappable in tests so the default policy
// is exercisable without killing the test binary.
var exitFunc = os.Exit

var policy atomic.Pointer[PanicPolicy]

func init() {
	p := PanicPolicy(ExitPolicy)
	policy.Store(&p)
}

// SetPanicPolicy installs the process-wide policy. Call once at boot.
func SetPanicPolicy(p PanicPolicy) {
	policy.Store(&p)
}

// Go runs fn in a goroutine, recovering any panic and routing it to the
// configured policy. name is a stable label for logs/metrics.
func Go(name string, fn func()) {
	go func() {
		defer func() {
			if r := recover(); r != nil {
				(*policy.Load())(name, r, debug.Stack())
			}
		}()
		fn()
	}()
}

// ExitPolicy logs the panic and terminates the process (default). Single-
// tenant semantics: a panicking goroutine crashes the container, which the
// orchestrator restarts — same as before, now with a structured log first.
func ExitPolicy(name string, recovered any, stack []byte) {
	slog.Error("fatal goroutine panic", "goroutine", name, "panic", recovered, "stack", string(stack))
	exitFunc(1)
}

// RecoverPolicy logs the panic and lets the process continue. Pooled
// semantics: the offending goroutine dies, but the process — and every other
// tenant in it — survives.
func RecoverPolicy(name string, recovered any, stack []byte) {
	slog.Error("recovered goroutine panic", "goroutine", name, "panic", recovered, "stack", string(stack))
}
