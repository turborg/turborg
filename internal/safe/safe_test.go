package safe

import (
	"sync"
	"testing"
	"time"
)

// restorePolicy resets the global policy after a test mutates it.
func restorePolicy(t *testing.T) {
	t.Helper()
	t.Cleanup(func() {
		p := PanicPolicy(ExitPolicy)
		policy.Store(&p)
	})
}

func TestGoRunsFn(t *testing.T) {
	restorePolicy(t)
	done := make(chan struct{})
	Go("noop", func() { close(done) })
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("fn did not run")
	}
}

func TestGoRoutesPanicToPolicy(t *testing.T) {
	restorePolicy(t)
	var (
		mu      sync.Mutex
		gotName string
		gotPan  any
	)
	SetPanicPolicy(func(name string, recovered any, _ []byte) {
		mu.Lock()
		gotName, gotPan = name, recovered
		mu.Unlock()
	})

	Go("boom", func() { panic("kaboom") })

	deadline := time.After(2 * time.Second)
	for {
		mu.Lock()
		n, p := gotName, gotPan
		mu.Unlock()
		if n == "boom" && p == "kaboom" {
			return
		}
		select {
		case <-deadline:
			t.Fatalf("policy not invoked with panic; name=%q panic=%v", n, p)
		case <-time.After(5 * time.Millisecond):
		}
	}
}

func TestExitPolicyCallsExit(t *testing.T) {
	restorePolicy(t)
	orig := exitFunc
	t.Cleanup(func() { exitFunc = orig })

	var code int
	called := false
	exitFunc = func(c int) { code, called = c, true }

	ExitPolicy("g", "boom", []byte("stack"))
	if !called || code != 1 {
		t.Fatalf("ExitPolicy should exit(1); called=%v code=%d", called, code)
	}
}

func TestRecoverPolicyDoesNotExit(t *testing.T) {
	restorePolicy(t)
	orig := exitFunc
	t.Cleanup(func() { exitFunc = orig })
	exitFunc = func(int) { t.Fatal("RecoverPolicy must not exit") }

	RecoverPolicy("g", "boom", []byte("stack")) // must simply return
}
