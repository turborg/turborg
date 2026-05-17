package statepush

import (
	"testing"
	"time"
)

// OverrideBackoffsForTesting swaps the package-level retry-backoff
// schedule for the duration of a test. Returns a restore func; pair
// it with a `defer restore()` so the production schedule comes back
// before the next test runs.
//
// Lives in this file (only built under `go test`) so production
// builds can't reach the override. Exported because external test
// packages (statepush_test) need to call it across the package
// boundary.
func OverrideBackoffsForTesting(t *testing.T, replacement []time.Duration) func() {
	t.Helper()
	original := retryBackoffs
	retryBackoffs = replacement
	return func() { retryBackoffs = original }
}
