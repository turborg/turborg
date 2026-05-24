package main

import (
	"testing"
	"time"
)

// TestLoadgenSmoke exercises the M8 harness end to end at a small scale: a
// handful of synthetic tenants must all register against the in-process fake
// IRC and the run must complete cleanly (boot → register → sample → drain).
// The headline 10k numbers come from running the binary on real hardware.
func TestLoadgenSmoke(t *testing.T) {
	if err := run(25, 1, 20*time.Second, 200*time.Millisecond); err != nil {
		t.Fatalf("loadgen run failed: %v", err)
	}
}

func TestBuildSpecsShape(t *testing.T) {
	specs := buildSpecs(3, 2, "127.0.0.1:6667")
	if len(specs) != 3 {
		t.Fatalf("want 3 specs, got %d", len(specs))
	}
	cfg := specs[0].Connectors[0].Config
	if cfg["network"] != "127.0.0.1:6667" {
		t.Fatalf("network not set: %v", cfg["network"])
	}
	chans, ok := cfg["channels"].([]any)
	if !ok || len(chans) != 2 {
		t.Fatalf("want 2 channels, got %v", cfg["channels"])
	}
}
