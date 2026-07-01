package skill

import (
	"context"
	"slices"
	"testing"
	"time"
)

// applyTimed is a small helper that runs a resolved effect with a duration.
func applyTimed(e *Engine, act EffectAction, modes string, secs int) {
	eff := &Effect{Action: act, Modes: modes, DurationSeconds: secs}
	e.applyEffect(context.Background(), Skill{Name: "guard"}, renderFields{channel: "#c", user: "spammer"}, eff, act)
}

func TestTimedEffectAppliesThenAutoLifts(t *testing.T) {
	fa := &fakeActor{}
	e := NewEngine(Options{Actor: fa, Store: NewMemoryStore()})

	applyTimed(e, EffectMute, "", 60)
	if got := fa.snapshot(); !slices.Contains(got, "mode #c +q spammer") {
		t.Fatalf("expected +q applied, got %v", got)
	}

	// Before the deadline: no reversal.
	e.SweepLifts(time.Now())
	if slices.Contains(fa.snapshot(), "mode #c -q spammer") {
		t.Fatal("lift fired before its deadline")
	}

	// After the deadline: the mute auto-lifts.
	e.SweepLifts(time.Now().Add(61 * time.Second))
	if got := fa.snapshot(); !slices.Contains(got, "mode #c -q spammer") {
		t.Fatalf("expected -q lift after deadline, got %v", got)
	}

	// Idempotent: a second sweep does not re-issue the reversal.
	before := len(fa.snapshot())
	e.SweepLifts(time.Now().Add(120 * time.Second))
	if len(fa.snapshot()) != before {
		t.Fatal("lift re-fired after it was already applied")
	}
}

func TestTimedEffectSurvivesRestartViaStore(t *testing.T) {
	store := NewMemoryStore()

	// Engine A applies a timed +m channel lockdown, then goes away.
	a := NewEngine(Options{Actor: &fakeActor{}, Store: store})
	applyTimed(a, EffectMode, "+m", 60)

	// Engine B shares the store (a fresh process/tenant restart) and must
	// resume the pending lift.
	fb := &fakeActor{}
	b := NewEngine(Options{Actor: fb, Store: store})
	b.SweepLifts(time.Now().Add(61 * time.Second))
	if got := fb.snapshot(); !slices.Contains(got, "mode #c -m") {
		t.Fatalf("restart engine did not resume the lift, got %v", got)
	}
}

func TestNonReversibleEffectSchedulesNoLift(t *testing.T) {
	fa := &fakeActor{}
	e := NewEngine(Options{Actor: fa, Store: NewMemoryStore()})
	applyTimed(e, EffectKick, "", 60)
	e.SweepLifts(time.Now().Add(120 * time.Second))
	for _, c := range fa.snapshot() {
		if len(c) >= 4 && c[:4] == "mode" {
			t.Fatalf("kick must not schedule a mode lift, got %v", fa.snapshot())
		}
	}
}

func TestReverseEffect(t *testing.T) {
	cases := []struct {
		act      EffectAction
		modes    string
		wantMode string
		wantArg  string
		wantOK   bool
	}{
		{EffectMute, "", "-q", "spammer", true},
		{EffectBan, "", "-b", "spammer", true},
		{EffectOp, "", "-o", "spammer", true},
		{EffectVoice, "", "-v", "spammer", true},
		{EffectMode, "+m", "-m", "", true},
		{EffectMode, "-i", "+i", "", true},
		{EffectMode, "", "", "", false},
		{EffectKick, "", "", "", false},
		{EffectNotice, "", "", "", false},
	}
	for _, c := range cases {
		m, arg, ok := reverseEffect(c.act, "spammer", c.modes)
		if m != c.wantMode || arg != c.wantArg || ok != c.wantOK {
			t.Errorf("reverseEffect(%q,%q) = (%q,%q,%v), want (%q,%q,%v)",
				c.act, c.modes, m, arg, ok, c.wantMode, c.wantArg, c.wantOK)
		}
	}
}

func TestFlipModeSign(t *testing.T) {
	for in, want := range map[string]string{"+m": "-m", "-m": "+m", "m": "-m", "": ""} {
		if got := flipModeSign(in); got != want {
			t.Errorf("flipModeSign(%q) = %q, want %q", in, got, want)
		}
	}
}
