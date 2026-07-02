package skill

import (
	"context"
	"iter"
	"sync"
	"testing"

	"github.com/turborg/turborg/internal/llm"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

// fakeActor records every action call so tests can assert on the wire-agnostic
// surface the engine drives.
type fakeActor struct {
	mu    sync.Mutex
	calls []string
}

func (f *fakeActor) rec(s string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, s)
}

func (f *fakeActor) snapshot() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.calls...)
}

func (f *fakeActor) Say(channel, text string) error { f.rec("say " + channel + " " + text); return nil }
func (f *fakeActor) Notice(target, text string) error {
	f.rec("notice " + target + " " + text)
	return nil
}
func (f *fakeActor) Kick(c, n, r string) error { f.rec("kick " + c + " " + n + " " + r); return nil }
func (f *fakeActor) Ban(c, m string) error     { f.rec("ban " + c + " " + m); return nil }
func (f *fakeActor) Op(c, n string) error      { f.rec("op " + c + " " + n); return nil }
func (f *fakeActor) Voice(c, n string) error   { f.rec("voice " + c + " " + n); return nil }
func (f *fakeActor) Topic(c, t string) error   { f.rec("topic " + c + " " + t); return nil }
func (f *fakeActor) Invite(c, n string) error  { f.rec("invite " + c + " " + n); return nil }
func (f *fakeActor) SetMode(c, modes string, a ...string) error {
	line := "mode " + c + " " + modes
	for _, x := range a {
		line += " " + x
	}
	f.rec(line)
	return nil
}

// fakeProvider returns a fixed response (or error) and records the last prompt.
type fakeProvider struct {
	mu     sync.Mutex
	resp   string
	err    error
	prompt string
	system string
}

func (p *fakeProvider) Model() string { return "fake" }
func (p *fakeProvider) Ask(_ context.Context, prompt string, opts ...llm.CallOption) (string, llm.Usage, error) {
	co := llm.ApplyOptions(opts)
	p.mu.Lock()
	p.prompt = prompt
	p.system = co.System
	p.mu.Unlock()
	return p.resp, llm.Usage{}, p.err
}
func (p *fakeProvider) Stream(context.Context, string, ...llm.CallOption) iter.Seq2[string, error] {
	return func(func(string, error) bool) {}
}

func (p *fakeProvider) lastPrompt() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.prompt
}
