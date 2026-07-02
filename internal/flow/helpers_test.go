package flow

import (
	"context"
	"iter"
	"sync"
	"testing"

	"github.com/turborg/turborg/internal/agent"
	"github.com/turborg/turborg/internal/llm"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

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

func (f *fakeActor) Say(c, t string) error     { f.rec("say " + c + " " + t); return nil }
func (f *fakeActor) Notice(t, x string) error  { f.rec("notice " + t + " " + x); return nil }
func (f *fakeActor) Kick(c, n, r string) error { f.rec("kick " + c + " " + n + " " + r); return nil }
func (f *fakeActor) Ban(c, m string) error     { f.rec("ban " + c + " " + m); return nil }
func (f *fakeActor) Op(c, n string) error      { f.rec("op " + c + " " + n); return nil }
func (f *fakeActor) Voice(c, n string) error   { f.rec("voice " + c + " " + n); return nil }
func (f *fakeActor) Topic(c, t string) error   { f.rec("topic " + c + " " + t); return nil }
func (f *fakeActor) Invite(c, n string) error  { f.rec("invite " + c + " " + n); return nil }
func (f *fakeActor) SetMode(c, m string, a ...string) error {
	line := "mode " + c + " " + m
	for _, x := range a {
		line += " " + x
	}
	f.rec(line)
	return nil
}

type fakeProvider struct {
	mu     sync.Mutex
	resp   string
	err    error
	prompt string
}

func (p *fakeProvider) Model() string { return "fake" }
func (p *fakeProvider) Ask(_ context.Context, prompt string, _ ...llm.CallOption) (string, llm.Usage, error) {
	p.mu.Lock()
	p.prompt = prompt
	p.mu.Unlock()
	return p.resp, llm.Usage{}, p.err
}
func (p *fakeProvider) Stream(context.Context, string, ...llm.CallOption) iter.Seq2[string, error] {
	return func(func(string, error) bool) {}
}

func msgEvent(channel, sender, text string) *agent.Event {
	return &agent.Event{
		Type:   agent.EventMessage,
		Fields: map[string]any{"envelope": agent.NewInbound("irc", channel, sender, text)},
	}
}
