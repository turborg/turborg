package runtime

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/turborg/turborg/internal/agent"
)

func TestBuildLLMProviderKinds(t *testing.T) {
	// Anthropic path (default kind).
	_, _ = BuildLLMProvider("anthropic", "", "", "")
	_, _ = BuildLLMProvider("", "", "", "")

	// openai_compat with no key is the explicit "no provider" signal.
	p, err := BuildLLMProvider("openai_compat", "", "", "")
	require.NoError(t, err)
	require.Nil(t, p)

	// openai_compat with a key builds a provider.
	got, err := BuildLLMProvider("openai", "https://example/v1", "k", "m")
	require.NoError(t, err)
	require.NotNil(t, got)
}

func TestMatchesAllowlistAndRegistryGuard(t *testing.T) {
	a := agent.New(nil)
	ApplyRegistryGuard(a, GuardParams{}) // just exercise the wiring

	set := map[string]struct{}{"alice": {}, "acc": {}}
	assert.False(t, matchesAllowlist(nil, nil))
	assert.True(t, matchesAllowlist(set, &agent.InboundEnvelope{Sender: "Alice"}))
	assert.True(t, matchesAllowlist(set, &agent.InboundEnvelope{Metadata: map[string]any{"account": "Acc"}}))
	assert.False(t, matchesAllowlist(set, &agent.InboundEnvelope{Sender: ""}))
	assert.False(t, matchesAllowlist(set, &agent.InboundEnvelope{Sender: "bob"}))
}
