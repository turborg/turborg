package llm_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/turborg/turborg/internal/llm"
)

func TestApplyOptionsDefaults(t *testing.T) {
	got := llm.ApplyOptions(nil)
	assert.Equal(t, llm.CallOptions{}, got)
}

func TestApplyOptionsAppliesEachField(t *testing.T) {
	got := llm.ApplyOptions([]llm.CallOption{
		llm.WithSystem("you are friendly"),
		llm.WithMaxTokens(123),
		llm.WithModel("claude-opus-4-7"),
	})
	assert.Equal(t, "you are friendly", got.System)
	assert.Equal(t, 123, got.MaxTokens)
	assert.Equal(t, "claude-opus-4-7", got.Model)
}

func TestApplyOptionsLaterWins(t *testing.T) {
	got := llm.ApplyOptions([]llm.CallOption{
		llm.WithModel("first"),
		llm.WithModel("second"),
	})
	assert.Equal(t, "second", got.Model, "later option must override the earlier one")
}
