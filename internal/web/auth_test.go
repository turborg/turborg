package web_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/web"
)

func TestStaticPasswordVerifier(t *testing.T) {
	_, err := web.NewStaticPasswordVerifier("")
	require.Error(t, err, "empty password must be rejected at construction")

	v, err := web.NewStaticPasswordVerifier("hunter2")
	require.NoError(t, err)

	assert.True(t, v.Verify("hunter2"))
	assert.False(t, v.Verify(""))
	assert.False(t, v.Verify("hunter3"))
	assert.False(t, v.Verify("hunter2 "), "no leading/trailing whitespace tolerance")
}
