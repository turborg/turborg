package hive_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/hive"
)

func TestNoopClientImplementsInterface(t *testing.T) {
	var c hive.Client = hive.NoopClient{}
	ctx := context.Background()

	require.NoError(t, c.Connect(ctx))
	require.NoError(t, c.Heartbeat(ctx))
	require.NoError(t, c.Disconnect(ctx))
	assert.True(t, c.Connected(), "noop client must always report as connected")
}
