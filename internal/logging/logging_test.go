package logging_test

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/turborg/turborg/internal/logging"
)

func TestNewTextHandler(t *testing.T) {
	var buf bytes.Buffer
	log, err := logging.New(&buf, "DEBUG", "text")
	require.NoError(t, err)
	log.Debug("hello")
	out := buf.String()
	assert.Contains(t, out, "level=DEBUG")
	assert.Contains(t, out, "msg=hello")
}

func TestNewJSONHandler(t *testing.T) {
	var buf bytes.Buffer
	log, err := logging.New(&buf, "INFO", "json")
	require.NoError(t, err)
	log.Info("structured", "k", "v")
	var got map[string]any
	require.NoError(t, json.Unmarshal(bytes.TrimSpace(buf.Bytes()), &got))
	assert.Equal(t, "structured", got["msg"])
	assert.Equal(t, "v", got["k"])
}

func TestNewAcceptsAllLevelAliases(t *testing.T) {
	for _, lvl := range []string{"DEBUG", "INFO", "WARNING", "WARN", "ERROR", "CRITICAL", ""} {
		_, err := logging.New(nil, lvl, "text")
		assert.NoError(t, err, "level %q must be accepted", lvl)
	}
}

func TestNewRejectsUnknownLevel(t *testing.T) {
	_, err := logging.New(nil, "TRACE", "text")
	require.Error(t, err)
	assert.True(t, strings.Contains(err.Error(), "unknown level"))
}

func TestNewRejectsUnknownFormat(t *testing.T) {
	_, err := logging.New(nil, "INFO", "yaml")
	require.Error(t, err)
}

func TestNewDefaultsToTextOnEmptyFormat(t *testing.T) {
	var buf bytes.Buffer
	log, err := logging.New(&buf, "INFO", "")
	require.NoError(t, err)
	log.Info("ok")
	assert.Contains(t, buf.String(), "msg=ok")
}
