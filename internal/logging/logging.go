// Package logging configures the process-wide slog handler from settings.
//
// Two formats are supported: text (human-readable, the default) and json
// (one JSON object per line for ingestion by Loki/Datadog/CloudWatch
// Insights without regex parsing). Level names accept the familiar
// DEBUG/INFO/WARNING/ERROR/CRITICAL spelling (case-insensitive) and
// also the slog spelling WARN; both resolve to slog.LevelWarn.
package logging

import (
	"errors"
	"io"
	"log/slog"
	"os"
	"strings"
)

// New builds a *slog.Logger configured per level + format. Level names
// are case-insensitive; unknown levels return an error rather than
// silently defaulting.
func New(w io.Writer, level, format string) (*slog.Logger, error) {
	if w == nil {
		w = os.Stderr
	}
	lvl, err := parseLevel(level)
	if err != nil {
		return nil, err
	}
	opts := &slog.HandlerOptions{Level: lvl}

	var handler slog.Handler
	switch strings.ToLower(format) {
	case "", "text":
		handler = slog.NewTextHandler(w, opts)
	case "json":
		handler = slog.NewJSONHandler(w, opts)
	default:
		return nil, errors.New("logging: format must be 'text' or 'json'")
	}
	return slog.New(handler), nil
}

func parseLevel(level string) (slog.Level, error) {
	switch strings.ToUpper(level) {
	case "", "INFO":
		return slog.LevelInfo, nil
	case "DEBUG":
		return slog.LevelDebug, nil
	case "WARNING", "WARN":
		return slog.LevelWarn, nil
	case "ERROR":
		return slog.LevelError, nil
	case "CRITICAL":
		// slog tops out at ERROR — CRITICAL collapses to the same gate
		// so operators used to CRITICAL from other ecosystems still get
		// the strictest level rather than a parse error.
		return slog.LevelError, nil
	}
	return 0, errors.New("logging: unknown level " + level)
}
