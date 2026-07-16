// Package logx provides a thin structured-logging helper built on log/slog so
// every component logs consistently. It supports text and JSON handlers and a
// verbosity switch.
package logx

import (
	"io"
	"log/slog"
	"os"
	"strings"
)

// Options configures a logger.
type Options struct {
	// Level is one of "debug", "info", "warn", "error" (case-insensitive).
	Level string
	// JSON selects the JSON handler instead of the human-readable text handler.
	JSON bool
	// Writer is where logs go; defaults to os.Stderr when nil.
	Writer io.Writer
}

// New builds a *slog.Logger from Options.
func New(opts Options) *slog.Logger {
	w := opts.Writer
	if w == nil {
		w = os.Stderr
	}
	h := &slog.HandlerOptions{Level: ParseLevel(opts.Level)}
	if opts.JSON {
		return slog.New(slog.NewJSONHandler(w, h))
	}
	return slog.New(slog.NewTextHandler(w, h))
}

// ParseLevel maps a level string to a slog.Level, defaulting to Info.
func ParseLevel(s string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// Discard returns a logger that drops everything, for tests.
func Discard() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}
