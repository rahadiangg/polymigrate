package logx

import (
	"bytes"
	"log/slog"
	"strings"
	"testing"
)

func TestParseLevel(t *testing.T) {
	cases := map[string]slog.Level{
		"debug": slog.LevelDebug,
		"DEBUG": slog.LevelDebug,
		"info":  slog.LevelInfo,
		"":      slog.LevelInfo,
		"warn":  slog.LevelWarn,
		"warning": slog.LevelWarn,
		"error": slog.LevelError,
		"bogus": slog.LevelInfo,
	}
	for in, want := range cases {
		if got := ParseLevel(in); got != want {
			t.Errorf("ParseLevel(%q) = %v want %v", in, got, want)
		}
	}
}

func TestNewTextAndLevel(t *testing.T) {
	var buf bytes.Buffer
	log := New(Options{Level: "warn", Writer: &buf})
	log.Info("suppressed")
	log.Warn("shown", "k", "v")
	out := buf.String()
	if strings.Contains(out, "suppressed") {
		t.Error("info should be filtered at warn level")
	}
	if !strings.Contains(out, "shown") || !strings.Contains(out, "k=v") {
		t.Errorf("warn line missing: %q", out)
	}
}

func TestNewJSON(t *testing.T) {
	var buf bytes.Buffer
	log := New(Options{Level: "info", JSON: true, Writer: &buf})
	log.Info("hello", "n", 1)
	out := buf.String()
	if !strings.HasPrefix(strings.TrimSpace(out), "{") || !strings.Contains(out, `"msg":"hello"`) {
		t.Errorf("expected JSON log, got %q", out)
	}
}

func TestDiscard(t *testing.T) {
	// Should not panic and should produce no output side effects.
	Discard().Info("nothing", "x", 1)
}
