package progress

import (
	"bytes"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/weefer/mongomigrate/internal/engine"
	"github.com/weefer/mongomigrate/internal/namespace"
)

func TestLogReporterThrottlesProgress(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelInfo}))
	r := NewLogReporter(log)
	ns := namespace.Namespace{DB: "d", Collection: "c"}
	// Many calls at the same percentage should log once.
	for range 100 {
		r.Progress(ns, 5, 1000) // 0%
	}
	if n := strings.Count(buf.String(), "progress"); n != 1 {
		t.Errorf("expected 1 progress line at constant pct, got %d", n)
	}
	r.Progress(ns, 500, 1000) // 50% -> new line
	if n := strings.Count(buf.String(), "progress"); n != 2 {
		t.Errorf("expected 2 progress lines after pct change, got %d", n)
	}
}

func TestLogReporterUnknownTotal(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	r := NewLogReporter(log)
	r.Progress(namespace.Namespace{DB: "d", Collection: "c"}, 7, -1)
	if !strings.Contains(buf.String(), "done=7") {
		t.Errorf("expected raw done count, got %q", buf.String())
	}
}

func TestCountingReporter(t *testing.T) {
	c := NewCountingReporter()
	c.Phase(engine.PhaseInit)
	c.Phase(engine.PhaseSnapshot)
	c.Event("x")
	c.Event("y")
	c.Progress(namespace.Namespace{}, 1, 2)
	if c.Phases.Load() != 2 || c.Events.Load() != 2 || c.ProgressN.Load() != 1 {
		t.Errorf("counts wrong: phases=%d events=%d progress=%d", c.Phases.Load(), c.Events.Load(), c.ProgressN.Load())
	}
	order := c.PhaseOrder()
	if len(order) != 2 || order[0] != engine.PhaseInit || order[1] != engine.PhaseSnapshot {
		t.Errorf("phase order wrong: %v", order)
	}
	if c.LastPhase != engine.PhaseSnapshot {
		t.Errorf("last phase = %v", c.LastPhase)
	}
}

func TestCountingReporterConcurrent(t *testing.T) {
	c := NewCountingReporter()
	var wg sync.WaitGroup
	for range 50 {
		wg.Go(func() {
			c.Event("e")
			c.Progress(namespace.Namespace{}, 1, 1)
			c.Phase(engine.PhaseApply)
		})
	}
	wg.Wait()
	if c.Events.Load() != 50 || c.ProgressN.Load() != 50 || c.Phases.Load() != 50 {
		t.Errorf("concurrent counts wrong: %d %d %d", c.Events.Load(), c.ProgressN.Load(), c.Phases.Load())
	}
}

func TestNilLoggerSafe(t *testing.T) {
	r := NewLogReporter(nil) // must not panic
	r.Phase(engine.PhaseDone)
	r.Event("ok")
}
