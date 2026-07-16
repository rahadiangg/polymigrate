// Package progress provides engine.Reporter implementations: a concurrency-safe
// logger-backed reporter for the CLI and a counting reporter for tests.
package progress

import (
	"log/slog"
	"sync"
	"sync/atomic"

	"github.com/weefer/mongomigrate/internal/engine"
	"github.com/weefer/mongomigrate/internal/namespace"
)

// LogReporter reports progress through an *slog.Logger. It is safe for
// concurrent use by multiple worker goroutines.
type LogReporter struct {
	log *slog.Logger

	mu      sync.Mutex
	lastPct map[string]int // ns -> last logged percentage, to throttle output
}

// NewLogReporter builds a LogReporter. A nil logger is replaced with a discarding one.
func NewLogReporter(log *slog.Logger) *LogReporter {
	if log == nil {
		log = slog.Default()
	}
	return &LogReporter{log: log, lastPct: make(map[string]int)}
}

func (r *LogReporter) Phase(p engine.Phase) {
	r.log.Info("phase", "phase", string(p))
}

func (r *LogReporter) Event(msg string, kv ...any) {
	r.log.Info(msg, kv...)
}

// Progress logs at most once per whole-percent change per namespace to avoid
// flooding on large collections. total <= 0 logs the raw done count.
func (r *LogReporter) Progress(ns namespace.Namespace, done, total int64) {
	key := ns.String()
	if total <= 0 {
		r.log.Debug("progress", "ns", key, "done", done)
		return
	}
	pct := int(done * 100 / total)
	r.mu.Lock()
	last, ok := r.lastPct[key]
	if !ok || pct != last {
		r.lastPct[key] = pct
		r.mu.Unlock()
		r.log.Info("progress", "ns", key, "done", done, "total", total, "pct", pct)
		return
	}
	r.mu.Unlock()
}

// CountingReporter records events for assertions in tests. Safe for concurrent use.
type CountingReporter struct {
	Phases    atomic.Int64
	Events    atomic.Int64
	ProgressN atomic.Int64

	mu        sync.Mutex
	LastPhase engine.Phase
	PhaseList []engine.Phase
}

func NewCountingReporter() *CountingReporter { return &CountingReporter{} }

func (c *CountingReporter) Phase(p engine.Phase) {
	c.Phases.Add(1)
	c.mu.Lock()
	c.LastPhase = p
	c.PhaseList = append(c.PhaseList, p)
	c.mu.Unlock()
}

func (c *CountingReporter) Event(string, ...any) { c.Events.Add(1) }

func (c *CountingReporter) Progress(namespace.Namespace, int64, int64) { c.ProgressN.Add(1) }

// Phases snapshot returns a copy of the recorded phase order.
func (c *CountingReporter) PhaseOrder() []engine.Phase {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]engine.Phase, len(c.PhaseList))
	copy(out, c.PhaseList)
	return out
}

var _ engine.Reporter = (*LogReporter)(nil)
var _ engine.Reporter = (*CountingReporter)(nil)
