package migrate

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/weefer/mongomigrate/internal/checkpoint"
	"github.com/weefer/mongomigrate/internal/engine"
	"github.com/weefer/mongomigrate/internal/namespace"
)

// checkpointer serializes and throttles writes to a checkpoint.Store. Its update
// methods are called from copier worker goroutines (recordProgress) and the cdc
// apply goroutine (recordToken), so all state is mutex-guarded. Intermediate
// saves are throttled; a final Flush (called from the orchestrator's defer with a
// fresh context) guarantees the last state is durable even if the run context was
// cancelled.
type checkpointer struct {
	store   checkpoint.Store
	log     *slog.Logger
	saveCtx context.Context // used for intermediate saves during the run

	minInterval time.Duration
	now         func() time.Time

	mu       sync.Mutex
	state    checkpoint.State
	lastSave time.Time
}

func newCheckpointer(ctx context.Context, store checkpoint.Store, st checkpoint.State, log *slog.Logger) *checkpointer {
	if st.Namespaces == nil {
		st.Namespaces = map[string]checkpoint.NSProgress{}
	}
	return &checkpointer{
		store:       store,
		log:         log,
		saveCtx:     ctx,
		minInterval: time.Second,
		now:         time.Now,
		state:       st,
	}
}

// recordProgress updates a namespace's snapshot progress. done forces an
// immediate save so a completed collection is never re-copied after a crash.
func (c *checkpointer) recordProgress(ns namespace.Namespace, lastID []byte, docsCopied int64, done bool) {
	c.mu.Lock()
	p := c.state.Namespaces[ns.String()]
	if len(lastID) > 0 {
		p.LastID = lastID
	}
	p.DocsCopied = docsCopied
	if done {
		p.Done = true
	}
	c.state.Namespaces[ns.String()] = p
	c.mu.Unlock()
	c.maybeSave(done)
}

// recordToken updates the last-applied CDC resume token. The tailer already
// throttles OnToken, so this always attempts a save (subject to this
// checkpointer's own throttle).
func (c *checkpointer) recordToken(tok engine.ResumeToken) {
	c.mu.Lock()
	c.state.ResumeToken = []byte(tok)
	c.mu.Unlock()
	c.maybeSave(false)
}

// recordPhase records the current lifecycle phase and saves immediately.
func (c *checkpointer) recordPhase(p engine.Phase) {
	c.mu.Lock()
	c.state.Phase = string(p)
	c.mu.Unlock()
	c.maybeSave(true)
}

// markNonResumable flags the run as unable to resume (e.g. the change-stream
// history was lost) and saves immediately.
func (c *checkpointer) markNonResumable() {
	c.mu.Lock()
	c.state.NonResumable = true
	c.mu.Unlock()
	c.maybeSave(true)
}

// maybeSave persists the state, throttled to minInterval unless force is set.
func (c *checkpointer) maybeSave(force bool) {
	c.mu.Lock()
	if !force && c.now().Sub(c.lastSave) < c.minInterval {
		c.mu.Unlock()
		return
	}
	c.lastSave = c.now()
	c.state.UpdatedAt = c.now()
	s := c.state.Clone()
	c.mu.Unlock()
	c.save(c.saveCtx, s)
}

// Flush unconditionally persists the current state using the given context. It is
// called from the orchestrator's defer with context.Background() so the final
// checkpoint survives even a cancelled run context.
func (c *checkpointer) Flush(ctx context.Context) {
	c.mu.Lock()
	c.state.UpdatedAt = c.now()
	s := c.state.Clone()
	c.lastSave = c.now()
	c.mu.Unlock()
	c.save(ctx, s)
}

func (c *checkpointer) save(ctx context.Context, s checkpoint.State) {
	if err := c.store.Save(ctx, s); err != nil {
		c.log.Warn("checkpoint save failed", "runId", s.RunID, "err", err)
	}
}
