// Package cdc implements the change-stream tailer (change data capture) used for
// near-zero-downtime cutover. It watches the source deployment's change stream,
// buffers events in a bounded in-memory queue, and applies them idempotently to
// the destination.
//
// The design deliberately decouples reading from applying: a single reader
// goroutine pulls events from the change stream as fast as the buffer allows and
// records the latest resume token, while Apply drains the buffer in batches on
// the caller's goroutine. Keeping the reader off the (potentially slow) apply
// path is what prevents the server from expiring the change stream's oplog
// history (error 286, ChangeStreamHistoryLost).
//
// Every event is applied with last-writer-wins semantics using the change
// event's post-image (fullDocument = updateLookup): inserts, replaces and
// updates become an upserting ReplaceOne keyed by _id, and deletes become a
// DeleteOne. Because each apply overwrites the destination document with the
// latest source image, re-applying the same events (after a crash or a resume)
// converges to the same state, so Apply is safe to retry.
package cdc

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/weefer/mongomigrate/internal/engine"
	"github.com/weefer/mongomigrate/internal/mongox"
	"github.com/weefer/mongomigrate/internal/namespace"
)

// Defaults applied when TailOptions leaves a sizing field at its zero value.
const (
	defaultBufferSize     = 4096
	defaultApplyBatchSize = 500
)

// Change-stream operationType values.
const (
	opInsert       = "insert"
	opUpdate       = "update"
	opReplace      = "replace"
	opDelete       = "delete"
	opDrop         = "drop"
	opRename       = "rename"
	opDropDatabase = "dropDatabase"
	opInvalidate   = "invalidate"
)

// codeDuplicateKey is the MongoDB server error code for a duplicate key. A
// duplicate key during an idempotent replay is benign and tolerated.
const codeDuplicateKey = 11000

// ErrChangeStreamHistoryLost signals that the source oplog no longer retains the
// change-stream history needed to resume (server error 286, ChangeStreamHistoryLost).
// It is fatal and non-resumable: the run cannot continue from its checkpoint token,
// so the operator must start a fresh snapshot. Apply (and Open) wrap this sentinel
// so callers can detect the condition with errors.Is and mark the run non-resumable.
// It is an alias of engine.ErrChangeStreamHistoryLost so the driver-free
// orchestrator can detect it without importing this package.
var ErrChangeStreamHistoryLost = engine.ErrChangeStreamHistoryLost

// ChangeEvent is the in-package, driver-independent representation of a single
// change-stream event. Keeping the tailer's logic in terms of this type (rather
// than raw BSON or driver structs) is what lets the orchestration be unit-tested
// with in-memory fakes.
type ChangeEvent struct {
	// OpType is the change stream operationType (insert/update/replace/delete/
	// drop/rename/dropDatabase/invalidate/...).
	OpType string
	// NS is the affected namespace (may be zero for deployment-level events such
	// as invalidate).
	NS namespace.Namespace
	// ID is the documentKey._id of the affected document, as a raw BSON value so
	// any _id type round-trips unchanged. Zero for non-document events.
	ID bson.RawValue
	// Full is the fullDocument post-image. Present for insert/replace and, with
	// FullDocument=updateLookup, for update — unless the document was deleted
	// between the change and the lookup. Empty for delete and DDL events.
	Full bson.Raw
	// At is the wall-clock approximation of the event's cluster time.
	At time.Time

	// Token is the raw BSON of the change event document's own _id, which IS this
	// event's change-stream resume token. Checkpointing the token of the last
	// APPLIED event lets a crashed run resume from exactly the first unapplied
	// event, never skipping data. It aliases the parsed event's backing bytes (like
	// ID and Full) and is empty when the source surfaced no token (e.g. synthetic
	// events in tests).
	Token []byte

	// capturedAt is the wall-clock time the reader buffered the event. It is used
	// only for lag estimation and is never serialized.
	capturedAt time.Time
}

// eventSource abstracts the change stream so the tailer's loop can be tested
// with a scripted in-memory fake. The real implementation wraps
// *mongo.ChangeStream (see changeStreamSource).
type eventSource interface {
	// Next blocks for the next event. It returns (event, true, nil) on success,
	// (zero, false, nil) when the stream is closed by the server, and
	// (zero, false, err) on error (including ctx cancellation).
	Next(ctx context.Context) (ChangeEvent, bool, error)
	// ResumeToken returns the latest resume token observed by the stream.
	ResumeToken() []byte
	// Close releases the underlying stream.
	Close(ctx context.Context) error
}

// applier abstracts the destination write path so the tailer can be tested with
// an in-memory sink. The real implementation issues unordered BulkWrites (see
// bulkApplier). Implementations must be idempotent with respect to replays.
type applier interface {
	// apply writes a deduplicated batch of data events to the destination and
	// returns the number of applied upserts (insert/replace/update) and deletes.
	apply(ctx context.Context, batch []ChangeEvent) (inserts, deletes int, err error)
}

// Tailer captures change-stream events from the source and applies them
// idempotently to the destination. It satisfies engine.Tailer.
type Tailer struct {
	src      *mongox.Client
	dst      *mongox.Client
	reporter engine.Reporter
	log      *slog.Logger

	// Configured by Open.
	allowed        []namespace.Namespace
	applyBatchSize int
	source         eventSource
	// sourceFactory builds the event source; nil selects the real driver adapter
	// (openChangeStream). Tests inject a factory to observe the resolved options
	// (e.g. that TailOptions.Resume is passed through) and return a scripted source.
	sourceFactory func(ctx context.Context, src *mongox.Client, opts engine.TailOptions) (eventSource, error)
	applier       applier
	buffer        chan ChangeEvent

	streamCtx    context.Context
	streamCancel context.CancelFunc
	stopOnce     sync.Once
	readerDone   chan struct{}
	opened       bool

	// Applied-token checkpointing. onToken and now are set in Open and only read
	// thereafter; appliedToken and lastTokenEmitAt are touched solely by the apply
	// goroutine (applyBatch and Apply's deferred flush), so they need no locking.
	onToken         func(engine.ResumeToken)
	now             func() time.Time
	appliedToken    []byte
	lastTokenEmitAt time.Time

	// mu guards all mutable shared state below.
	mu              sync.Mutex
	captured        int64
	consumed        int64
	applied         int64
	inserts         int64
	deletes         int64
	lastEventAt     time.Time
	lastAppliedAt   time.Time
	oldestPendingAt time.Time
	token           []byte
	readErr         error
	closed          bool
}

// New builds a Tailer that captures from src and applies to dst. A nil reporter
// or logger is replaced with a no-op so callers need not supply them.
func New(src, dst *mongox.Client, reporter engine.Reporter, log *slog.Logger) *Tailer {
	if reporter == nil {
		reporter = engine.NopReporter{}
	}
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Tailer{src: src, dst: dst, reporter: reporter, log: log, now: time.Now}
}

// Open opens the change stream and starts the background reader goroutine that
// buffers events. It must be called exactly once before Apply. The context
// governs the lifetime of the stream: cancelling it (like Stop or Close) winds
// the reader down.
func (t *Tailer) Open(ctx context.Context, opts engine.TailOptions) error {
	if t.opened {
		return errors.New("cdc: tailer already opened")
	}

	bufSize := opts.BufferSize
	if bufSize <= 0 {
		bufSize = defaultBufferSize
	}
	t.applyBatchSize = opts.ApplyBatchSize
	if t.applyBatchSize <= 0 {
		t.applyBatchSize = defaultApplyBatchSize
	}
	t.allowed = append([]namespace.Namespace(nil), opts.Namespaces...)
	t.buffer = make(chan ChangeEvent, bufSize)
	t.readerDone = make(chan struct{})
	t.streamCtx, t.streamCancel = context.WithCancel(ctx)

	// The applied-token checkpoint callback and clock are read only by the apply
	// goroutine, which starts strictly after Open returns.
	t.onToken = opts.OnToken
	if t.now == nil {
		t.now = time.Now
	}

	// Seed the resume token so Token() returns the caller-provided token before
	// the first event advances it.
	if !opts.Resume.IsZero() {
		t.token = cloneBytes(opts.Resume)
	}

	// The applier and source are pre-set by tests; only build the real driver
	// adapters when they are absent.
	if t.applier == nil {
		t.applier = newBulkApplier(t.dst)
	}
	if t.source == nil {
		factory := t.sourceFactory
		if factory == nil {
			factory = openChangeStream
		}
		src, err := factory(ctx, t.src, opts)
		if err != nil {
			t.streamCancel()
			if isChangeStreamHistoryLost(err) {
				return fmt.Errorf("cdc: open change stream: %w: %w", ErrChangeStreamHistoryLost, err)
			}
			return fmt.Errorf("cdc: open change stream: %w", err)
		}
		t.source = src
	}

	t.opened = true
	t.reporter.Phase(engine.PhaseTailOpen)
	go t.readLoop()
	return nil
}

// readLoop continuously reads events from the source, filters by namespace,
// records the latest resume token, and pushes matching events into the bounded
// buffer. It never performs destination writes, so it keeps draining the change
// stream even while Apply is slow. It exits when the stream ends, an error
// occurs, or the stream context is cancelled, and it always closes the buffer
// (so Apply can finish) and readerDone (so Close can join) on the way out.
func (t *Tailer) readLoop() {
	defer close(t.readerDone)
	defer close(t.buffer)

	for {
		ev, ok, err := t.source.Next(t.streamCtx)
		if err != nil {
			if t.streamCtx.Err() != nil {
				return // graceful: Stop, Close, or parent cancellation.
			}
			// A lost change-stream history (286) is unrecoverable: wrap the sentinel
			// so Apply surfaces it and the orchestrator can mark the run non-resumable.
			if isChangeStreamHistoryLost(err) {
				t.setReadErr(fmt.Errorf("cdc: change stream read: %w: %w", ErrChangeStreamHistoryLost, err))
				return
			}
			t.setReadErr(fmt.Errorf("cdc: change stream read: %w", err))
			return
		}
		if !ok {
			return // stream closed by the server (e.g. after invalidate).
		}

		tok := t.source.ResumeToken()
		if !matchNS(ev, t.allowed) {
			// Advance the token even for filtered-out events so a resume does not
			// re-deliver them.
			t.mu.Lock()
			if len(tok) > 0 {
				t.token = cloneBytes(tok)
			}
			t.mu.Unlock()
			continue
		}

		ev.capturedAt = time.Now()
		// Advance captured/lastEventAt/token ONLY once the event is safely buffered.
		// If Stop cancels the stream while this event is in flight, we drop it
		// without advancing the resume token past it, so a resumed stream
		// re-delivers it rather than silently skipping it.
		select {
		case t.buffer <- ev:
			t.mu.Lock()
			t.captured++
			if !ev.At.IsZero() {
				t.lastEventAt = ev.At
			}
			if len(tok) > 0 {
				t.token = cloneBytes(tok)
			}
			t.mu.Unlock()
		case <-t.streamCtx.Done():
			return
		}
	}
}

// Apply drains buffered events and applies them to the destination in batches.
// It blocks until Stop (or Close) has been called and the buffer is fully
// drained, or until ctx is cancelled. It returns nil on a clean, fully-drained
// shutdown, the change-stream read error if the reader failed, or ctx.Err() on
// cancellation.
func (t *Tailer) Apply(ctx context.Context) error {
	if !t.opened {
		return errors.New("cdc: Apply called before Open")
	}
	t.reporter.Phase(engine.PhaseApply)
	// Always checkpoint the final applied position when Apply stops (drain, error,
	// or cancellation), even if the throttle suppressed its last periodic emit.
	defer t.flushToken()

	for {
		batch, closed, err := t.nextBatch(ctx)
		if len(batch) > 0 {
			if aerr := t.applyBatch(ctx, batch); aerr != nil {
				return aerr
			}
		}
		if err != nil {
			return err
		}
		if closed {
			return t.readErrOrNil()
		}
	}
}

// nextBatch blocks for the first buffered event then greedily collects up to
// applyBatchSize events without blocking. closed is true once the buffer has
// been closed and fully drained. err is non-nil only when ctx is cancelled
// while waiting for the first event.
func (t *Tailer) nextBatch(ctx context.Context) (batch []ChangeEvent, closed bool, err error) {
	select {
	case ev, ok := <-t.buffer:
		if !ok {
			return nil, true, nil
		}
		batch = append(batch, ev)
		t.mu.Lock()
		t.oldestPendingAt = ev.capturedAt
		t.mu.Unlock()
	case <-ctx.Done():
		return nil, false, ctx.Err()
	}

	for len(batch) < t.applyBatchSize {
		select {
		case ev, ok := <-t.buffer:
			if !ok {
				return batch, true, nil
			}
			batch = append(batch, ev)
		default:
			return batch, false, nil
		}
	}
	return batch, false, nil
}

// applyBatch handles the control (DDL) events in a batch explicitly, deduplicates
// the data events, applies them idempotently, and updates the stats.
func (t *Tailer) applyBatch(ctx context.Context, batch []ChangeEvent) error {
	var data []ChangeEvent
	invalidate := false
	for _, e := range batch {
		switch e.OpType {
		case opInsert, opUpdate, opReplace, opDelete:
			data = append(data, e)
		case opInvalidate:
			invalidate = true
			t.log.Warn("cdc: change stream invalidated; stopping tailer", "ns", e.NS.String())
			t.reporter.Event("cdc invalidate", "ns", e.NS.String())
		case opDrop, opRename, opDropDatabase:
			// DDL is not replayable through the data path (and createIndexes-style
			// DDL is not even emitted on MongoDB 4.4); log and no-op.
			t.log.Warn("cdc: unsupported DDL change event; not replayed", "op", e.OpType, "ns", e.NS.String())
			t.reporter.Event("cdc ddl skipped", "op", e.OpType, "ns", e.NS.String())
		default:
			t.log.Warn("cdc: unknown change event; skipped", "op", e.OpType, "ns", e.NS.String())
		}
	}

	data = dedupeBatch(data)
	var ins, del int
	if len(data) > 0 {
		var err error
		ins, del, err = t.applier.apply(ctx, data)
		if err != nil {
			return fmt.Errorf("cdc: apply batch: %w", err)
		}
	}

	now := time.Now()
	t.mu.Lock()
	t.consumed += int64(len(batch))
	t.applied += int64(ins + del)
	t.inserts += int64(ins)
	t.deletes += int64(del)
	if ins+del > 0 {
		t.lastAppliedAt = now
	}
	t.oldestPendingAt = time.Time{}
	t.mu.Unlock()

	// Checkpoint the APPLIED resume position: the token of the newest event in this
	// batch. Every earlier event was applied (data) or handled (DDL/invalidate)
	// above, and this runs after the destination write succeeds, so the token never
	// runs ahead of applied data — a resume from it re-reads only unapplied events.
	t.recordAppliedToken(lastToken(batch))

	if invalidate {
		t.Stop()
	}
	return nil
}

// computeLag estimates apply lag from the backlog. It MUST return a strictly
// positive duration whenever buffered>0, because the orchestrator treats Lag<=0
// as "fully drained" and cuts over: a spurious zero while events remain buffered
// would let the cutover stop the reader with events still unapplied and silently
// lose data. oldestPendingAt is only set while a batch's head is in flight and is
// reset to zero between batches, so this falls back to lastEventAt and finally to
// a 1ns sentinel to preserve the invariant. It is zero only when buffered==0.
func computeLag(buffered int64, oldestPendingAt, lastEventAt, now time.Time) time.Duration {
	if buffered <= 0 {
		return 0
	}
	switch {
	case !oldestPendingAt.IsZero():
		return positive(now.Sub(oldestPendingAt))
	case !lastEventAt.IsZero():
		return positive(now.Sub(lastEventAt))
	default:
		return time.Nanosecond
	}
}

// positive clamps a duration to a strictly positive value, so a backlog never
// reports Lag<=0 even if a clock read went backwards.
func positive(d time.Duration) time.Duration {
	if d <= 0 {
		return time.Nanosecond
	}
	return d
}

// Stats returns a consistent snapshot of tailer progress. It is safe to call
// concurrently with the running tailer.
func (t *Tailer) Stats() engine.TailStats {
	t.mu.Lock()
	defer t.mu.Unlock()

	buffered := max(t.captured-t.consumed, 0)
	lag := computeLag(buffered, t.oldestPendingAt, t.lastEventAt, time.Now())

	return engine.TailStats{
		Captured:      t.captured,
		Applied:       t.applied,
		Inserts:       t.inserts,
		Deletes:       t.deletes,
		Buffered:      buffered,
		LastEventAt:   t.lastEventAt,
		LastAppliedAt: t.lastAppliedAt,
		Lag:           lag,
	}
}

// Token returns the latest resume token, or the zero token before any event has
// been observed. The returned slice is a copy owned by the caller.
func (t *Tailer) Token() engine.ResumeToken {
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.token) == 0 {
		return nil
	}
	return engine.ResumeToken(cloneBytes(t.token))
}

// Stop signals the reader to wind down. Apply then drains the remaining buffered
// events and returns. Stop is idempotent and safe to call from any goroutine.
func (t *Tailer) Stop() {
	t.stopOnce.Do(func() {
		if t.streamCancel != nil {
			t.streamCancel()
		}
	})
}

// Close stops the tailer, waits for the reader goroutine to exit, and releases
// the change stream. It is safe to call more than once.
func (t *Tailer) Close(ctx context.Context) error {
	t.Stop()

	// Stop cancelled the reader's stream context, so the reader exits promptly.
	// Join it BEFORE touching the stream so Close never calls ChangeStream.Close
	// concurrently with the reader's in-flight Next(). Do not key the wait off the
	// caller's ctx (which is frequently already cancelled — that is exactly the
	// case that used to trip the race); use an independent backstop instead so a
	// misbehaving driver still cannot hang Close forever.
	if t.readerDone != nil {
		select {
		case <-t.readerDone:
		case <-time.After(30 * time.Second):
			t.log.Warn("cdc: reader did not exit within 30s of Stop; closing stream anyway")
		}
	}

	t.mu.Lock()
	already := t.closed
	t.closed = true
	t.mu.Unlock()
	if already {
		return nil
	}

	// Close the stream with a fresh context so a cancelled caller ctx cannot abort
	// the killCursors round-trip.
	if t.source != nil {
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		if err := t.source.Close(closeCtx); err != nil {
			return fmt.Errorf("cdc: close change stream: %w", err)
		}
	}
	return nil
}

// recordAppliedToken remembers tok as the newest durably-applied resume token and
// emits it through OnToken, throttled to at most once per second. It is a no-op for
// an empty token. Called only from the apply goroutine.
func (t *Tailer) recordAppliedToken(tok []byte) {
	if len(tok) == 0 {
		return
	}
	t.appliedToken = tok
	t.emitAppliedToken(tok, false)
}

// emitAppliedToken invokes OnToken with a caller-owned copy of tok. Unless force is
// set it is throttled to at most one call per second (wall clock) to avoid
// checkpoint spam. A nil OnToken or empty token is a no-op. Called only from the
// apply goroutine.
func (t *Tailer) emitAppliedToken(tok []byte, force bool) {
	if t.onToken == nil || len(tok) == 0 {
		return
	}
	now := time.Now
	if t.now != nil {
		now = t.now
	}
	ts := now()
	if !force && !t.lastTokenEmitAt.IsZero() && ts.Sub(t.lastTokenEmitAt) < time.Second {
		return
	}
	t.lastTokenEmitAt = ts
	t.onToken(engine.ResumeToken(cloneBytes(tok)))
}

// flushToken force-emits the most recently applied resume token, bypassing the
// throttle, so Apply always checkpoints its final applied position when it stops.
func (t *Tailer) flushToken() {
	t.emitAppliedToken(t.appliedToken, true)
}

// lastToken returns the resume token of the last event in the batch that carries
// one, or nil if none do. Change events arrive in oplog order, so the last token in
// an applied batch is the furthest-along position that batch reached.
func lastToken(batch []ChangeEvent) []byte {
	for i := len(batch) - 1; i >= 0; i-- {
		if len(batch[i].Token) > 0 {
			return batch[i].Token
		}
	}
	return nil
}

func (t *Tailer) setReadErr(err error) {
	t.mu.Lock()
	if t.readErr == nil {
		t.readErr = err
	}
	t.mu.Unlock()
}

func (t *Tailer) readErrOrNil() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.readErr
}

// cloneBytes returns a copy of b, or nil when b is empty.
func cloneBytes(b []byte) []byte {
	if len(b) == 0 {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

var _ engine.Tailer = (*Tailer)(nil)
