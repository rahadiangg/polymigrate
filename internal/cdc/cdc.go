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
	applier        applier
	buffer         chan ChangeEvent

	streamCtx    context.Context
	streamCancel context.CancelFunc
	stopOnce     sync.Once
	readerDone   chan struct{}
	opened       bool

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
	return &Tailer{src: src, dst: dst, reporter: reporter, log: log}
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
		src, err := openChangeStream(ctx, t.src, opts)
		if err != nil {
			t.streamCancel()
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
		t.mu.Lock()
		t.captured++
		if !ev.At.IsZero() {
			t.lastEventAt = ev.At
		}
		if len(tok) > 0 {
			t.token = cloneBytes(tok)
		}
		t.mu.Unlock()

		select {
		case t.buffer <- ev:
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

	if invalidate {
		t.Stop()
	}
	return nil
}

// Stats returns a consistent snapshot of tailer progress. It is safe to call
// concurrently with the running tailer.
func (t *Tailer) Stats() engine.TailStats {
	t.mu.Lock()
	defer t.mu.Unlock()

	buffered := t.captured - t.consumed
	if buffered < 0 {
		buffered = 0
	}
	// Lag is the wall-clock age of the oldest not-yet-applied event: zero when
	// nothing is buffered, otherwise how long the current backlog's head has been
	// waiting. It is an estimate, refined each time a batch is dequeued.
	var lag time.Duration
	if buffered > 0 && !t.oldestPendingAt.IsZero() {
		lag = time.Since(t.oldestPendingAt)
	}

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

	if t.readerDone != nil {
		select {
		case <-t.readerDone:
		case <-ctx.Done():
			// Fall through to close the stream even if joining timed out.
		}
	}

	t.mu.Lock()
	already := t.closed
	t.closed = true
	t.mu.Unlock()
	if already {
		return nil
	}

	if t.source != nil {
		if err := t.source.Close(ctx); err != nil {
			return fmt.Errorf("cdc: close change stream: %w", err)
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
