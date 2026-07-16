package cdc

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/weefer/mongomigrate/internal/engine"
	"github.com/weefer/mongomigrate/internal/mongox"
	"github.com/weefer/mongomigrate/internal/namespace"
	"github.com/weefer/mongomigrate/internal/progress"
)

// scriptItem is one step in a fakeSource script: either an event or an error.
type scriptItem struct {
	ev    ChangeEvent
	err   error
	token []byte
}

// fakeSource is a scripted, in-memory eventSource. Scripted items are delivered
// in order via a channel; once drained it blocks until the channel is closed
// (end of stream) or the context is cancelled (Stop/Close), mirroring a live
// change stream that waits for new events.
type fakeSource struct {
	items chan scriptItem

	mu        sync.Mutex
	lastToken []byte
	closed    bool
	nextCalls int
}

func newFakeSource(buffer int) *fakeSource {
	return &fakeSource{items: make(chan scriptItem, buffer)}
}

func (f *fakeSource) push(items ...scriptItem) {
	for _, it := range items {
		f.items <- it
	}
}

// endStream signals a server-side end of the change stream (Next returns ok=false).
func (f *fakeSource) endStream() { close(f.items) }

func (f *fakeSource) Next(ctx context.Context) (ChangeEvent, bool, error) {
	f.mu.Lock()
	f.nextCalls++
	f.mu.Unlock()

	select {
	case it, ok := <-f.items:
		if !ok {
			return ChangeEvent{}, false, nil // stream closed by server
		}
		if it.err != nil {
			return ChangeEvent{}, false, it.err
		}
		if it.token != nil {
			f.mu.Lock()
			f.lastToken = it.token
			f.mu.Unlock()
		}
		return it.ev, true, nil
	case <-ctx.Done():
		return ChangeEvent{}, false, ctx.Err()
	}
}

func (f *fakeSource) ResumeToken() []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastToken
}

// callCount reports how many times Next has been entered. Once it exceeds the
// number of scripted items, the reader has drained the script and is blocked
// waiting for the next event, which means every prior event has been buffered.
func (f *fakeSource) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.nextCalls
}

func (f *fakeSource) Close(context.Context) error {
	f.mu.Lock()
	f.closed = true
	f.mu.Unlock()
	return nil
}

// memApplier is an in-memory destination that mirrors Mongo's replace/delete
// semantics so tests can assert the final sink state and idempotency.
type memApplier struct {
	mu            sync.Mutex
	docs          map[string]bson.Raw
	applyCalls    int
	blockUntil    chan struct{} // when non-nil, apply blocks on it (to force backlog)
	appliedTokens [][]byte      // resume tokens of applied events, in apply order
}

func newMemApplier() *memApplier {
	return &memApplier{docs: make(map[string]bson.Raw)}
}

func (m *memApplier) apply(ctx context.Context, batch []ChangeEvent) (int, int, error) {
	if m.blockUntil != nil {
		select {
		case <-m.blockUntil:
		case <-ctx.Done():
			return 0, 0, ctx.Err()
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.applyCalls++
	var ins, del int
	for _, e := range batch {
		if _, ok := eventToModel(e); !ok {
			continue
		}
		if len(e.Token) > 0 {
			m.appliedTokens = append(m.appliedTokens, append([]byte(nil), e.Token...))
		}
		k := idKey(e)
		if e.OpType == opDelete {
			delete(m.docs, k)
			del++
		} else {
			m.docs[k] = e.Full
			ins++
		}
	}
	return ins, del, nil
}

func (m *memApplier) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.docs)
}

// tokenOrder returns a copy of the resume tokens of applied events, in the order
// they were applied.
func (m *memApplier) tokenOrder() [][]byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([][]byte, len(m.appliedTokens))
	copy(out, m.appliedTokens)
	return out
}

// tokenRecorder captures the tokens delivered to TailOptions.OnToken. It is safe
// for concurrent use because OnToken may be called from the apply goroutine.
type tokenRecorder struct {
	mu     sync.Mutex
	tokens [][]byte
}

func (r *tokenRecorder) record(tok engine.ResumeToken) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.tokens = append(r.tokens, append([]byte(nil), tok...))
}

func (r *tokenRecorder) len() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.tokens)
}

func (r *tokenRecorder) all() [][]byte {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([][]byte, len(r.tokens))
	copy(out, r.tokens)
	return out
}

// tokenFor builds a deterministic, distinct per-event resume token.
func tokenFor(i int) []byte { return []byte(fmt.Sprintf("tok-%03d", i)) }

// dataEventTok is dataEvent with a per-event resume Token attached.
func dataEventTok(t *testing.T, op, db, coll string, id int, token []byte) ChangeEvent {
	t.Helper()
	e := dataEvent(t, op, db, coll, id)
	e.Token = token
	return e
}

// errApplier always fails, to exercise Apply's error propagation.
type errApplier struct{ err error }

func (e errApplier) apply(context.Context, []ChangeEvent) (int, int, error) {
	return 0, 0, e.err
}

// dataEvent builds a data ChangeEvent for the loop tests.
func dataEvent(t *testing.T, op, db, coll string, id int) ChangeEvent {
	t.Helper()
	e := ChangeEvent{
		OpType: op,
		NS:     namespace.Namespace{DB: db, Collection: coll},
		ID:     rawID(t, id),
		At:     time.Now(),
	}
	if op != opDelete {
		e.Full = rawDoc(t, bson.D{{Key: "_id", Value: id}, {Key: "op", Value: op}})
	}
	return e
}

// newTestTailer wires a Tailer with injected fakes (no live Mongo).
func newTestTailer(src eventSource, app applier, reporter engine.Reporter) *Tailer {
	tl := New(nil, nil, reporter, nil)
	tl.source = src
	tl.applier = app
	return tl
}

func waitStopped(t *testing.T, ctx context.Context, tl *Tailer, applyErr <-chan error) error {
	t.Helper()
	tl.Stop()
	select {
	case err := <-applyErr:
		return err
	case <-time.After(2 * time.Second):
		t.Fatal("Apply did not return after Stop")
		return nil
	}
}

func TestApply_BasicUpsertAndDelete(t *testing.T) {
	src := newFakeSource(16)
	app := newMemApplier()
	rep := progress.NewCountingReporter()
	tl := newTestTailer(src, app, rep)

	ctx := context.Background()
	// Batch size 1 applies each event on its own, so counts are independent of
	// scheduling (no cross-event dedup).
	if err := tl.Open(ctx, engine.TailOptions{ApplyBatchSize: 1}); err != nil {
		t.Fatalf("Open: %v", err)
	}

	src.push(
		scriptItem{ev: dataEvent(t, opInsert, "d", "c", 1), token: []byte("t1")},
		scriptItem{ev: dataEvent(t, opInsert, "d", "c", 2), token: []byte("t2")},
		scriptItem{ev: dataEvent(t, opUpdate, "d", "c", 1), token: []byte("t3")},
		scriptItem{ev: dataEvent(t, opDelete, "d", "c", 2), token: []byte("t4")},
	)

	applyErr := make(chan error, 1)
	go func() { applyErr <- tl.Apply(ctx) }()

	// Wait until all four events have been applied.
	waitFor(t, func() bool { return tl.Stats().Applied == 4 && tl.Stats().Buffered == 0 })

	if err := waitStopped(t, ctx, tl, applyErr); err != nil {
		t.Fatalf("Apply returned error: %v", err)
	}

	// Final state: doc 1 present (updated), doc 2 deleted.
	if got := app.count(); got != 1 {
		t.Errorf("sink docs = %d, want 1", got)
	}
	st := tl.Stats()
	if st.Captured != 4 {
		t.Errorf("Captured = %d, want 4", st.Captured)
	}
	if st.Deletes != 1 {
		t.Errorf("Deletes = %d, want 1", st.Deletes)
	}
	if st.Inserts != 3 {
		t.Errorf("Inserts = %d, want 3 (2 insert + 1 update)", st.Inserts)
	}
	if st.Buffered != 0 {
		t.Errorf("Buffered = %d, want 0", st.Buffered)
	}
	if tok := tl.Token(); string(tok) != "t4" {
		t.Errorf("Token = %q, want t4", tok)
	}
	if rep.PhaseOrder()[0] != engine.PhaseTailOpen {
		t.Errorf("first phase = %v, want tail-open", rep.PhaseOrder()[0])
	}

	if err := tl.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

func TestApply_DedupWithinBatch(t *testing.T) {
	src := newFakeSource(16)
	app := newMemApplier()
	tl := newTestTailer(src, app, nil)

	ctx := context.Background()
	if err := tl.Open(ctx, engine.TailOptions{ApplyBatchSize: 100}); err != nil {
		t.Fatalf("Open: %v", err)
	}

	src.push(
		scriptItem{ev: dataEvent(t, opInsert, "d", "c", 1)},
		scriptItem{ev: dataEvent(t, opInsert, "d", "c", 1)},
		scriptItem{ev: dataEvent(t, opUpdate, "d", "c", 1)},
		scriptItem{ev: dataEvent(t, opInsert, "d", "c", 2)},
		scriptItem{ev: dataEvent(t, opDelete, "d", "c", 2)},
	)

	// Let the reader buffer all five events (it blocks on the 6th Next once the
	// script is drained) BEFORE Apply runs, so nextBatch collects them as one
	// batch and dedup happens within it.
	waitFor(t, func() bool { return src.callCount() >= 6 && tl.Stats().Captured == 5 })

	applyErr := make(chan error, 1)
	go func() { applyErr <- tl.Apply(ctx) }()

	waitFor(t, func() bool { return tl.Stats().Buffered == 0 })
	if err := waitStopped(t, ctx, tl, applyErr); err != nil {
		t.Fatalf("Apply error: %v", err)
	}

	if app.applyCalls != 1 {
		t.Errorf("applyCalls = %d, want 1 (single deduped batch)", app.applyCalls)
	}
	// After dedup: id 1 -> update (present), id 2 -> delete (absent).
	if got := app.count(); got != 1 {
		t.Errorf("sink docs = %d, want 1", got)
	}
	st := tl.Stats()
	// Two writes survive dedup: upsert id 1 (latest = update) and delete id 2.
	if st.Applied != 2 {
		t.Errorf("Applied = %d, want 2 (upsert id1 + delete id2)", st.Applied)
	}
	if st.Inserts != 1 || st.Deletes != 1 {
		t.Errorf("Inserts=%d Deletes=%d, want 1 and 1", st.Inserts, st.Deletes)
	}
	if st.Buffered != 0 {
		t.Errorf("Buffered = %d, want 0 after drain", st.Buffered)
	}
	_ = tl.Close(ctx)
}

func TestApply_IdempotentReplay(t *testing.T) {
	events := func(t *testing.T) []scriptItem {
		return []scriptItem{
			{ev: dataEvent(t, opInsert, "d", "c", 1)},
			{ev: dataEvent(t, opUpdate, "d", "c", 1)},
			{ev: dataEvent(t, opInsert, "d", "c", 2)},
			{ev: dataEvent(t, opDelete, "d", "c", 1)},
		}
	}

	run := func() map[string]bson.Raw {
		src := newFakeSource(16)
		app := newMemApplier()
		tl := newTestTailer(src, app, nil)
		ctx := context.Background()
		if err := tl.Open(ctx, engine.TailOptions{ApplyBatchSize: 2}); err != nil {
			t.Fatalf("Open: %v", err)
		}
		src.push(events(t)...)
		applyErr := make(chan error, 1)
		go func() { applyErr <- tl.Apply(ctx) }()
		waitFor(t, func() bool { return tl.Stats().Captured == 4 && tl.Stats().Buffered == 0 })
		if err := waitStopped(t, ctx, tl, applyErr); err != nil {
			t.Fatalf("Apply error: %v", err)
		}
		_ = tl.Close(ctx)
		app.mu.Lock()
		defer app.mu.Unlock()
		out := make(map[string]bson.Raw, len(app.docs))
		for k, v := range app.docs {
			out[k] = v
		}
		return out
	}

	first := run()
	second := run()

	// Replaying the same stream yields the same sink state (only id 2 remains).
	if len(first) != 1 || len(second) != 1 {
		t.Fatalf("expected single surviving doc, got %d and %d", len(first), len(second))
	}
	for k := range first {
		if _, ok := second[k]; !ok {
			t.Errorf("key %q present in first run but not second", k)
		}
	}
}

func TestReadLoop_NamespaceFiltering(t *testing.T) {
	src := newFakeSource(16)
	app := newMemApplier()
	tl := newTestTailer(src, app, nil)

	ctx := context.Background()
	allowed := []namespace.Namespace{{DB: "d", Collection: "keep"}}
	if err := tl.Open(ctx, engine.TailOptions{Namespaces: allowed, ApplyBatchSize: 10}); err != nil {
		t.Fatalf("Open: %v", err)
	}

	src.push(
		scriptItem{ev: dataEvent(t, opInsert, "d", "keep", 1), token: []byte("k1")},
		scriptItem{ev: dataEvent(t, opInsert, "d", "drop", 2), token: []byte("d2")}, // filtered
		scriptItem{ev: dataEvent(t, opInsert, "d", "keep", 3), token: []byte("k3")},
	)

	applyErr := make(chan error, 1)
	go func() { applyErr <- tl.Apply(ctx) }()
	waitFor(t, func() bool { return tl.Stats().Captured == 2 && tl.Stats().Buffered == 0 })
	if err := waitStopped(t, ctx, tl, applyErr); err != nil {
		t.Fatalf("Apply error: %v", err)
	}

	st := tl.Stats()
	if st.Captured != 2 {
		t.Errorf("Captured = %d, want 2 (filtered event excluded)", st.Captured)
	}
	if app.count() != 2 {
		t.Errorf("sink docs = %d, want 2", app.count())
	}
	// The token advances even across the filtered-out event.
	if tok := tl.Token(); string(tok) != "k3" {
		t.Errorf("Token = %q, want k3", tok)
	}
	_ = tl.Close(ctx)
}

func TestApply_InvalidateStopsStream(t *testing.T) {
	src := newFakeSource(16)
	app := newMemApplier()
	rep := progress.NewCountingReporter()
	tl := newTestTailer(src, app, rep)

	ctx := context.Background()
	if err := tl.Open(ctx, engine.TailOptions{ApplyBatchSize: 1}); err != nil {
		t.Fatalf("Open: %v", err)
	}

	src.push(
		scriptItem{ev: dataEvent(t, opInsert, "d", "c", 1)},
		scriptItem{ev: ChangeEvent{OpType: opInvalidate, At: time.Now()}},
	)

	// No explicit Stop: the invalidate event must wind the tailer down on its own.
	applyErr := make(chan error, 1)
	go func() { applyErr <- tl.Apply(ctx) }()

	select {
	case err := <-applyErr:
		if err != nil {
			t.Fatalf("Apply returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Apply did not return after invalidate")
	}

	if app.count() != 1 {
		t.Errorf("sink docs = %d, want 1", app.count())
	}
	_ = tl.Close(ctx)
}

func TestApply_StreamErrorSurfaced(t *testing.T) {
	src := newFakeSource(16)
	app := newMemApplier()
	tl := newTestTailer(src, app, nil)

	ctx := context.Background()
	if err := tl.Open(ctx, engine.TailOptions{ApplyBatchSize: 4}); err != nil {
		t.Fatalf("Open: %v", err)
	}

	wantErr := errors.New("history lost 286")
	src.push(
		scriptItem{ev: dataEvent(t, opInsert, "d", "c", 1)},
		scriptItem{err: wantErr},
	)

	applyErr := make(chan error, 1)
	go func() { applyErr <- tl.Apply(ctx) }()

	select {
	case err := <-applyErr:
		if !errors.Is(err, wantErr) {
			t.Fatalf("Apply err = %v, want wrapping %v", err, wantErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Apply did not return after stream error")
	}
	_ = tl.Close(ctx)
}

func TestApply_ContextCancellation(t *testing.T) {
	src := newFakeSource(16)
	app := newMemApplier()
	tl := newTestTailer(src, app, nil)

	ctx, cancel := context.WithCancel(context.Background())
	if err := tl.Open(ctx, engine.TailOptions{ApplyBatchSize: 4}); err != nil {
		t.Fatalf("Open: %v", err)
	}

	// No events; Apply blocks waiting on the buffer. Cancelling ctx must unblock it.
	applyErr := make(chan error, 1)
	go func() { applyErr <- tl.Apply(ctx) }()
	time.Sleep(20 * time.Millisecond)
	cancel()

	select {
	case err := <-applyErr:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Apply err = %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Apply did not return after ctx cancel")
	}
	_ = tl.Close(context.Background())
}

func TestApply_ErrorFromApplierPropagates(t *testing.T) {
	src := newFakeSource(16)
	wantErr := errors.New("write failed")
	tl := newTestTailer(src, errApplier{err: wantErr}, nil)

	ctx := context.Background()
	if err := tl.Open(ctx, engine.TailOptions{ApplyBatchSize: 4}); err != nil {
		t.Fatalf("Open: %v", err)
	}
	src.push(scriptItem{ev: dataEvent(t, opInsert, "d", "c", 1)})

	applyErr := make(chan error, 1)
	go func() { applyErr <- tl.Apply(ctx) }()

	select {
	case err := <-applyErr:
		if !errors.Is(err, wantErr) {
			t.Fatalf("Apply err = %v, want wrapping %v", err, wantErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Apply did not return after applier error")
	}
	tl.Stop()
	_ = tl.Close(ctx)
}

func TestLag_ZeroWhenDrained(t *testing.T) {
	src := newFakeSource(16)
	app := newMemApplier()
	tl := newTestTailer(src, app, nil)

	ctx := context.Background()
	if err := tl.Open(ctx, engine.TailOptions{ApplyBatchSize: 10}); err != nil {
		t.Fatalf("Open: %v", err)
	}
	src.push(scriptItem{ev: dataEvent(t, opInsert, "d", "c", 1)})

	applyErr := make(chan error, 1)
	go func() { applyErr <- tl.Apply(ctx) }()
	waitFor(t, func() bool { return tl.Stats().Applied == 1 && tl.Stats().Buffered == 0 })

	if lag := tl.Stats().Lag; lag != 0 {
		t.Errorf("Lag = %v, want 0 when buffer drained", lag)
	}
	if err := waitStopped(t, ctx, tl, applyErr); err != nil {
		t.Fatalf("Apply error: %v", err)
	}
	_ = tl.Close(ctx)
}

func TestLag_NonZeroWithBacklog(t *testing.T) {
	src := newFakeSource(16)
	app := newMemApplier()
	app.blockUntil = make(chan struct{})
	tl := newTestTailer(src, app, nil)

	ctx := context.Background()
	if err := tl.Open(ctx, engine.TailOptions{ApplyBatchSize: 1}); err != nil {
		t.Fatalf("Open: %v", err)
	}
	// Two events; apply blocks on the first, so the second stays buffered.
	src.push(
		scriptItem{ev: dataEvent(t, opInsert, "d", "c", 1)},
		scriptItem{ev: dataEvent(t, opInsert, "d", "c", 2)},
	)

	applyErr := make(chan error, 1)
	go func() { applyErr <- tl.Apply(ctx) }()

	waitFor(t, func() bool { return tl.Stats().Buffered > 0 && tl.Stats().Lag > 0 })
	if lag := tl.Stats().Lag; lag <= 0 {
		t.Errorf("Lag = %v, want > 0 with backlog", lag)
	}

	close(app.blockUntil)
	if err := waitStopped(t, ctx, tl, applyErr); err != nil {
		t.Fatalf("Apply error: %v", err)
	}
	_ = tl.Close(ctx)
}

func TestClose_JoinsReaderNoLeak(t *testing.T) {
	src := newFakeSource(16)
	app := newMemApplier()
	tl := newTestTailer(src, app, nil)

	ctx := context.Background()
	if err := tl.Open(ctx, engine.TailOptions{}); err != nil {
		t.Fatalf("Open: %v", err)
	}
	src.push(scriptItem{ev: dataEvent(t, opInsert, "d", "c", 1)})

	applyErr := make(chan error, 1)
	go func() { applyErr <- tl.Apply(ctx) }()
	waitFor(t, func() bool { return tl.Stats().Applied == 1 })

	tl.Stop()
	if err := <-applyErr; err != nil {
		t.Fatalf("Apply error: %v", err)
	}
	// Close must return promptly and be safe to call twice.
	if err := tl.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := tl.Close(ctx); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	// The reader goroutine must have exited (readerDone closed).
	select {
	case <-tl.readerDone:
	case <-time.After(time.Second):
		t.Fatal("reader goroutine did not exit")
	}
}

func TestOpen_Twice(t *testing.T) {
	tl := newTestTailer(newFakeSource(1), newMemApplier(), nil)
	ctx := context.Background()
	if err := tl.Open(ctx, engine.TailOptions{}); err != nil {
		t.Fatalf("first Open: %v", err)
	}
	if err := tl.Open(ctx, engine.TailOptions{}); err == nil {
		t.Fatal("second Open should error")
	}
	tl.Stop()
	_ = tl.Close(ctx)
}

func TestApply_BeforeOpen(t *testing.T) {
	tl := New(nil, nil, nil, nil)
	if err := tl.Apply(context.Background()); err == nil {
		t.Fatal("Apply before Open should error")
	}
}

func TestResumeTokenSeeded(t *testing.T) {
	tl := newTestTailer(newFakeSource(1), newMemApplier(), nil)
	ctx := context.Background()
	if err := tl.Open(ctx, engine.TailOptions{Resume: engine.ResumeToken("seed-token")}); err != nil {
		t.Fatalf("Open: %v", err)
	}
	if tok := tl.Token(); string(tok) != "seed-token" {
		t.Errorf("Token = %q, want seed-token before any event", tok)
	}
	tl.Stop()
	_ = tl.Close(ctx)
}

// TestOnToken_NeverAheadOfApplied is the checkpoint-ordering invariant: OnToken
// must only ever receive the token of an event that has ALREADY been applied to the
// destination — never a token the reader captured ahead of a slow applier. Losing
// that ordering would let a resumed stream skip unapplied events and drop data.
func TestOnToken_NeverAheadOfApplied(t *testing.T) {
	src := newFakeSource(64)
	app := newMemApplier()
	app.blockUntil = make(chan struct{})
	tl := newTestTailer(src, app, nil)

	// An ever-advancing clock so the 1/sec throttle never suppresses an emit: every
	// applied batch checkpoints its token, making the ordering observable.
	var clkMu sync.Mutex
	clk := time.Unix(0, 0)
	tl.now = func() time.Time {
		clkMu.Lock()
		defer clkMu.Unlock()
		clk = clk.Add(2 * time.Second)
		return clk
	}

	rec := &tokenRecorder{}
	// Each delivered token must equal the LAST APPLIED event's token at the instant
	// OnToken fires — never the captured-ahead token. Because recordAppliedToken runs
	// right after applier.apply returns (same goroutine), the applier's last recorded
	// token is exactly the batch just written. A bug that checkpointed the captured
	// token (which has raced ahead to tokenFor(n)) would violate this immediately.
	onToken := func(tok engine.ResumeToken) {
		order := app.tokenOrder()
		if len(order) == 0 {
			t.Errorf("OnToken fired before any event was applied: %q", tok)
		} else if want := order[len(order)-1]; string(tok) != string(want) {
			t.Errorf("OnToken = %q, but last APPLIED token is %q (token ran ahead of applied data)", tok, want)
		}
		rec.record(tok)
	}
	ctx := context.Background()
	if err := tl.Open(ctx, engine.TailOptions{ApplyBatchSize: 1, BufferSize: 64, OnToken: onToken}); err != nil {
		t.Fatalf("Open: %v", err)
	}

	const n = 8
	for i := 1; i <= n; i++ {
		src.push(scriptItem{ev: dataEventTok(t, opInsert, "d", "c", i, tokenFor(i)), token: tokenFor(i)})
	}

	applyErr := make(chan error, 1)
	go func() { applyErr <- tl.Apply(ctx) }()

	// The reader races ahead of the blocked applier: all n events are captured and
	// the CAPTURED token has reached the last event, yet NO applied token has been
	// emitted. This is the core proof that OnToken never runs ahead of applied data.
	waitFor(t, func() bool { return tl.Stats().Captured == n })
	if got := tl.Token(); string(got) != string(tokenFor(n)) {
		t.Fatalf("captured Token = %q, want %q (reader should be ahead)", got, tokenFor(n))
	}
	if got := rec.len(); got != 0 {
		t.Fatalf("OnToken emitted %d tokens while the applier was blocked; it must never run ahead of applied data", got)
	}

	close(app.blockUntil) // let the applier drain the backlog
	if err := waitStopped(t, ctx, tl, applyErr); err != nil {
		t.Fatalf("Apply error: %v", err)
	}

	// Every emitted token corresponds to an already-applied event, in non-decreasing
	// apply order, and the final token is the last applied event's token.
	emitted := rec.all()
	if len(emitted) == 0 {
		t.Fatal("no applied tokens were emitted")
	}
	pos := make(map[string]int)
	for i, tok := range app.tokenOrder() {
		pos[string(tok)] = i
	}
	last := -1
	for _, tok := range emitted {
		p, ok := pos[string(tok)]
		if !ok {
			t.Fatalf("emitted token %q was never applied (ran ahead of applied data)", tok)
		}
		if p < last {
			t.Fatalf("emitted tokens not in apply order: %q at apply pos %d after %d", tok, p, last)
		}
		last = p
	}
	if final := emitted[len(emitted)-1]; string(final) != string(tokenFor(n)) {
		t.Errorf("final emitted token = %q, want %q (last applied)", final, tokenFor(n))
	}
	_ = tl.Close(ctx)
}

// TestOnToken_Throttle asserts OnToken is throttled to at most once per second, yet
// always flushes the final applied token when Apply stops. With a frozen clock only
// the first batch clears the throttle, so exactly two tokens surface: the first and
// the forced final flush.
func TestOnToken_Throttle(t *testing.T) {
	src := newFakeSource(16)
	app := newMemApplier()
	tl := newTestTailer(src, app, nil)

	frozen := time.Unix(1000, 0)
	tl.now = func() time.Time { return frozen }

	rec := &tokenRecorder{}
	ctx := context.Background()
	if err := tl.Open(ctx, engine.TailOptions{ApplyBatchSize: 1, OnToken: rec.record}); err != nil {
		t.Fatalf("Open: %v", err)
	}

	const n = 5
	for i := 1; i <= n; i++ {
		src.push(scriptItem{ev: dataEventTok(t, opInsert, "d", "c", i, tokenFor(i))})
	}

	applyErr := make(chan error, 1)
	go func() { applyErr <- tl.Apply(ctx) }()
	waitFor(t, func() bool { return tl.Stats().Applied == n && tl.Stats().Buffered == 0 })
	if err := waitStopped(t, ctx, tl, applyErr); err != nil {
		t.Fatalf("Apply error: %v", err)
	}

	got := rec.all()
	if len(got) != 2 {
		t.Fatalf("emitted %d tokens, want 2 (first batch + forced final flush)", len(got))
	}
	if string(got[0]) != string(tokenFor(1)) {
		t.Errorf("first emit = %q, want %q", got[0], tokenFor(1))
	}
	if string(got[1]) != string(tokenFor(n)) {
		t.Errorf("final emit = %q, want %q (last applied)", got[1], tokenFor(n))
	}
	_ = tl.Close(ctx)
}

// TestOnToken_NilGuard confirms a nil OnToken is tolerated end to end.
func TestOnToken_NilGuard(t *testing.T) {
	src := newFakeSource(16)
	app := newMemApplier()
	tl := newTestTailer(src, app, nil)

	ctx := context.Background()
	if err := tl.Open(ctx, engine.TailOptions{ApplyBatchSize: 1}); err != nil { // OnToken nil
		t.Fatalf("Open: %v", err)
	}
	src.push(scriptItem{ev: dataEventTok(t, opInsert, "d", "c", 1, tokenFor(1))})

	applyErr := make(chan error, 1)
	go func() { applyErr <- tl.Apply(ctx) }()
	waitFor(t, func() bool { return tl.Stats().Applied == 1 })
	if err := waitStopped(t, ctx, tl, applyErr); err != nil {
		t.Fatalf("Apply error: %v", err)
	}
	_ = tl.Close(ctx)
}

// TestOpen_PassesResumeToSource asserts the resume token from TailOptions.Resume is
// handed to the source factory (which wires it to SetResumeAfter).
func TestOpen_PassesResumeToSource(t *testing.T) {
	var gotOpts engine.TailOptions
	fs := newFakeSource(1)

	tl := New(nil, nil, nil, nil)
	tl.sourceFactory = func(_ context.Context, _ *mongox.Client, opts engine.TailOptions) (eventSource, error) {
		gotOpts = opts
		return fs, nil
	}

	ctx := context.Background()
	resume := engine.ResumeToken("resume-token-xyz")
	if err := tl.Open(ctx, engine.TailOptions{Resume: resume}); err != nil {
		t.Fatalf("Open: %v", err)
	}
	if string(gotOpts.Resume) != string(resume) {
		t.Errorf("factory received Resume = %q, want %q", gotOpts.Resume, resume)
	}
	// And Token() reflects the seeded resume token before any event advances it.
	if tok := tl.Token(); string(tok) != string(resume) {
		t.Errorf("Token = %q, want seeded %q", tok, resume)
	}
	tl.Stop()
	_ = tl.Close(ctx)
}

// TestApply_ChangeStreamHistoryLost asserts a 286 (ChangeStreamHistoryLost) read
// error surfaces from Apply as the exported sentinel, matchable with errors.Is, so
// the orchestrator can mark the run non-resumable.
func TestApply_ChangeStreamHistoryLost(t *testing.T) {
	src := newFakeSource(16)
	app := newMemApplier()
	tl := newTestTailer(src, app, nil)

	ctx := context.Background()
	if err := tl.Open(ctx, engine.TailOptions{ApplyBatchSize: 4}); err != nil {
		t.Fatalf("Open: %v", err)
	}

	historyLost := mongo.CommandError{
		Code:    286,
		Name:    "ChangeStreamHistoryLost",
		Message: "resume of change stream was not possible, as the resume point may no longer be in the oplog",
	}
	src.push(
		scriptItem{ev: dataEvent(t, opInsert, "d", "c", 1)},
		scriptItem{err: historyLost},
	)

	applyErr := make(chan error, 1)
	go func() { applyErr <- tl.Apply(ctx) }()

	select {
	case err := <-applyErr:
		if !errors.Is(err, ErrChangeStreamHistoryLost) {
			t.Fatalf("Apply err = %v, want wrapping ErrChangeStreamHistoryLost", err)
		}
		// The underlying command error is preserved in the chain too.
		var ce mongo.CommandError
		if !errors.As(err, &ce) || ce.Code != 286 {
			t.Errorf("underlying CommandError (code 286) not preserved: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Apply did not return after history-lost error")
	}
	_ = tl.Close(ctx)
}

// TestIsChangeStreamHistoryLost is a focused table test of the 286 classifier.
func TestIsChangeStreamHistoryLost(t *testing.T) {
	if isChangeStreamHistoryLost(nil) {
		t.Error("nil must not be history-lost")
	}
	if isChangeStreamHistoryLost(errors.New("history lost 286")) {
		t.Error("a plain error mentioning 286 must not be classified as history-lost")
	}
	if !isChangeStreamHistoryLost(mongo.CommandError{Code: 286}) {
		t.Error("CommandError code 286 must be history-lost")
	}
	if !isChangeStreamHistoryLost(fmt.Errorf("wrap: %w", mongo.CommandError{Code: 286})) {
		t.Error("wrapped CommandError code 286 must be history-lost")
	}
	if isChangeStreamHistoryLost(mongo.CommandError{Code: 11000}) {
		t.Error("a duplicate-key CommandError must not be history-lost")
	}
}

// waitFor polls cond until true or a timeout, failing the test on timeout. It
// lets the concurrent Open/Apply loops make progress without fixed sleeps.
func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("condition not met within timeout")
}
