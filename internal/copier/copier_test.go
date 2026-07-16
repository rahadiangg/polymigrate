package copier

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/weefer/mongomigrate/internal/engine"
	"github.com/weefer/mongomigrate/internal/namespace"
	"github.com/weefer/mongomigrate/internal/progress"
)

// -----------------------------------------------------------------------------
// Test doubles
// -----------------------------------------------------------------------------

// gate is a synchronization barrier used by the concurrency test: every source
// collection that opens a cursor announces its arrival, then blocks until the
// test releases it. This proves N collections are genuinely in flight at once.
type gate struct {
	arrived chan struct{}
	release chan struct{}
}

func (g *gate) arrive(ctx context.Context) {
	if g == nil {
		return
	}
	select {
	case g.arrived <- struct{}{}:
	case <-ctx.Done():
		return
	}
	select {
	case <-g.release:
	case <-ctx.Done():
	}
}

type fakeCursor struct {
	docs        []bson.Raw
	pos         int
	cur         bson.Raw
	err         error
	closed      bool
	decodeErrAt int // 0-based document index that fails Decode; -1 = never
	respectCtx  bool
	cancel      context.CancelFunc // invoked once pos reaches cancelAt
	cancelAt    int
}

func (c *fakeCursor) Next(ctx context.Context) bool {
	if c.respectCtx && ctx.Err() != nil {
		c.err = ctx.Err()
		return false
	}
	if c.pos >= len(c.docs) {
		return false
	}
	c.cur = c.docs[c.pos]
	c.pos++
	if c.cancel != nil && c.pos == c.cancelAt {
		c.cancel()
	}
	return true
}

func (c *fakeCursor) Decode(v *bson.Raw) error {
	if c.decodeErrAt >= 0 && c.pos-1 == c.decodeErrAt {
		return errors.New("decode boom")
	}
	*v = c.cur
	return nil
}

func (c *fakeCursor) Err() error                  { return c.err }
func (c *fakeCursor) Close(context.Context) error { c.closed = true; return nil }

type fakeSrcColl struct {
	docs       []bson.Raw
	countVal   int64
	countErr   error
	findErr    error
	indexes    []bson.Raw
	indexErr   error
	gate       *gate
	respectCtx bool
	cancel     context.CancelFunc
	cancelAt   int
	decodeErr  int // passed to cursor; -1 = never

	mu         sync.Mutex
	lastCursor *fakeCursor
	findBatch  int // records the batch size the copier requested
}

func (s *fakeSrcColl) count(ctx context.Context) (int64, error) {
	if s.countErr != nil {
		return 0, s.countErr
	}
	if s.countVal != 0 {
		return s.countVal, nil
	}
	return int64(len(s.docs)), nil
}

func (s *fakeSrcColl) find(ctx context.Context, batch int) (docCursor, error) {
	s.gate.arrive(ctx)
	if s.findErr != nil {
		return nil, s.findErr
	}
	cur := &fakeCursor{
		docs:        s.docs,
		decodeErrAt: s.decodeErr,
		respectCtx:  s.respectCtx,
		cancel:      s.cancel,
		cancelAt:    s.cancelAt,
	}
	s.mu.Lock()
	s.lastCursor = cur
	s.findBatch = batch
	s.mu.Unlock()
	return cur, nil
}

func (s *fakeSrcColl) listIndexes(context.Context) ([]bson.Raw, error) {
	if s.indexErr != nil {
		return nil, s.indexErr
	}
	return s.indexes, nil
}

type fakeDstColl struct {
	dropErr   error
	countVal  int64
	countErr  error
	createErr error
	createN   int // returned index count; 0 => len(models)
	writeFn   func(docs []bson.Raw) (inserted, dupes, failed int, err error)

	mu            sync.Mutex
	dropped       bool
	batches       [][]bson.Raw
	createdModels []mongo.IndexModel
}

func (d *fakeDstColl) drop(context.Context) error {
	if d.dropErr != nil {
		return d.dropErr
	}
	d.mu.Lock()
	d.dropped = true
	d.mu.Unlock()
	return nil
}

func (d *fakeDstColl) count(context.Context) (int64, error) {
	if d.countErr != nil {
		return 0, d.countErr
	}
	return d.countVal, nil
}

func (d *fakeDstColl) writeBatch(ctx context.Context, docs []bson.Raw) (int, int, int, error) {
	cp := make([]bson.Raw, len(docs))
	copy(cp, docs)
	d.mu.Lock()
	d.batches = append(d.batches, cp)
	d.mu.Unlock()
	if d.writeFn != nil {
		return d.writeFn(cp)
	}
	return len(docs), 0, 0, nil
}

func (d *fakeDstColl) createIndexes(ctx context.Context, models []mongo.IndexModel) (int, error) {
	d.mu.Lock()
	d.createdModels = append(d.createdModels, models...)
	d.mu.Unlock()
	if d.createErr != nil {
		return 0, d.createErr
	}
	if d.createN != 0 {
		return d.createN, nil
	}
	return len(models), nil
}

// allBatches returns a copy of the recorded batches for assertion.
func (d *fakeDstColl) allBatches() [][]bson.Raw {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([][]bson.Raw, len(d.batches))
	copy(out, d.batches)
	return out
}

// flatten concatenates every recorded batch in order.
func (d *fakeDstColl) flatten() []bson.Raw {
	var out []bson.Raw
	for _, b := range d.allBatches() {
		out = append(out, b...)
	}
	return out
}

type fakeFactory struct {
	src map[string]srcColl
	dst map[string]dstColl
}

func newFakeFactory() *fakeFactory {
	return &fakeFactory{src: map[string]srcColl{}, dst: map[string]dstColl{}}
}

func (f *fakeFactory) add(ns namespace.Namespace, s *fakeSrcColl, d *fakeDstColl) {
	if s == nil {
		s = &fakeSrcColl{decodeErr: -1}
	}
	if d == nil {
		d = &fakeDstColl{}
	}
	f.src[ns.String()] = s
	f.dst[ns.String()] = d
}

func (f *fakeFactory) srcColl(ns namespace.Namespace) srcColl { return f.src[ns.String()] }
func (f *fakeFactory) dstColl(ns namespace.Namespace) dstColl { return f.dst[ns.String()] }

// recordReporter captures progress calls so tests can assert on the total value.
type recordReporter struct {
	mu       sync.Mutex
	events   int
	phases   int
	progress []progressCall
}

type progressCall struct {
	ns          namespace.Namespace
	done, total int64
}

func (r *recordReporter) Phase(engine.Phase) {
	r.mu.Lock()
	r.phases++
	r.mu.Unlock()
}

func (r *recordReporter) Event(string, ...any) {
	r.mu.Lock()
	r.events++
	r.mu.Unlock()
}

func (r *recordReporter) Progress(ns namespace.Namespace, done, total int64) {
	r.mu.Lock()
	r.progress = append(r.progress, progressCall{ns, done, total})
	r.mu.Unlock()
}

func (r *recordReporter) progressCalls() []progressCall {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]progressCall, len(r.progress))
	copy(out, r.progress)
	return out
}

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

func ns(db, coll string) namespace.Namespace { return namespace.Namespace{DB: db, Collection: coll} }

func doc(t *testing.T, id int) bson.Raw {
	t.Helper()
	return mustRaw(t, bson.D{{Key: "_id", Value: int32(id)}, {Key: "n", Value: int32(id * 10)}})
}

func makeDocs(t *testing.T, n int) []bson.Raw {
	t.Helper()
	out := make([]bson.Raw, n)
	for i := 0; i < n; i++ {
		out[i] = doc(t, i)
	}
	return out
}

func batchLens(batches [][]bson.Raw) []int {
	out := make([]int, len(batches))
	for i, b := range batches {
		out[i] = len(b)
	}
	return out
}

func intsEqual(a, b []int) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// -----------------------------------------------------------------------------
// Tests
// -----------------------------------------------------------------------------

func TestCopy_BasicBatching(t *testing.T) {
	f := newFakeFactory()
	n1 := ns("shop", "orders")
	src := &fakeSrcColl{docs: makeDocs(t, 5), decodeErr: -1}
	dst := &fakeDstColl{}
	f.add(n1, src, dst)

	c := newWithFactory(f, engine.NopReporter{}, nil)
	res, err := c.Copy(context.Background(), engine.CopyOptions{
		Namespaces: []namespace.Namespace{n1},
		Workers:    1,
		BatchSize:  2,
	})
	if err != nil {
		t.Fatalf("Copy error: %v", err)
	}
	if res.TotalDocs != 5 || res.TotalFailed != 0 {
		t.Errorf("TotalDocs=%d TotalFailed=%d, want 5/0", res.TotalDocs, res.TotalFailed)
	}
	if res.Failed() {
		t.Errorf("unexpected Failed()=true: %+v", res.Collections)
	}
	if got := batchLens(dst.allBatches()); !intsEqual(got, []int{2, 2, 1}) {
		t.Errorf("batch sizes = %v, want [2 2 1]", got)
	}
	// Every source document must reach the destination, in order.
	got := dst.flatten()
	want := src.docs
	if len(got) != len(want) {
		t.Fatalf("delivered %d docs, want %d", len(got), len(want))
	}
	for i := range want {
		if !bytes.Equal(got[i], want[i]) {
			t.Errorf("doc[%d] mismatch", i)
		}
	}
	if len(res.Collections) != 1 || res.Collections[0].DocsCopied != 5 {
		t.Errorf("collection result = %+v", res.Collections)
	}
	if !src.lastCursor.closed {
		t.Errorf("cursor was not closed")
	}
}

func TestCopy_BatchSizeDefaultAndWorkerClamp(t *testing.T) {
	f := newFakeFactory()
	n1 := ns("db", "c")
	src := &fakeSrcColl{docs: makeDocs(t, 3), decodeErr: -1}
	dst := &fakeDstColl{}
	f.add(n1, src, dst)

	c := newWithFactory(f, engine.NopReporter{}, nil)
	// Workers=0 and BatchSize=0 must be normalized (1 worker, default batch).
	res, err := c.Copy(context.Background(), engine.CopyOptions{
		Namespaces: []namespace.Namespace{n1},
	})
	if err != nil {
		t.Fatalf("Copy error: %v", err)
	}
	if res.TotalDocs != 3 {
		t.Errorf("TotalDocs=%d, want 3", res.TotalDocs)
	}
	// With the large default batch size, all 3 docs land in a single batch.
	if got := batchLens(dst.allBatches()); !intsEqual(got, []int{3}) {
		t.Errorf("batch sizes = %v, want [3]", got)
	}
}

func TestCopy_EmptyCollection(t *testing.T) {
	f := newFakeFactory()
	n1 := ns("db", "empty")
	src := &fakeSrcColl{docs: nil, decodeErr: -1}
	dst := &fakeDstColl{}
	f.add(n1, src, dst)

	c := newWithFactory(f, engine.NopReporter{}, nil)
	res, err := c.Copy(context.Background(), engine.CopyOptions{
		Namespaces: []namespace.Namespace{n1},
		Workers:    1,
		BatchSize:  10,
	})
	if err != nil {
		t.Fatalf("Copy error: %v", err)
	}
	if res.TotalDocs != 0 || res.Failed() {
		t.Errorf("expected empty clean copy, got docs=%d failed=%v", res.TotalDocs, res.Failed())
	}
	if len(dst.allBatches()) != 0 {
		t.Errorf("expected no batches, got %d", len(dst.allBatches()))
	}
}

func TestCopy_MultipleCollectionsAggregate(t *testing.T) {
	f := newFakeFactory()
	names := []namespace.Namespace{ns("a", "x"), ns("a", "y"), ns("b", "z")}
	counts := []int{4, 7, 1}
	for i, n := range names {
		f.add(n, &fakeSrcColl{docs: makeDocs(t, counts[i]), decodeErr: -1}, &fakeDstColl{})
	}
	c := newWithFactory(f, engine.NopReporter{}, nil)
	res, err := c.Copy(context.Background(), engine.CopyOptions{
		Namespaces: names,
		Workers:    2,
		BatchSize:  3,
	})
	if err != nil {
		t.Fatalf("Copy error: %v", err)
	}
	if res.TotalDocs != 12 {
		t.Errorf("TotalDocs=%d, want 12", res.TotalDocs)
	}
	if len(res.Collections) != 3 {
		t.Fatalf("got %d collection results, want 3", len(res.Collections))
	}
	// Results must be aligned to the input order regardless of worker scheduling.
	for i, n := range names {
		if res.Collections[i].Namespace != n {
			t.Errorf("result[%d].Namespace=%v, want %v", i, res.Collections[i].Namespace, n)
		}
		if res.Collections[i].DocsCopied != int64(counts[i]) {
			t.Errorf("result[%d].DocsCopied=%d, want %d", i, res.Collections[i].DocsCopied, counts[i])
		}
	}
}

func TestCopy_DropTarget(t *testing.T) {
	f := newFakeFactory()
	n1 := ns("db", "c")
	src := &fakeSrcColl{docs: makeDocs(t, 2), decodeErr: -1}
	// Destination is non-empty; DropTarget must take precedence over any skip.
	dst := &fakeDstColl{countVal: 99}
	f.add(n1, src, dst)

	c := newWithFactory(f, engine.NopReporter{}, nil)
	res, err := c.Copy(context.Background(), engine.CopyOptions{
		Namespaces:           []namespace.Namespace{n1},
		Workers:              1,
		BatchSize:            10,
		DropTarget:           true,
		SkipExistingNonEmpty: true, // must be ignored because DropTarget wins
	})
	if err != nil {
		t.Fatalf("Copy error: %v", err)
	}
	if !dst.dropped {
		t.Errorf("expected destination to be dropped")
	}
	if res.Collections[0].Skipped {
		t.Errorf("collection should not be skipped when DropTarget is set")
	}
	if res.TotalDocs != 2 {
		t.Errorf("TotalDocs=%d, want 2", res.TotalDocs)
	}
}

func TestCopy_DropError(t *testing.T) {
	f := newFakeFactory()
	n1 := ns("db", "c")
	src := &fakeSrcColl{docs: makeDocs(t, 2), decodeErr: -1}
	dst := &fakeDstColl{dropErr: errors.New("drop failed")}
	f.add(n1, src, dst)

	c := newWithFactory(f, engine.NopReporter{}, nil)
	res, err := c.Copy(context.Background(), engine.CopyOptions{
		Namespaces: []namespace.Namespace{n1},
		Workers:    1,
		BatchSize:  10,
		DropTarget: true,
	})
	if err != nil {
		t.Fatalf("Copy should not return a fatal error for a per-collection drop failure: %v", err)
	}
	if !res.Failed() || res.Collections[0].Err == "" {
		t.Errorf("expected per-collection Err to be recorded, got %+v", res.Collections[0])
	}
	if len(dst.allBatches()) != 0 {
		t.Errorf("no docs should be written after a drop failure")
	}
}

func TestCopy_SkipExistingNonEmpty(t *testing.T) {
	f := newFakeFactory()
	n1 := ns("db", "c")
	src := &fakeSrcColl{docs: makeDocs(t, 5), decodeErr: -1}
	dst := &fakeDstColl{countVal: 3}
	f.add(n1, src, dst)

	c := newWithFactory(f, engine.NopReporter{}, nil)
	res, err := c.Copy(context.Background(), engine.CopyOptions{
		Namespaces:           []namespace.Namespace{n1},
		Workers:              1,
		BatchSize:            10,
		SkipExistingNonEmpty: true,
	})
	if err != nil {
		t.Fatalf("Copy error: %v", err)
	}
	if !res.Collections[0].Skipped {
		t.Errorf("expected collection to be skipped")
	}
	if res.TotalDocs != 0 || len(dst.allBatches()) != 0 {
		t.Errorf("skipped collection should copy nothing, got docs=%d batches=%d", res.TotalDocs, len(dst.allBatches()))
	}
}

func TestCopy_SkipExistingButEmptyProceeds(t *testing.T) {
	f := newFakeFactory()
	n1 := ns("db", "c")
	src := &fakeSrcColl{docs: makeDocs(t, 5), decodeErr: -1}
	dst := &fakeDstColl{countVal: 0}
	f.add(n1, src, dst)

	c := newWithFactory(f, engine.NopReporter{}, nil)
	res, err := c.Copy(context.Background(), engine.CopyOptions{
		Namespaces:           []namespace.Namespace{n1},
		Workers:              1,
		BatchSize:            10,
		SkipExistingNonEmpty: true,
	})
	if err != nil {
		t.Fatalf("Copy error: %v", err)
	}
	if res.Collections[0].Skipped {
		t.Errorf("empty destination should not be skipped")
	}
	if res.TotalDocs != 5 {
		t.Errorf("TotalDocs=%d, want 5", res.TotalDocs)
	}
}

func TestCopy_SkipCountError(t *testing.T) {
	f := newFakeFactory()
	n1 := ns("db", "c")
	src := &fakeSrcColl{docs: makeDocs(t, 5), decodeErr: -1}
	dst := &fakeDstColl{countErr: errors.New("count failed")}
	f.add(n1, src, dst)

	c := newWithFactory(f, engine.NopReporter{}, nil)
	res, err := c.Copy(context.Background(), engine.CopyOptions{
		Namespaces:           []namespace.Namespace{n1},
		Workers:              1,
		BatchSize:            10,
		SkipExistingNonEmpty: true,
	})
	if err != nil {
		t.Fatalf("unexpected fatal error: %v", err)
	}
	if res.Collections[0].Err == "" {
		t.Errorf("expected count error to be recorded in Err")
	}
	if len(dst.allBatches()) != 0 {
		t.Errorf("no writes expected after a count failure")
	}
}

func TestCopy_DuplicateKeyCountsAsCopied(t *testing.T) {
	f := newFakeFactory()
	n1 := ns("db", "c")
	src := &fakeSrcColl{docs: makeDocs(t, 6), decodeErr: -1}
	// Each batch of 2: 1 inserted, 1 duplicate. Dupes must count as copied.
	dst := &fakeDstColl{writeFn: func(docs []bson.Raw) (int, int, int, error) {
		return len(docs) - 1, 1, 0, nil
	}}
	f.add(n1, src, dst)

	c := newWithFactory(f, engine.NopReporter{}, nil)
	res, err := c.Copy(context.Background(), engine.CopyOptions{
		Namespaces: []namespace.Namespace{n1},
		Workers:    1,
		BatchSize:  2,
	})
	if err != nil {
		t.Fatalf("Copy error: %v", err)
	}
	// 3 batches of 2 -> 6 copied (inserted+dupes), 0 failed.
	if res.TotalDocs != 6 || res.TotalFailed != 0 {
		t.Errorf("TotalDocs=%d TotalFailed=%d, want 6/0", res.TotalDocs, res.TotalFailed)
	}
	if res.Failed() {
		t.Errorf("duplicate keys must not mark the collection failed")
	}
}

func TestCopy_OtherWriteErrorsCountFailedButContinue(t *testing.T) {
	f := newFakeFactory()
	n1 := ns("db", "c")
	src := &fakeSrcColl{docs: makeDocs(t, 6), decodeErr: -1}
	// Each batch of 2: 1 inserted, 1 non-dup failure, nil error (continue).
	dst := &fakeDstColl{writeFn: func(docs []bson.Raw) (int, int, int, error) {
		return len(docs) - 1, 0, 1, nil
	}}
	f.add(n1, src, dst)

	c := newWithFactory(f, engine.NopReporter{}, nil)
	res, err := c.Copy(context.Background(), engine.CopyOptions{
		Namespaces: []namespace.Namespace{n1},
		Workers:    1,
		BatchSize:  2,
	})
	if err != nil {
		t.Fatalf("Copy error: %v", err)
	}
	if res.TotalDocs != 3 || res.TotalFailed != 3 {
		t.Errorf("TotalDocs=%d TotalFailed=%d, want 3/3", res.TotalDocs, res.TotalFailed)
	}
	// All three batches must have been attempted (copy did not abort).
	if got := len(dst.allBatches()); got != 3 {
		t.Errorf("attempted %d batches, want 3", got)
	}
	// Non-fatal in-batch failures are not a per-collection Err.
	if res.Collections[0].Err != "" {
		t.Errorf("unexpected Err: %q", res.Collections[0].Err)
	}
}

func TestCopy_FatalWriteErrorAbortsCollectionOnly(t *testing.T) {
	f := newFakeFactory()
	good := ns("db", "good")
	bad := ns("db", "bad")
	f.add(good, &fakeSrcColl{docs: makeDocs(t, 4), decodeErr: -1}, &fakeDstColl{})
	badDst := &fakeDstColl{writeFn: func(docs []bson.Raw) (int, int, int, error) {
		return 0, 0, len(docs), errors.New("connection reset")
	}}
	f.add(bad, &fakeSrcColl{docs: makeDocs(t, 4), decodeErr: -1}, badDst)

	c := newWithFactory(f, engine.NopReporter{}, nil)
	res, err := c.Copy(context.Background(), engine.CopyOptions{
		Namespaces: []namespace.Namespace{good, bad},
		Workers:    2,
		BatchSize:  2,
	})
	if err != nil {
		t.Fatalf("per-collection failure must not be returned as a fatal error: %v", err)
	}
	if !res.Failed() {
		t.Errorf("expected Failed()=true")
	}
	// The bad collection aborts after its first batch; the good one completes.
	var goodRes, badRes engine.CollectionCopyResult
	for _, cr := range res.Collections {
		switch cr.Namespace {
		case good:
			goodRes = cr
		case bad:
			badRes = cr
		}
	}
	if goodRes.Err != "" || goodRes.DocsCopied != 4 {
		t.Errorf("good collection = %+v, want 4 copied, no err", goodRes)
	}
	if badRes.Err == "" {
		t.Errorf("bad collection should have Err set")
	}
	if len(badDst.allBatches()) != 1 {
		t.Errorf("bad collection should abort after the first batch, got %d batches", len(badDst.allBatches()))
	}
}

func TestCopy_FindErrorRecorded(t *testing.T) {
	f := newFakeFactory()
	good := ns("db", "good")
	bad := ns("db", "bad")
	f.add(good, &fakeSrcColl{docs: makeDocs(t, 3), decodeErr: -1}, &fakeDstColl{})
	f.add(bad, &fakeSrcColl{findErr: errors.New("cursor open failed"), decodeErr: -1}, &fakeDstColl{})

	c := newWithFactory(f, engine.NopReporter{}, nil)
	res, err := c.Copy(context.Background(), engine.CopyOptions{
		Namespaces: []namespace.Namespace{good, bad},
		Workers:    2,
		BatchSize:  2,
	})
	if err != nil {
		t.Fatalf("unexpected fatal error: %v", err)
	}
	if !res.Failed() {
		t.Errorf("expected Failed()=true")
	}
	if res.TotalDocs != 3 {
		t.Errorf("good collection should still be fully copied, TotalDocs=%d want 3", res.TotalDocs)
	}
}

func TestCopy_DecodeErrorRecorded(t *testing.T) {
	f := newFakeFactory()
	n1 := ns("db", "c")
	// Fail decoding the 3rd document (index 2).
	src := &fakeSrcColl{docs: makeDocs(t, 5), decodeErr: 2}
	dst := &fakeDstColl{}
	f.add(n1, src, dst)

	c := newWithFactory(f, engine.NopReporter{}, nil)
	res, err := c.Copy(context.Background(), engine.CopyOptions{
		Namespaces: []namespace.Namespace{n1},
		Workers:    1,
		BatchSize:  1,
	})
	if err != nil {
		t.Fatalf("unexpected fatal error: %v", err)
	}
	if res.Collections[0].Err == "" {
		t.Errorf("expected decode error to be recorded")
	}
	// Two docs copied before the decode failure on the third.
	if res.Collections[0].DocsCopied != 2 {
		t.Errorf("DocsCopied=%d, want 2", res.Collections[0].DocsCopied)
	}
}

func TestCopy_CursorErrRecorded(t *testing.T) {
	f := newFakeFactory()
	n1 := ns("db", "c")
	// A source whose cursor reports an error after yielding its documents.
	f.src[n1.String()] = &erroringCursorSrc{docs: makeDocs(t, 2)}
	f.dst[n1.String()] = &fakeDstColl{}

	c := newWithFactory(f, engine.NopReporter{}, nil)
	res, err := c.Copy(context.Background(), engine.CopyOptions{
		Namespaces: []namespace.Namespace{n1},
		Workers:    1,
		BatchSize:  1,
	})
	if err != nil {
		t.Fatalf("unexpected fatal error: %v", err)
	}
	if res.Collections[0].Err == "" {
		t.Errorf("expected cursor error to be recorded")
	}
}

// erroringCursorSrc yields all docs then reports a cursor error.
type erroringCursorSrc struct {
	docs []bson.Raw
}

func (s *erroringCursorSrc) count(context.Context) (int64, error) { return int64(len(s.docs)), nil }
func (s *erroringCursorSrc) listIndexes(context.Context) ([]bson.Raw, error) {
	return nil, nil
}
func (s *erroringCursorSrc) find(context.Context, int) (docCursor, error) {
	return &erroringCursor{docs: s.docs}, nil
}

type erroringCursor struct {
	docs []bson.Raw
	pos  int
	cur  bson.Raw
}

func (c *erroringCursor) Next(context.Context) bool {
	if c.pos >= len(c.docs) {
		return false
	}
	c.cur = c.docs[c.pos]
	c.pos++
	return true
}
func (c *erroringCursor) Decode(v *bson.Raw) error    { *v = c.cur; return nil }
func (c *erroringCursor) Err() error                  { return errors.New("cursor network error") }
func (c *erroringCursor) Close(context.Context) error { return nil }

func TestCopy_CopyIndexes(t *testing.T) {
	f := newFakeFactory()
	n1 := ns("db", "c")
	specs := []bson.Raw{
		mustRaw(t, bson.D{{Key: "key", Value: bson.D{{Key: "_id", Value: int32(1)}}}, {Key: "name", Value: "_id_"}}),
		mustRaw(t, bson.D{{Key: "key", Value: bson.D{{Key: "email", Value: int32(1)}}}, {Key: "name", Value: "email_1"}, {Key: "unique", Value: true}}),
		mustRaw(t, bson.D{{Key: "key", Value: bson.D{{Key: "age", Value: int32(-1)}}}, {Key: "name", Value: "age_-1"}}),
	}
	src := &fakeSrcColl{docs: makeDocs(t, 2), indexes: specs, decodeErr: -1}
	dst := &fakeDstColl{}
	f.add(n1, src, dst)

	c := newWithFactory(f, engine.NopReporter{}, nil)
	res, err := c.Copy(context.Background(), engine.CopyOptions{
		Namespaces:  []namespace.Namespace{n1},
		Workers:     1,
		BatchSize:   10,
		CopyIndexes: true,
	})
	if err != nil {
		t.Fatalf("Copy error: %v", err)
	}
	// _id_ must be skipped; the other two recreated.
	if res.Collections[0].IndexesCopied != 2 {
		t.Errorf("IndexesCopied=%d, want 2", res.Collections[0].IndexesCopied)
	}
	dst.mu.Lock()
	created := dst.createdModels
	dst.mu.Unlock()
	if len(created) != 2 {
		t.Fatalf("createIndexes received %d models, want 2", len(created))
	}
	for _, m := range created {
		keys := m.Keys.(bson.D)
		if keys[0].Key == "_id" {
			t.Errorf("_id_ index should never be recreated")
		}
	}
}

func TestCopy_CopyIndexesListError(t *testing.T) {
	f := newFakeFactory()
	n1 := ns("db", "c")
	src := &fakeSrcColl{docs: makeDocs(t, 2), indexErr: errors.New("listIndexes denied"), decodeErr: -1}
	dst := &fakeDstColl{}
	f.add(n1, src, dst)

	c := newWithFactory(f, engine.NopReporter{}, nil)
	res, err := c.Copy(context.Background(), engine.CopyOptions{
		Namespaces:  []namespace.Namespace{n1},
		Workers:     1,
		BatchSize:   10,
		CopyIndexes: true,
	})
	if err != nil {
		t.Fatalf("unexpected fatal error: %v", err)
	}
	// Data copy succeeded; index listing failure is recorded in Err.
	if res.TotalDocs != 2 {
		t.Errorf("TotalDocs=%d, want 2", res.TotalDocs)
	}
	if res.Collections[0].Err == "" {
		t.Errorf("expected index list error to be recorded in Err")
	}
}

func TestCopy_ProgressReportsTotals(t *testing.T) {
	f := newFakeFactory()
	n1 := ns("db", "c")
	src := &fakeSrcColl{docs: makeDocs(t, 5), decodeErr: -1}
	dst := &fakeDstColl{}
	f.add(n1, src, dst)

	rep := &recordReporter{}
	c := newWithFactory(f, rep, nil)
	_, err := c.Copy(context.Background(), engine.CopyOptions{
		Namespaces: []namespace.Namespace{n1},
		Workers:    1,
		BatchSize:  2,
	})
	if err != nil {
		t.Fatalf("Copy error: %v", err)
	}
	calls := rep.progressCalls()
	if len(calls) == 0 {
		t.Fatalf("expected progress to be reported")
	}
	last := calls[len(calls)-1]
	if last.done != 5 || last.total != 5 {
		t.Errorf("final progress = (%d/%d), want (5/5)", last.done, last.total)
	}
}

func TestCopy_ProgressTotalUnknownOnCountError(t *testing.T) {
	f := newFakeFactory()
	n1 := ns("db", "c")
	// Source count errors -> total should be reported as -1.
	src := &fakeSrcColl{docs: makeDocs(t, 3), countErr: errors.New("count denied"), decodeErr: -1}
	dst := &fakeDstColl{}
	f.add(n1, src, dst)

	rep := &recordReporter{}
	c := newWithFactory(f, rep, nil)
	res, err := c.Copy(context.Background(), engine.CopyOptions{
		Namespaces: []namespace.Namespace{n1},
		Workers:    1,
		BatchSize:  1,
	})
	if err != nil {
		t.Fatalf("Copy error: %v", err)
	}
	if res.TotalDocs != 3 {
		t.Errorf("TotalDocs=%d, want 3", res.TotalDocs)
	}
	for _, p := range rep.progressCalls() {
		if p.total != -1 {
			t.Errorf("progress total = %d, want -1 when count is unavailable", p.total)
		}
	}
}

func TestCopy_ReadBatchSizeThreaded(t *testing.T) {
	f := newFakeFactory()
	n1 := ns("db", "c")
	src := &fakeSrcColl{docs: makeDocs(t, 2), decodeErr: -1}
	f.add(n1, src, &fakeDstColl{})

	c := newWithFactory(f, engine.NopReporter{}, nil)
	_, err := c.Copy(context.Background(), engine.CopyOptions{
		Namespaces:    []namespace.Namespace{n1},
		Workers:       1,
		BatchSize:     10,
		ReadBatchSize: 250,
	})
	if err != nil {
		t.Fatalf("Copy error: %v", err)
	}
	src.mu.Lock()
	got := src.findBatch
	src.mu.Unlock()
	if got != 250 {
		t.Errorf("find batch size = %d, want 250", got)
	}
}

func TestCopy_Concurrency(t *testing.T) {
	const workers = 3
	f := newFakeFactory()
	g := &gate{arrived: make(chan struct{}, workers), release: make(chan struct{})}
	names := make([]namespace.Namespace, workers)
	for i := 0; i < workers; i++ {
		n := ns("db", string(rune('a'+i)))
		names[i] = n
		f.add(n, &fakeSrcColl{docs: makeDocs(t, 2), gate: g, decodeErr: -1}, &fakeDstColl{})
	}

	c := newWithFactory(f, engine.NopReporter{}, nil)
	done := make(chan engine.CopyResult, 1)
	go func() {
		res, _ := c.Copy(context.Background(), engine.CopyOptions{
			Namespaces: names,
			Workers:    workers,
			BatchSize:  10,
		})
		done <- res
	}()

	// All `workers` collections must reach the gate simultaneously, proving true
	// parallelism. If the pool serialized, fewer than `workers` would arrive and
	// this loop would time out.
	deadline := time.After(2 * time.Second)
	for i := 0; i < workers; i++ {
		select {
		case <-g.arrived:
		case <-deadline:
			t.Fatalf("only %d of %d collections reached the gate; not running concurrently", i, workers)
		}
	}
	close(g.release)

	select {
	case res := <-done:
		if res.TotalDocs != int64(workers*2) {
			t.Errorf("TotalDocs=%d, want %d", res.TotalDocs, workers*2)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Copy did not finish after releasing the gate")
	}
}

func TestCopy_ContextCancelledUpfront(t *testing.T) {
	f := newFakeFactory()
	n1 := ns("db", "c")
	f.add(n1, &fakeSrcColl{docs: makeDocs(t, 3), decodeErr: -1}, &fakeDstColl{})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	c := newWithFactory(f, engine.NopReporter{}, nil)
	_, err := c.Copy(ctx, engine.CopyOptions{
		Namespaces: []namespace.Namespace{n1},
		Workers:    2,
		BatchSize:  2,
	})
	if err == nil {
		t.Fatalf("expected a fatal error for a cancelled context")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("error = %v, want context.Canceled", err)
	}
}

func TestCopy_ContextCancelledMidCopyStopsEarly(t *testing.T) {
	f := newFakeFactory()
	n1 := ns("db", "c")
	ctx, cancel := context.WithCancel(context.Background())
	// Deliver 2 docs (flushed one-per-batch), then cancel before the rest.
	src := &fakeSrcColl{
		docs:       makeDocs(t, 100),
		respectCtx: true,
		cancel:     cancel,
		cancelAt:   2,
		decodeErr:  -1,
	}
	dst := &fakeDstColl{}
	f.add(n1, src, dst)

	c := newWithFactory(f, engine.NopReporter{}, nil)
	res, err := c.Copy(ctx, engine.CopyOptions{
		Namespaces: []namespace.Namespace{n1},
		Workers:    1,
		BatchSize:  1,
	})
	if err == nil {
		t.Fatalf("expected a fatal error after mid-copy cancellation")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("error = %v, want context.Canceled", err)
	}
	// It must have stopped well before copying all 100 documents.
	if res.TotalDocs >= 100 {
		t.Errorf("copy did not stop early: TotalDocs=%d", res.TotalDocs)
	}
	if res.Collections[0].Err == "" {
		t.Errorf("expected per-collection Err to record the cancellation")
	}
}

func TestCopy_NilReporterAndLoggerDoNotPanic(t *testing.T) {
	f := newFakeFactory()
	n1 := ns("db", "c")
	f.add(n1, &fakeSrcColl{docs: makeDocs(t, 2), decodeErr: -1}, &fakeDstColl{})

	// newWithFactory should install no-op reporter/logger for nil inputs.
	c := newWithFactory(f, nil, nil)
	if _, err := c.Copy(context.Background(), engine.CopyOptions{
		Namespaces: []namespace.Namespace{n1},
		Workers:    1,
		BatchSize:  1,
	}); err != nil {
		t.Fatalf("Copy error: %v", err)
	}
}

// TestCopy_UsesCountingReporter exercises the shared progress.CountingReporter to
// ensure the copier integrates with the real reporter implementation.
func TestCopy_UsesCountingReporter(t *testing.T) {
	f := newFakeFactory()
	n1 := ns("db", "c")
	f.add(n1, &fakeSrcColl{docs: makeDocs(t, 4), decodeErr: -1}, &fakeDstColl{})

	rep := progress.NewCountingReporter()
	c := newWithFactory(f, rep, nil)
	if _, err := c.Copy(context.Background(), engine.CopyOptions{
		Namespaces: []namespace.Namespace{n1},
		Workers:    1,
		BatchSize:  2,
	}); err != nil {
		t.Fatalf("Copy error: %v", err)
	}
	if rep.Events.Load() == 0 {
		t.Errorf("expected per-collection events to be reported")
	}
	if rep.ProgressN.Load() == 0 {
		t.Errorf("expected progress to be reported")
	}
}

func TestNew_SmokeConstructsCopier(t *testing.T) {
	// New must not panic with nil clients when no Copy is performed; it only wires
	// a factory. This guards the public constructor's shape.
	c := New(nil, nil, nil, nil)
	if c == nil {
		t.Fatalf("New returned nil")
	}
	if c.reporter == nil || c.log == nil || c.factory == nil {
		t.Errorf("New left a dependency unset: %+v", c)
	}
}
