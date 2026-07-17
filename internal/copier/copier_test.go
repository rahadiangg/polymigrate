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

	"github.com/rahadiangg/polymigrate/internal/engine"
	"github.com/rahadiangg/polymigrate/internal/namespace"
	"github.com/rahadiangg/polymigrate/internal/progress"
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

	mu           sync.Mutex
	lastCursor   *fakeCursor
	findBatch    int    // records the batch size the copier requested
	findFromLast []byte // records the resume boundary passed to findFrom
	findFromCall bool   // records that findFrom (not find) was used
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

// findFrom mirrors find but filters the fake's documents to those whose (int32)
// _id is strictly greater than the resume boundary encoded in lastID, matching
// the {_id: {$gt: lastID}} semantics of the real adapter.
func (s *fakeSrcColl) findFrom(ctx context.Context, batch int, lastID []byte) (docCursor, error) {
	s.gate.arrive(ctx)
	if s.findErr != nil {
		return nil, s.findErr
	}
	docs := s.docs
	if len(lastID) > 0 {
		boundary, ok := idFromBytes(lastID)
		if !ok {
			return nil, errors.New("fake findFrom: undecodable resume boundary")
		}
		filtered := make([]bson.Raw, 0, len(s.docs))
		for _, d := range s.docs {
			if id, ok := docIntID(d); ok && id > boundary {
				filtered = append(filtered, d)
			}
		}
		docs = filtered
	}
	cur := &fakeCursor{
		docs:        docs,
		decodeErrAt: s.decodeErr,
		respectCtx:  s.respectCtx,
		cancel:      s.cancel,
		cancelAt:    s.cancelAt,
	}
	s.mu.Lock()
	s.lastCursor = cur
	s.findBatch = batch
	s.findFromLast = append([]byte(nil), lastID...)
	s.findFromCall = true
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
	upsertFn  func(docs []bson.Raw) (written, failed int, err error)
	// upsertStore, when non-nil, makes writeBatchUpsert idempotent: docs are keyed
	// by their (int32) _id so re-copying an overlapping batch does not duplicate.
	upsertStore map[int32]bson.Raw

	mu            sync.Mutex
	dropped       bool
	batches       [][]bson.Raw
	upsertBatches [][]bson.Raw
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

func (d *fakeDstColl) writeBatchUpsert(ctx context.Context, docs []bson.Raw) (int, int, error) {
	cp := make([]bson.Raw, len(docs))
	copy(cp, docs)
	d.mu.Lock()
	d.upsertBatches = append(d.upsertBatches, cp)
	if d.upsertStore != nil {
		for _, doc := range cp {
			if id, ok := docIntID(doc); ok {
				d.upsertStore[id] = doc
			}
		}
	}
	d.mu.Unlock()
	if d.upsertFn != nil {
		return d.upsertFn(cp)
	}
	return len(docs), 0, nil
}

// allUpsertBatches returns a copy of the recorded upsert batches for assertion.
func (d *fakeDstColl) allUpsertBatches() [][]bson.Raw {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([][]bson.Raw, len(d.upsertBatches))
	copy(out, d.upsertBatches)
	return out
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

// docIntID reads the int32 _id of a test document.
func docIntID(d bson.Raw) (int32, bool) {
	return d.Lookup("_id").Int32OK()
}

// idFromBytes reads the int32 _id out of the {"_id": value} boundary encoding
// produced by extractID (the format the copier hands to OnProgress / ResumeFrom).
func idFromBytes(b []byte) (int32, bool) {
	return bson.Raw(b).Lookup("_id").Int32OK()
}

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

// mustExtractID returns the resume-boundary encoding of a document's _id or fails
// the test. It is the same encoding the copier hands to OnProgress and reads back
// from ResumeFrom.
func mustExtractID(t *testing.T, d bson.Raw) []byte {
	t.Helper()
	b, err := extractID(d)
	if err != nil {
		t.Fatalf("extractID: %v", err)
	}
	return b
}

// progressRec captures OnProgress callbacks. It is safe for concurrent use since
// the copier may invoke OnProgress from worker goroutines.
type progressRec struct {
	mu    sync.Mutex
	calls []onProgressCall
}

type onProgressCall struct {
	ns         namespace.Namespace
	lastID     []byte
	docsCopied int64
	done       bool
}

func (p *progressRec) fn(ns namespace.Namespace, lastID []byte, docsCopied int64, done bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls = append(p.calls, onProgressCall{
		ns:         ns,
		lastID:     append([]byte(nil), lastID...),
		docsCopied: docsCopied,
		done:       done,
	})
}

func (p *progressRec) snapshot() []onProgressCall {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]onProgressCall, len(p.calls))
	copy(out, p.calls)
	return out
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
	// Per-document failures do not abort the collection, but they DO mark it
	// failed so the run cannot report success and the collection stays resumable.
	if res.Collections[0].Err == "" {
		t.Error("expected a per-collection Err summarizing the failed documents")
	}
	if res.Collections[0].DocsFailed != 3 {
		t.Errorf("DocsFailed=%d, want 3", res.Collections[0].DocsFailed)
	}
	if !res.Failed() {
		t.Error("CopyResult.Failed() should be true when documents failed")
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
func (s *erroringCursorSrc) findFrom(context.Context, int, []byte) (docCursor, error) {
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

// -----------------------------------------------------------------------------
// Resume / resilience tests
// -----------------------------------------------------------------------------

// TestCopy_SkipNamespacesSkipsEntirely verifies a namespace already complete on a
// prior run is skipped: marked Skipped, no source cursor opened, no writes, and
// OnProgress is not re-invoked.
func TestCopy_SkipNamespacesSkipsEntirely(t *testing.T) {
	f := newFakeFactory()
	n1 := ns("shop", "orders")
	src := &fakeSrcColl{docs: makeDocs(t, 5), decodeErr: -1}
	// A non-empty destination represents an intact target: the "already complete"
	// checkpoint is trustworthy, so the collection is skipped.
	dst := &fakeDstColl{countVal: 5}
	f.add(n1, src, dst)

	rec := &progressRec{}
	c := newWithFactory(f, engine.NopReporter{}, nil)
	res, err := c.Copy(context.Background(), engine.CopyOptions{
		Namespaces:     []namespace.Namespace{n1},
		Workers:        1,
		BatchSize:      2,
		SkipNamespaces: map[string]bool{n1.String(): true},
		OnProgress:     rec.fn,
	})
	if err != nil {
		t.Fatalf("Copy error: %v", err)
	}
	if !res.Collections[0].Skipped {
		t.Errorf("expected collection to be marked Skipped")
	}
	if res.Collections[0].Err != "" {
		t.Errorf("skipped namespace should have no Err, got %q", res.Collections[0].Err)
	}
	if res.TotalDocs != 0 {
		t.Errorf("skipped namespace should copy nothing, TotalDocs=%d", res.TotalDocs)
	}
	if len(dst.allBatches()) != 0 || len(dst.allUpsertBatches()) != 0 {
		t.Errorf("skipped namespace must not write any batch")
	}
	src.mu.Lock()
	opened := src.lastCursor != nil || src.findFromCall
	src.mu.Unlock()
	if opened {
		t.Errorf("skipped namespace must not open a source cursor")
	}
	if calls := rec.snapshot(); len(calls) != 0 {
		t.Errorf("OnProgress must not be called for a skipped namespace, got %d calls", len(calls))
	}
}

// TestCopy_SkipNamespacesReCopiesWhenDestinationEmpty verifies the guard against a
// stale checkpoint outliving a rebuilt (empty) destination: a namespace marked
// complete is re-copied — not skipped — when the destination collection is empty.
func TestCopy_SkipNamespacesReCopiesWhenDestinationEmpty(t *testing.T) {
	f := newFakeFactory()
	n1 := ns("shop", "orders")
	src := &fakeSrcColl{docs: makeDocs(t, 5), decodeErr: -1}
	dst := &fakeDstColl{countVal: 0} // rebuilt / empty destination
	f.add(n1, src, dst)

	c := newWithFactory(f, engine.NopReporter{}, nil)
	res, err := c.Copy(context.Background(), engine.CopyOptions{
		Namespaces:     []namespace.Namespace{n1},
		Workers:        1,
		BatchSize:      2,
		SkipNamespaces: map[string]bool{n1.String(): true},
	})
	if err != nil {
		t.Fatalf("Copy error: %v", err)
	}
	if res.Collections[0].Skipped {
		t.Error("must NOT skip a complete namespace when the destination is empty")
	}
	if res.TotalDocs != 5 {
		t.Errorf("expected all 5 docs re-copied, got %d", res.TotalDocs)
	}
}

// TestCopy_BoundaryFreezesAfterFailure is the regression test for the critical
// resume-boundary bug: once an earlier batch leaves a hole (a per-document
// failure), a later CLEAN batch must NOT advance the checkpoint boundary past the
// hole — otherwise a resume ({_id:{$gt:boundary}}) would skip the failed document.
func TestCopy_BoundaryFreezesAfterFailure(t *testing.T) {
	f := newFakeFactory()
	n1 := ns("db", "c")
	src := &fakeSrcColl{docs: makeDocs(t, 6), decodeErr: -1} // _ids 0..5
	// Batches of 2: [0,1], [2,3], [4,5]. Fail the SECOND batch only.
	var call int
	dst := &fakeDstColl{writeFn: func(docs []bson.Raw) (int, int, int, error) {
		call++
		if call == 2 {
			return len(docs) - 1, 0, 1, nil // one doc fails, no fatal error
		}
		return len(docs), 0, 0, nil
	}}
	f.add(n1, src, dst)

	rec := &progressRec{}
	res, err := newWithFactory(f, engine.NopReporter{}, nil).Copy(context.Background(), engine.CopyOptions{
		Namespaces: []namespace.Namespace{n1},
		Workers:    1,
		BatchSize:  2,
		OnProgress: rec.fn,
	})
	if err != nil {
		t.Fatalf("Copy error: %v", err)
	}
	// The collection had a failure: not complete, marked failed, boundary frozen.
	calls := rec.snapshot()
	if len(calls) == 0 {
		t.Fatal("expected OnProgress calls")
	}
	final := calls[len(calls)-1]
	if final.done {
		t.Error("collection with a failed document must not signal done=true")
	}
	// The frozen boundary must be the last hole-free _id (1), never 3 or 5.
	got, ok := idFromBytes(final.lastID)
	if !ok {
		t.Fatalf("could not decode boundary lastID")
	}
	if got != 1 {
		t.Errorf("resume boundary = _id %d, want 1 (frozen before the hole); a value >1 would skip the failed doc on resume", got)
	}
	if res.Collections[0].DocsFailed != 1 {
		t.Errorf("DocsFailed=%d, want 1", res.Collections[0].DocsFailed)
	}
}

// TestCopy_ResumeFromFiltersAndUpserts verifies a resumed namespace reads from
// {_id: {$gt: lastID}} via findFrom and writes through the idempotent upsert path
// (never the InsertMany path).
func TestCopy_ResumeFromFiltersAndUpserts(t *testing.T) {
	f := newFakeFactory()
	n1 := ns("db", "c")
	src := &fakeSrcColl{docs: makeDocs(t, 6), decodeErr: -1} // _ids 0..5
	dst := &fakeDstColl{}
	f.add(n1, src, dst)

	boundary := mustExtractID(t, doc(t, 2)) // resume after _id=2

	c := newWithFactory(f, engine.NopReporter{}, nil)
	res, err := c.Copy(context.Background(), engine.CopyOptions{
		Namespaces: []namespace.Namespace{n1},
		Workers:    1,
		BatchSize:  2,
		ResumeFrom: map[string][]byte{n1.String(): boundary},
	})
	if err != nil {
		t.Fatalf("Copy error: %v", err)
	}
	if res.TotalDocs != 3 {
		t.Errorf("TotalDocs=%d, want 3 (only _id>2)", res.TotalDocs)
	}

	src.mu.Lock()
	fromCall, fromLast := src.findFromCall, src.findFromLast
	src.mu.Unlock()
	if !fromCall {
		t.Fatalf("resume must open the cursor via findFrom, not find")
	}
	if !bytes.Equal(fromLast, boundary) {
		t.Errorf("findFrom boundary = %x, want %x", fromLast, boundary)
	}

	if len(dst.allBatches()) != 0 {
		t.Errorf("resume must not use the InsertMany writeBatch path")
	}
	ub := dst.allUpsertBatches()
	if got := batchLens(ub); !intsEqual(got, []int{2, 1}) {
		t.Errorf("upsert batch sizes = %v, want [2 1]", got)
	}
	var gotIDs []int32
	for _, b := range ub {
		for _, d := range b {
			id, _ := docIntID(d)
			gotIDs = append(gotIDs, id)
		}
	}
	want := []int32{3, 4, 5}
	if len(gotIDs) != len(want) {
		t.Fatalf("copied ids = %v, want %v", gotIDs, want)
	}
	for i := range want {
		if gotIDs[i] != want[i] {
			t.Errorf("copied id[%d]=%d, want %d", i, gotIDs[i], want[i])
		}
	}
}

// TestCopy_ResumeUpsertIsIdempotent proves re-copying the boundary batch is safe:
// against a destination pre-seeded with docs that overlap the resume range, and
// across two resume runs, the upsert path never duplicates.
func TestCopy_ResumeUpsertIsIdempotent(t *testing.T) {
	f := newFakeFactory()
	n1 := ns("db", "c")
	src := &fakeSrcColl{docs: makeDocs(t, 6), decodeErr: -1} // _ids 0..5
	// A crash left docs 3 and 4 already written even though the checkpoint only
	// recorded _id=2; re-copying that overlap must not duplicate.
	dst := &fakeDstColl{upsertStore: map[int32]bson.Raw{
		3: doc(t, 3),
		4: doc(t, 4),
	}}
	f.add(n1, src, dst)

	boundary := mustExtractID(t, doc(t, 2))
	opts := engine.CopyOptions{
		Namespaces: []namespace.Namespace{n1},
		Workers:    1,
		BatchSize:  2,
		ResumeFrom: map[string][]byte{n1.String(): boundary},
	}
	c := newWithFactory(f, engine.NopReporter{}, nil)
	if _, err := c.Copy(context.Background(), opts); err != nil {
		t.Fatalf("Copy error: %v", err)
	}
	// A second resume of the same boundary must remain idempotent.
	if _, err := c.Copy(context.Background(), opts); err != nil {
		t.Fatalf("second Copy error: %v", err)
	}

	dst.mu.Lock()
	n := len(dst.upsertStore)
	_, has3 := dst.upsertStore[3]
	_, has4 := dst.upsertStore[4]
	_, has5 := dst.upsertStore[5]
	dst.mu.Unlock()
	if n != 3 {
		t.Errorf("destination holds %d docs, want 3 (idempotent upsert over the overlap)", n)
	}
	if !has3 || !has4 || !has5 {
		t.Errorf("destination must hold exactly _id 3,4,5 after resume")
	}
}

// TestCopy_OnProgressPerBatchAndCompletion verifies OnProgress fires once per
// written batch with the batch's last _id and running count (done=false), then a
// final time at completion (done=true).
func TestCopy_OnProgressPerBatchAndCompletion(t *testing.T) {
	f := newFakeFactory()
	n1 := ns("db", "c")
	src := &fakeSrcColl{docs: makeDocs(t, 5), decodeErr: -1} // _ids 0..4
	dst := &fakeDstColl{}
	f.add(n1, src, dst)

	rec := &progressRec{}
	c := newWithFactory(f, engine.NopReporter{}, nil)
	if _, err := c.Copy(context.Background(), engine.CopyOptions{
		Namespaces: []namespace.Namespace{n1},
		Workers:    1,
		BatchSize:  2,
		OnProgress: rec.fn,
	}); err != nil {
		t.Fatalf("Copy error: %v", err)
	}

	calls := rec.snapshot()
	// Batches [0,1],[2,3],[4] -> three done=false callbacks, then one done=true.
	if len(calls) != 4 {
		t.Fatalf("OnProgress called %d times, want 4: %+v", len(calls), calls)
	}
	wants := []struct {
		lastID     int32
		docsCopied int64
		done       bool
	}{
		{1, 2, false},
		{3, 4, false},
		{4, 5, false},
		{4, 5, true},
	}
	for i, w := range wants {
		got := calls[i]
		if got.ns != n1 {
			t.Errorf("call[%d].ns=%v, want %v", i, got.ns, n1)
		}
		id, ok := idFromBytes(got.lastID)
		if !ok {
			t.Fatalf("call[%d] lastID not decodable: %x", i, got.lastID)
		}
		if id != w.lastID || got.docsCopied != w.docsCopied || got.done != w.done {
			t.Errorf("call[%d]=(id=%d docs=%d done=%v), want (id=%d docs=%d done=%v)",
				i, id, got.docsCopied, got.done, w.lastID, w.docsCopied, w.done)
		}
	}
}

// TestCopy_FreshCopyUsesInsertPathNotResume guards that a plain copy (no
// SkipNamespaces / ResumeFrom) is unchanged: it reads via find and writes via the
// InsertMany writeBatch path, never touching the resume paths.
func TestCopy_FreshCopyUsesInsertPathNotResume(t *testing.T) {
	f := newFakeFactory()
	n1 := ns("db", "c")
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
	if res.TotalDocs != 5 {
		t.Errorf("TotalDocs=%d, want 5", res.TotalDocs)
	}
	src.mu.Lock()
	fromCall := src.findFromCall
	src.mu.Unlock()
	if fromCall {
		t.Errorf("fresh copy must not use findFrom")
	}
	if len(dst.allUpsertBatches()) != 0 {
		t.Errorf("fresh copy must not use the upsert path")
	}
	if got := batchLens(dst.allBatches()); !intsEqual(got, []int{2, 2, 1}) {
		t.Errorf("fresh copy batch sizes = %v, want [2 2 1]", got)
	}
}

// TestExtractID_RoundTripsAllTypes verifies the resume-boundary encoding preserves
// the _id's BSON type and bytes across the id shapes a real deployment uses.
func TestExtractID_RoundTripsAllTypes(t *testing.T) {
	oid := bson.NewObjectID()
	cases := []struct {
		name string
		id   any
	}{
		{"int32", int32(7)},
		{"int64", int64(1) << 40},
		{"string", "customer-42"},
		{"objectID", oid},
		{"compound", bson.D{{Key: "a", Value: int32(1)}, {Key: "b", Value: "x"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			raw := mustRaw(t, bson.D{{Key: "_id", Value: tc.id}, {Key: "n", Value: int32(1)}})
			got, err := extractID(raw)
			if err != nil {
				t.Fatalf("extractID error: %v", err)
			}
			orig := raw.Lookup("_id")
			rebuilt := bson.Raw(got).Lookup("_id")
			if rebuilt.Type != orig.Type {
				t.Errorf("type = %v, want %v", rebuilt.Type, orig.Type)
			}
			if !bytes.Equal(rebuilt.Value, orig.Value) {
				t.Errorf("value bytes mismatch for %s", tc.name)
			}
		})
	}
}

// TestExtractID_MissingIDErrors verifies a document without _id is rejected rather
// than silently producing a bogus boundary.
func TestExtractID_MissingIDErrors(t *testing.T) {
	raw := mustRaw(t, bson.D{{Key: "x", Value: int32(1)}})
	if _, err := extractID(raw); err == nil {
		t.Fatalf("expected an error for a document without _id")
	}
}

// TestExtractID_StableForSameID verifies the boundary depends only on _id, not on
// the rest of the document.
func TestExtractID_StableForSameID(t *testing.T) {
	a := mustRaw(t, bson.D{{Key: "_id", Value: int32(5)}, {Key: "n", Value: int32(1)}})
	b := mustRaw(t, bson.D{{Key: "_id", Value: int32(5)}, {Key: "n", Value: int32(999)}})
	ga := mustExtractID(t, a)
	gb := mustExtractID(t, b)
	if !bytes.Equal(ga, gb) {
		t.Errorf("extractID not stable for equal _id: %x vs %x", ga, gb)
	}
}
