package verify

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"sync"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/rahadiangg/polymigrate/internal/engine"
	"github.com/rahadiangg/polymigrate/internal/namespace"
)

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// fakeStore is an in-memory store for orchestration tests. It mirrors the real
// adapter's semantics: counts reflect stored docs, sampleIDs returns _ids
// ordered by idKey, and fetchByIDs returns only the requested, present docs.
type fakeStore struct {
	mu      sync.Mutex
	docs    map[string][]bson.Raw // ns -> documents (each containing _id)
	indexes map[string][]bson.Raw // ns -> raw index specs

	// Optional error injection, keyed by namespace string.
	countErr  map[string]error
	indexErr  map[string]error
	sampleErr map[string]error
	fetchErr  map[string]error
}

func newFakeStore() *fakeStore {
	return &fakeStore{
		docs:      map[string][]bson.Raw{},
		indexes:   map[string][]bson.Raw{},
		countErr:  map[string]error{},
		indexErr:  map[string]error{},
		sampleErr: map[string]error{},
		fetchErr:  map[string]error{},
	}
}

func (f *fakeStore) addDoc(ns namespace.Namespace, d bson.Raw) {
	f.docs[ns.String()] = append(f.docs[ns.String()], d)
}

func (f *fakeStore) setIndexes(ns namespace.Namespace, specs ...bson.Raw) {
	f.indexes[ns.String()] = specs
}

func (f *fakeStore) countDocuments(ctx context.Context, ns namespace.Namespace) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.countErr[ns.String()]; err != nil {
		return 0, err
	}
	return int64(len(f.docs[ns.String()])), nil
}

func (f *fakeStore) listIndexes(ctx context.Context, ns namespace.Namespace) ([]bson.Raw, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.indexErr[ns.String()]; err != nil {
		return nil, err
	}
	return f.indexes[ns.String()], nil
}

func (f *fakeStore) sampleIDs(ctx context.Context, ns namespace.Namespace, n int) ([]bson.RawValue, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.sampleErr[ns.String()]; err != nil {
		return nil, err
	}
	docs := f.docs[ns.String()]
	ids := make([]bson.RawValue, 0, len(docs))
	for _, d := range docs {
		ids = append(ids, copyRawValue(d.Lookup("_id")))
	}
	sort.Slice(ids, func(i, j int) bool { return idKey(ids[i]) < idKey(ids[j]) })
	if n < len(ids) {
		ids = ids[:n]
	}
	return ids, nil
}

func (f *fakeStore) fetchByIDs(ctx context.Context, ns namespace.Namespace, ids []bson.RawValue) (map[string]bson.Raw, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := f.fetchErr[ns.String()]; err != nil {
		return nil, err
	}
	byKey := map[string]bson.Raw{}
	for _, d := range f.docs[ns.String()] {
		byKey[idKey(d.Lookup("_id"))] = d
	}
	want := map[string]bool{}
	for _, id := range ids {
		want[idKey(id)] = true
	}
	out := map[string]bson.Raw{}
	for k, d := range byKey {
		if want[k] {
			out[k] = d
		}
	}
	return out, nil
}

// idxSpec builds a minimal raw index spec.
func idxSpec(t *testing.T, name string, key bson.D, extra ...bson.E) bson.Raw {
	d := bson.D{{Key: "key", Value: key}, {Key: "name", Value: name}}
	d = append(d, extra...)
	return mustRaw(t, d)
}

func doc(t *testing.T, id int, v string) bson.Raw {
	return mustRaw(t, bson.D{{Key: "_id", Value: int32(id)}, {Key: "v", Value: v}})
}

func verdictFor(r engine.VerifyReport, ns namespace.Namespace) (engine.CollectionVerdict, bool) {
	for _, cv := range r.Collections {
		if cv.Namespace == ns {
			return cv, true
		}
	}
	return engine.CollectionVerdict{}, false
}

func TestVerify_AllMatch(t *testing.T) {
	ns := namespace.Namespace{DB: "shop", Collection: "orders"}
	src, dst := newFakeStore(), newFakeStore()
	for i := 1; i <= 5; i++ {
		src.addDoc(ns, doc(t, i, "v"))
		dst.addDoc(ns, doc(t, i, "v"))
	}
	idIdx := idxSpec(t, "_id_", bson.D{{Key: "_id", Value: int32(1)}})
	emailIdx := idxSpec(t, "email_1", bson.D{{Key: "email", Value: int32(1)}}, bson.E{Key: "unique", Value: true})
	src.setIndexes(ns, idIdx, emailIdx)
	dst.setIndexes(ns, idIdx, emailIdx)

	v := &Verifier{src: src, dst: dst, log: discardLogger()}
	report, err := v.Verify(context.Background(), engine.VerifyOptions{
		Namespaces:   []namespace.Namespace{ns},
		CheckCounts:  true,
		CheckIndexes: true,
		SampleSize:   10,
		Workers:      1,
	})
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if !report.Ok {
		t.Fatalf("report not Ok: %+v", report.Collections)
	}
	cv, _ := verdictFor(report, ns)
	if !cv.CountsMatch || cv.SrcCount != 5 || cv.DstCount != 5 {
		t.Errorf("counts wrong: %+v", cv)
	}
	if !cv.IndexesMatch || len(cv.MissingIndexes) != 0 || len(cv.ExtraIndexes) != 0 {
		t.Errorf("indexes wrong: %+v", cv)
	}
	if !cv.SampleMatch || cv.SampleChecked != 5 || cv.SampleMismatches != 0 {
		t.Errorf("sample wrong: %+v", cv)
	}
}

func TestVerify_CountMismatch(t *testing.T) {
	ns := namespace.Namespace{DB: "db", Collection: "c"}
	src, dst := newFakeStore(), newFakeStore()
	src.addDoc(ns, doc(t, 1, "v"))
	src.addDoc(ns, doc(t, 2, "v"))
	dst.addDoc(ns, doc(t, 1, "v"))

	v := &Verifier{src: src, dst: dst, log: discardLogger()}
	report, err := v.Verify(context.Background(), engine.VerifyOptions{
		Namespaces:  []namespace.Namespace{ns},
		CheckCounts: true,
		Workers:     1,
	})
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if report.Ok {
		t.Fatalf("expected report not Ok")
	}
	cv, _ := verdictFor(report, ns)
	if cv.CountsMatch || cv.Ok {
		t.Errorf("expected counts mismatch: %+v", cv)
	}
	if cv.SrcCount != 2 || cv.DstCount != 1 {
		t.Errorf("counts = %d/%d, want 2/1", cv.SrcCount, cv.DstCount)
	}
	if len(cv.Notes) == 0 {
		t.Errorf("expected a note explaining the mismatch")
	}
}

func TestVerify_MissingIndex(t *testing.T) {
	ns := namespace.Namespace{DB: "db", Collection: "c"}
	src, dst := newFakeStore(), newFakeStore()
	idIdx := idxSpec(t, "_id_", bson.D{{Key: "_id", Value: int32(1)}})
	emailIdx := idxSpec(t, "email_1", bson.D{{Key: "email", Value: int32(1)}})
	src.setIndexes(ns, idIdx, emailIdx)
	dst.setIndexes(ns, idIdx)

	v := &Verifier{src: src, dst: dst, log: discardLogger()}
	report, _ := v.Verify(context.Background(), engine.VerifyOptions{
		Namespaces:   []namespace.Namespace{ns},
		CheckIndexes: true,
		Workers:      1,
	})
	if report.Ok {
		t.Fatalf("expected report not Ok")
	}
	cv, _ := verdictFor(report, ns)
	if cv.IndexesMatch {
		t.Errorf("expected indexes mismatch")
	}
	if !equalStrings(cv.MissingIndexes, []string{"email_1"}) {
		t.Errorf("MissingIndexes = %v, want [email_1]", cv.MissingIndexes)
	}
	if len(cv.ExtraIndexes) != 0 {
		t.Errorf("ExtraIndexes = %v, want none", cv.ExtraIndexes)
	}
}

func TestVerify_ExtraIndex(t *testing.T) {
	ns := namespace.Namespace{DB: "db", Collection: "c"}
	src, dst := newFakeStore(), newFakeStore()
	idIdx := idxSpec(t, "_id_", bson.D{{Key: "_id", Value: int32(1)}})
	src.setIndexes(ns, idIdx)
	dst.setIndexes(ns, idIdx, idxSpec(t, "spurious_1", bson.D{{Key: "spurious", Value: int32(1)}}))

	v := &Verifier{src: src, dst: dst, log: discardLogger()}
	report, _ := v.Verify(context.Background(), engine.VerifyOptions{
		Namespaces:   []namespace.Namespace{ns},
		CheckIndexes: true,
		Workers:      1,
	})
	if report.Ok {
		t.Fatalf("expected report not Ok")
	}
	cv, _ := verdictFor(report, ns)
	if !equalStrings(cv.ExtraIndexes, []string{"spurious_1"}) {
		t.Errorf("ExtraIndexes = %v, want [spurious_1]", cv.ExtraIndexes)
	}
}

func TestVerify_SampleValueDrift(t *testing.T) {
	ns := namespace.Namespace{DB: "db", Collection: "c"}
	src, dst := newFakeStore(), newFakeStore()
	src.addDoc(ns, doc(t, 1, "original"))
	dst.addDoc(ns, doc(t, 1, "TAMPERED"))

	v := &Verifier{src: src, dst: dst, log: discardLogger()}
	report, _ := v.Verify(context.Background(), engine.VerifyOptions{
		Namespaces: []namespace.Namespace{ns},
		SampleSize: 10,
		Workers:    1,
	})
	if report.Ok {
		t.Fatalf("expected report not Ok")
	}
	cv, _ := verdictFor(report, ns)
	if cv.SampleMatch || cv.SampleChecked != 1 || cv.SampleMismatches != 1 {
		t.Errorf("sample verdict wrong: %+v", cv)
	}
}

func TestVerify_SampleMissingDstDoc(t *testing.T) {
	ns := namespace.Namespace{DB: "db", Collection: "c"}
	src, dst := newFakeStore(), newFakeStore()
	src.addDoc(ns, doc(t, 1, "v"))
	src.addDoc(ns, doc(t, 2, "v"))
	dst.addDoc(ns, doc(t, 1, "v")) // _id 2 absent on dst

	v := &Verifier{src: src, dst: dst, log: discardLogger()}
	report, _ := v.Verify(context.Background(), engine.VerifyOptions{
		Namespaces: []namespace.Namespace{ns},
		SampleSize: 10,
		Workers:    1,
	})
	if report.Ok {
		t.Fatalf("expected report not Ok")
	}
	cv, _ := verdictFor(report, ns)
	if cv.SampleChecked != 2 || cv.SampleMismatches != 1 {
		t.Errorf("sample verdict wrong: checked=%d mismatches=%d", cv.SampleChecked, cv.SampleMismatches)
	}
}

func TestVerify_NoChecksEnabledIsOk(t *testing.T) {
	ns := namespace.Namespace{DB: "db", Collection: "c"}
	src, dst := newFakeStore(), newFakeStore()
	src.addDoc(ns, doc(t, 1, "v")) // counts differ, but CheckCounts is off
	v := &Verifier{src: src, dst: dst, log: discardLogger()}
	report, _ := v.Verify(context.Background(), engine.VerifyOptions{
		Namespaces: []namespace.Namespace{ns},
		Workers:    1,
	})
	if !report.Ok {
		t.Fatalf("expected Ok when no checks are enabled")
	}
}

func TestVerify_PerCollectionErrorDoesNotAbort(t *testing.T) {
	good := namespace.Namespace{DB: "db", Collection: "good"}
	bad := namespace.Namespace{DB: "db", Collection: "bad"}
	src, dst := newFakeStore(), newFakeStore()
	src.addDoc(good, doc(t, 1, "v"))
	dst.addDoc(good, doc(t, 1, "v"))
	// bad: source count errors out.
	src.countErr[bad.String()] = errors.New("boom")

	v := &Verifier{src: src, dst: dst, log: discardLogger()}
	report, err := v.Verify(context.Background(), engine.VerifyOptions{
		Namespaces:  []namespace.Namespace{good, bad},
		CheckCounts: true,
		Workers:     2,
	})
	if err != nil {
		t.Fatalf("Verify returned fatal error, want per-collection handling: %v", err)
	}
	if report.Ok {
		t.Fatalf("expected report not Ok because one collection failed")
	}
	if len(report.Collections) != 2 {
		t.Fatalf("expected 2 verdicts, got %d", len(report.Collections))
	}
	gcv, _ := verdictFor(report, good)
	if !gcv.Ok {
		t.Errorf("good collection should be Ok: %+v", gcv)
	}
	bcv, _ := verdictFor(report, bad)
	if bcv.Ok || len(bcv.Notes) == 0 {
		t.Errorf("bad collection should fail with a note: %+v", bcv)
	}
}

func TestVerify_AllNamespacesConcurrent(t *testing.T) {
	const n = 50
	src, dst := newFakeStore(), newFakeStore()
	nss := make([]namespace.Namespace, n)
	for i := 0; i < n; i++ {
		ns := namespace.Namespace{DB: "db", Collection: fmt.Sprintf("c%02d", i)}
		nss[i] = ns
		src.addDoc(ns, doc(t, 1, "v"))
		dst.addDoc(ns, doc(t, 1, "v"))
		// Make every 7th collection drift so we exercise mixed verdicts.
		if i%7 == 0 {
			dst.addDoc(ns, doc(t, 2, "extra"))
		}
	}
	v := &Verifier{src: src, dst: dst, log: discardLogger()}
	report, err := v.Verify(context.Background(), engine.VerifyOptions{
		Namespaces:  nss,
		CheckCounts: true,
		Workers:     8,
	})
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if len(report.Collections) != n {
		t.Fatalf("expected %d verdicts, got %d", n, len(report.Collections))
	}
	// Every namespace must have a verdict, positioned by input order.
	for i, ns := range nss {
		if report.Collections[i].Namespace != ns {
			t.Fatalf("verdict %d is for %v, want %v", i, report.Collections[i].Namespace, ns)
		}
		wantOk := i%7 != 0
		if report.Collections[i].Ok != wantOk {
			t.Errorf("collection %v Ok=%v, want %v", ns, report.Collections[i].Ok, wantOk)
		}
	}
	if report.Ok {
		t.Errorf("expected overall not Ok due to drifted collections")
	}
}

func TestVerify_ContextCancelledIsNotFatal(t *testing.T) {
	ns := namespace.Namespace{DB: "db", Collection: "c"}
	src, dst := newFakeStore(), newFakeStore()
	src.addDoc(ns, doc(t, 1, "v"))
	dst.addDoc(ns, doc(t, 1, "v"))

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before running

	v := &Verifier{src: src, dst: dst, log: discardLogger()}
	report, err := v.Verify(ctx, engine.VerifyOptions{
		Namespaces:  []namespace.Namespace{ns},
		CheckCounts: true,
		Workers:     1,
	})
	if err != nil {
		t.Fatalf("Verify should not return a fatal error on cancellation: %v", err)
	}
	if report.Ok {
		t.Fatalf("cancelled run should not report Ok")
	}
}

func TestVerify_WorkersDefaulted(t *testing.T) {
	ns := namespace.Namespace{DB: "db", Collection: "c"}
	src, dst := newFakeStore(), newFakeStore()
	src.addDoc(ns, doc(t, 1, "v"))
	dst.addDoc(ns, doc(t, 1, "v"))
	v := &Verifier{src: src, dst: dst, log: discardLogger()}
	// Workers omitted (0) must be treated as 1, not deadlock.
	report, err := v.Verify(context.Background(), engine.VerifyOptions{
		Namespaces:  []namespace.Namespace{ns},
		CheckCounts: true,
	})
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if !report.Ok {
		t.Fatalf("expected Ok")
	}
}
