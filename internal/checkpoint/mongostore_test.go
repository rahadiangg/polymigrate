package checkpoint

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// fakeColl is an in-memory checkpointColl for unit-testing MongoStore without a
// live server. It stores marshalled documents keyed by _id and can be told to
// fail specific operations.
type fakeColl struct {
	mu         sync.Mutex
	docs       map[string]bson.Raw
	replaceErr error
	findErr    error
	deleteErr  error
}

func newFakeColl() *fakeColl { return &fakeColl{docs: map[string]bson.Raw{}} }

func (f *fakeColl) replaceOne(_ context.Context, id string, doc bson.Raw) error {
	if f.replaceErr != nil {
		return f.replaceErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	cp := make(bson.Raw, len(doc))
	copy(cp, doc)
	f.docs[id] = cp
	return nil
}

func (f *fakeColl) findOne(_ context.Context, id string) (bson.Raw, bool, error) {
	if f.findErr != nil {
		return nil, false, f.findErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	d, ok := f.docs[id]
	if !ok {
		return nil, false, nil
	}
	return d, true, nil
}

func (f *fakeColl) deleteOne(_ context.Context, id string) error {
	if f.deleteErr != nil {
		return f.deleteErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.docs, id)
	return nil
}

func TestMongoStore_SaveLoadRoundTrip(t *testing.T) {
	ms := &MongoStore{coll: newFakeColl()}
	ctx := context.Background()

	want := sampleState("run-mongo")
	if err := ms.Save(ctx, want); err != nil {
		t.Fatalf("Save: %v", err)
	}
	got, found, err := ms.Load(ctx, want.RunID)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !found {
		t.Fatal("Load: found = false")
	}
	assertStateEqual(t, want, got)
}

func TestMongoStore_LoadMissing(t *testing.T) {
	ms := &MongoStore{coll: newFakeColl()}
	got, found, err := ms.Load(context.Background(), "nope")
	if err != nil {
		t.Fatalf("Load missing: %v", err)
	}
	if found || !reflect.DeepEqual(got, State{}) {
		t.Errorf("Load missing: found=%v state=%+v", found, got)
	}
}

func TestMongoStore_Delete(t *testing.T) {
	ms := &MongoStore{coll: newFakeColl()}
	ctx := context.Background()
	if err := ms.Save(ctx, sampleState("run-d")); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := ms.Delete(ctx, "run-d"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	_, found, err := ms.Load(ctx, "run-d")
	if err != nil || found {
		t.Fatalf("after Delete: found=%v err=%v", found, err)
	}
}

func TestMongoStore_ErrorsWrapped(t *testing.T) {
	sentinel := errors.New("boom")

	t.Run("save", func(t *testing.T) {
		ms := &MongoStore{coll: &fakeColl{docs: map[string]bson.Raw{}, replaceErr: sentinel}}
		if err := ms.Save(context.Background(), sampleState("r")); !errors.Is(err, sentinel) {
			t.Fatalf("Save err = %v, want wrapping sentinel", err)
		}
	})
	t.Run("load", func(t *testing.T) {
		ms := &MongoStore{coll: &fakeColl{docs: map[string]bson.Raw{}, findErr: sentinel}}
		if _, _, err := ms.Load(context.Background(), "r"); !errors.Is(err, sentinel) {
			t.Fatalf("Load err = %v, want wrapping sentinel", err)
		}
	})
	t.Run("delete", func(t *testing.T) {
		ms := &MongoStore{coll: &fakeColl{docs: map[string]bson.Raw{}, deleteErr: sentinel}}
		if err := ms.Delete(context.Background(), "r"); !errors.Is(err, sentinel) {
			t.Fatalf("Delete err = %v, want wrapping sentinel", err)
		}
	})
}

func TestNamespacesMapArrayRoundTrip(t *testing.T) {
	tests := []struct {
		name string
		in   map[string]NSProgress
	}{
		{"nil", nil},
		{"empty", map[string]NSProgress{}},
		{"single", map[string]NSProgress{
			"db.coll": {Done: true, LastID: []byte{0x01, 0x02, 0x03}, DocsCopied: 9},
		}},
		{"many-with-dotted-keys", map[string]NSProgress{
			"shop.orders":         {Done: true, DocsCopied: 1},
			"shop.line.items":     {Done: false, LastID: []byte{0xaa}, DocsCopied: 2},
			"analytics.events":    {Done: false, DocsCopied: 3},
			"a.b.c.deeply.nested": {Done: true, LastID: []byte{0x00, 0xff}, DocsCopied: 4},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entries := namespacesToEntries(tt.in)
			got := entriesToNamespaces(entries)

			// nil and empty both round-trip to a nil map (a fresh State's zero value).
			if len(tt.in) == 0 {
				if got != nil {
					t.Errorf("empty/nil input round-tripped to %v, want nil", got)
				}
				return
			}
			if !reflect.DeepEqual(tt.in, got) {
				t.Errorf("round-trip mismatch:\n in = %#v\nout = %#v", tt.in, got)
			}
		})
	}
}

func TestNamespacesToEntries_SortedDeterministic(t *testing.T) {
	m := map[string]NSProgress{
		"z.z": {}, "a.a": {}, "m.m": {}, "b.b": {},
	}
	e1 := namespacesToEntries(m)
	e2 := namespacesToEntries(m)
	if !reflect.DeepEqual(e1, e2) {
		t.Fatal("namespacesToEntries not deterministic across calls")
	}
	// Verify ascending order by ns.
	for i := 1; i < len(e1); i++ {
		if e1[i-1].NS > e1[i].NS {
			t.Errorf("entries not sorted: %q before %q", e1[i-1].NS, e1[i].NS)
		}
	}
}

func TestStateToDoc_UsesArrayNotDottedFields(t *testing.T) {
	// Round-trip through actual BSON marshalling to prove the map keys never
	// become top-level BSON field names (which would be dangerous with dots).
	s := sampleState("run-bson")
	raw, err := bson.Marshal(stateToDoc(s))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var doc checkpointDoc
	if err := bson.Unmarshal(raw, &doc); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(doc.Namespaces) != len(s.Namespaces) {
		t.Fatalf("namespaces array len = %d, want %d", len(doc.Namespaces), len(s.Namespaces))
	}
	assertStateEqual(t, s, docToState(doc))
}

// assertStateEqual compares two States field by field, tolerating time zone
// representation differences and byte-slice nil/empty distinctions.
func assertStateEqual(t *testing.T, want, got State) {
	t.Helper()
	if want.RunID != got.RunID || want.Mode != got.Mode ||
		want.ConfigHash != got.ConfigHash || want.Phase != got.Phase ||
		want.NonResumable != got.NonResumable {
		t.Errorf("scalar fields differ:\nwant %+v\n got %+v", want, got)
	}
	if !want.UpdatedAt.Equal(got.UpdatedAt) {
		t.Errorf("UpdatedAt: want %v, got %v", want.UpdatedAt, got.UpdatedAt)
	}
	if !reflect.DeepEqual(want.ResumeToken, got.ResumeToken) {
		t.Errorf("ResumeToken: want %x, got %x", want.ResumeToken, got.ResumeToken)
	}
	if !reflect.DeepEqual(want.Namespaces, got.Namespaces) {
		t.Errorf("Namespaces differ:\nwant %#v\n got %#v", want.Namespaces, got.Namespaces)
	}
}
