package cdc

import (
	"bytes"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/weefer/mongomigrate/internal/namespace"
)

// rawID marshals v as an _id and returns it as a bson.RawValue, mirroring how a
// documentKey._id is extracted from a real event.
func rawID(t *testing.T, v any) bson.RawValue {
	t.Helper()
	raw, err := bson.Marshal(bson.D{{Key: "_id", Value: v}})
	if err != nil {
		t.Fatalf("marshal id: %v", err)
	}
	return bson.Raw(raw).Lookup("_id")
}

// rawDoc marshals v to a bson.Raw document.
func rawDoc(t *testing.T, v any) bson.Raw {
	t.Helper()
	raw, err := bson.Marshal(v)
	if err != nil {
		t.Fatalf("marshal doc: %v", err)
	}
	return bson.Raw(raw)
}

// eventRaw builds a raw change-stream event document from its parts. Zero-valued
// parts are omitted so callers can model deletes (no fullDocument), invalidates
// (no ns/documentKey), etc.
func eventRaw(t *testing.T, op string, db, coll string, id any, full bson.D, ct *bson.Timestamp) bson.Raw {
	t.Helper()
	d := bson.D{{Key: "operationType", Value: op}}
	if db != "" || coll != "" {
		d = append(d, bson.E{Key: "ns", Value: bson.D{{Key: "db", Value: db}, {Key: "coll", Value: coll}}})
	}
	if id != nil {
		d = append(d, bson.E{Key: "documentKey", Value: bson.D{{Key: "_id", Value: id}}})
	}
	if full != nil {
		d = append(d, bson.E{Key: "fullDocument", Value: full})
	}
	if ct != nil {
		d = append(d, bson.E{Key: "clusterTime", Value: *ct})
	}
	return rawDoc(t, d)
}

func TestParseEvent_Insert(t *testing.T) {
	ct := bson.Timestamp{T: 1_700_000_000, I: 3}
	full := bson.D{{Key: "_id", Value: 7}, {Key: "name", Value: "ada"}}
	raw := eventRaw(t, opInsert, "shop", "users", 7, full, &ct)

	e, err := parseEvent(raw)
	if err != nil {
		t.Fatalf("parseEvent: %v", err)
	}
	if e.OpType != opInsert {
		t.Errorf("OpType = %q, want %q", e.OpType, opInsert)
	}
	if e.NS != (namespace.Namespace{DB: "shop", Collection: "users"}) {
		t.Errorf("NS = %v", e.NS)
	}
	if got, ok := e.ID.Int32OK(); !ok || got != 7 {
		t.Errorf("ID = %v (ok=%v), want 7", got, ok)
	}
	if len(e.Full) == 0 {
		t.Error("Full is empty, want the post-image")
	}
	if name, ok := e.Full.Lookup("name").StringValueOK(); !ok || name != "ada" {
		t.Errorf("Full.name = %q (ok=%v), want ada", name, ok)
	}
	wantAt := time.Unix(1_700_000_000, 0).UTC()
	if !e.At.Equal(wantAt) {
		t.Errorf("At = %v, want %v", e.At, wantAt)
	}
}

func TestParseEvent_Token(t *testing.T) {
	// The change event document's own _id is a {_data: ...} resume-token document.
	tokenDoc := bson.D{{Key: "_data", Value: "826A1B2C3D000000012B022C0100296E5A10"}}
	full := bson.D{{Key: "_id", Value: 7}, {Key: "name", Value: "ada"}}
	d := bson.D{
		{Key: "_id", Value: tokenDoc},
		{Key: "operationType", Value: opInsert},
		{Key: "ns", Value: bson.D{{Key: "db", Value: "shop"}, {Key: "coll", Value: "users"}}},
		{Key: "documentKey", Value: bson.D{{Key: "_id", Value: 7}}},
		{Key: "fullDocument", Value: full},
	}
	raw := rawDoc(t, d)

	e, err := parseEvent(raw)
	if err != nil {
		t.Fatalf("parseEvent: %v", err)
	}
	// Token must be the raw BSON of the change doc's _id, byte-identical to the
	// standalone-marshaled resume-token document (usable with SetResumeAfter).
	want := rawDoc(t, tokenDoc)
	if !bytes.Equal(e.Token, want) {
		t.Errorf("Token = %x, want %x", e.Token, []byte(want))
	}
	// The document-key _id (7) must remain distinct from the resume token.
	if got, ok := e.ID.Int32OK(); !ok || got != 7 {
		t.Errorf("documentKey ID = %v (ok=%v), want 7", got, ok)
	}
}

func TestParseEvent_TokenAbsent(t *testing.T) {
	// A synthetic event with no top-level _id yields an empty Token.
	raw := eventRaw(t, opInsert, "d", "c", 1, bson.D{{Key: "_id", Value: 1}}, nil)
	e, err := parseEvent(raw)
	if err != nil {
		t.Fatalf("parseEvent: %v", err)
	}
	if len(e.Token) != 0 {
		t.Errorf("Token = %x, want empty when the event carries no _id", e.Token)
	}
}

func TestParseEvent_UpdateWithLookup(t *testing.T) {
	full := bson.D{{Key: "_id", Value: "abc"}, {Key: "n", Value: 42}}
	raw := eventRaw(t, opUpdate, "db1", "c1", "abc", full, nil)

	e, err := parseEvent(raw)
	if err != nil {
		t.Fatalf("parseEvent: %v", err)
	}
	if e.OpType != opUpdate {
		t.Errorf("OpType = %q", e.OpType)
	}
	if id, ok := e.ID.StringValueOK(); !ok || id != "abc" {
		t.Errorf("ID = %q (ok=%v)", id, ok)
	}
	if len(e.Full) == 0 {
		t.Error("update with updateLookup should carry a post-image")
	}
	if !e.At.IsZero() {
		t.Errorf("At = %v, want zero when clusterTime absent", e.At)
	}
}

func TestParseEvent_Delete(t *testing.T) {
	raw := eventRaw(t, opDelete, "db1", "c1", "gone", nil, nil)

	e, err := parseEvent(raw)
	if err != nil {
		t.Fatalf("parseEvent: %v", err)
	}
	if e.OpType != opDelete {
		t.Errorf("OpType = %q", e.OpType)
	}
	if len(e.Full) != 0 {
		t.Error("delete should not carry a fullDocument")
	}
	if id, ok := e.ID.StringValueOK(); !ok || id != "gone" {
		t.Errorf("ID = %q (ok=%v)", id, ok)
	}
}

func TestParseEvent_Invalidate(t *testing.T) {
	raw := eventRaw(t, opInvalidate, "", "", nil, nil, nil)

	e, err := parseEvent(raw)
	if err != nil {
		t.Fatalf("parseEvent: %v", err)
	}
	if e.OpType != opInvalidate {
		t.Errorf("OpType = %q", e.OpType)
	}
	if !e.NS.IsZero() {
		t.Errorf("NS = %v, want zero", e.NS)
	}
	if !e.ID.IsZero() {
		t.Error("invalidate should have no document key")
	}
}

func TestParseEvent_Errors(t *testing.T) {
	t.Run("empty", func(t *testing.T) {
		if _, err := parseEvent(nil); err == nil {
			t.Fatal("want error on empty raw")
		}
	})
	t.Run("missing operationType", func(t *testing.T) {
		raw := rawDoc(t, bson.D{{Key: "ns", Value: bson.D{{Key: "db", Value: "x"}}}})
		if _, err := parseEvent(raw); err == nil {
			t.Fatal("want error when operationType missing")
		}
	})
	t.Run("non-string operationType", func(t *testing.T) {
		raw := rawDoc(t, bson.D{{Key: "operationType", Value: 123}})
		if _, err := parseEvent(raw); err == nil {
			t.Fatal("want error when operationType is not a string")
		}
	})
}

func TestEventToModel(t *testing.T) {
	full := bson.D{{Key: "_id", Value: 1}, {Key: "v", Value: "x"}}

	tests := []struct {
		name     string
		event    ChangeEvent
		wantOK   bool
		wantKind string // "replace", "delete", or ""
	}{
		{
			name:     "insert",
			event:    ChangeEvent{OpType: opInsert, ID: rawID(t, 1), Full: rawDoc(t, full)},
			wantOK:   true,
			wantKind: "replace",
		},
		{
			name:     "replace",
			event:    ChangeEvent{OpType: opReplace, ID: rawID(t, 1), Full: rawDoc(t, full)},
			wantOK:   true,
			wantKind: "replace",
		},
		{
			name:     "update with post-image",
			event:    ChangeEvent{OpType: opUpdate, ID: rawID(t, 1), Full: rawDoc(t, full)},
			wantOK:   true,
			wantKind: "replace",
		},
		{
			name:     "update without post-image",
			event:    ChangeEvent{OpType: opUpdate, ID: rawID(t, 1)},
			wantOK:   false,
			wantKind: "",
		},
		{
			name:     "delete",
			event:    ChangeEvent{OpType: opDelete, ID: rawID(t, 1)},
			wantOK:   true,
			wantKind: "delete",
		},
		{name: "drop", event: ChangeEvent{OpType: opDrop}, wantOK: false},
		{name: "invalidate", event: ChangeEvent{OpType: opInvalidate}, wantOK: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			m, ok := eventToModel(tt.event)
			if ok != tt.wantOK {
				t.Fatalf("ok = %v, want %v", ok, tt.wantOK)
			}
			if !ok {
				if m != nil {
					t.Errorf("model = %v, want nil when !ok", m)
				}
				return
			}
			switch tt.wantKind {
			case "replace":
				rom, isReplace := m.(*mongo.ReplaceOneModel)
				if !isReplace {
					t.Fatalf("model type = %T, want *mongo.ReplaceOneModel", m)
				}
				if rom.Upsert == nil || !*rom.Upsert {
					t.Error("replace model must be an upsert")
				}
				if rom.Replacement == nil {
					t.Error("replace model must carry the replacement")
				}
			case "delete":
				if _, isDelete := m.(*mongo.DeleteOneModel); !isDelete {
					t.Fatalf("model type = %T, want *mongo.DeleteOneModel", m)
				}
			}
		})
	}
}

func TestDedupeBatch(t *testing.T) {
	ns := namespace.Namespace{DB: "d", Collection: "c"}
	mk := func(op string, id int) ChangeEvent {
		return ChangeEvent{OpType: op, NS: ns, ID: rawID(t, id)}
	}

	t.Run("delete after insert wins", func(t *testing.T) {
		out := dedupeBatch([]ChangeEvent{mk(opInsert, 1), mk(opDelete, 1)})
		if len(out) != 1 {
			t.Fatalf("len = %d, want 1", len(out))
		}
		if out[0].OpType != opDelete {
			t.Errorf("winner = %q, want delete", out[0].OpType)
		}
	})

	t.Run("insert after delete wins", func(t *testing.T) {
		out := dedupeBatch([]ChangeEvent{mk(opDelete, 1), mk(opInsert, 1)})
		if len(out) != 1 || out[0].OpType != opInsert {
			t.Fatalf("out = %+v, want single insert", out)
		}
	})

	t.Run("distinct ids preserved in first-appearance order", func(t *testing.T) {
		out := dedupeBatch([]ChangeEvent{mk(opInsert, 1), mk(opInsert, 2), mk(opUpdate, 1)})
		if len(out) != 2 {
			t.Fatalf("len = %d, want 2", len(out))
		}
		if id, _ := out[0].ID.Int32OK(); id != 1 {
			t.Errorf("out[0] id = %d, want 1", id)
		}
		if out[0].OpType != opUpdate {
			t.Errorf("out[0] op = %q, want update (latest for id 1)", out[0].OpType)
		}
		if id, _ := out[1].ID.Int32OK(); id != 2 {
			t.Errorf("out[1] id = %d, want 2", id)
		}
	})

	t.Run("same id across namespaces not merged", func(t *testing.T) {
		a := ChangeEvent{OpType: opInsert, NS: namespace.Namespace{DB: "d", Collection: "a"}, ID: rawID(t, 1)}
		b := ChangeEvent{OpType: opInsert, NS: namespace.Namespace{DB: "d", Collection: "b"}, ID: rawID(t, 1)}
		out := dedupeBatch([]ChangeEvent{a, b})
		if len(out) != 2 {
			t.Fatalf("len = %d, want 2 (different collections)", len(out))
		}
	})

	t.Run("zero id events never merged", func(t *testing.T) {
		e := ChangeEvent{OpType: opInvalidate, NS: ns}
		out := dedupeBatch([]ChangeEvent{e, e})
		if len(out) != 2 {
			t.Fatalf("len = %d, want 2", len(out))
		}
	})

	t.Run("empty and single pass through", func(t *testing.T) {
		if out := dedupeBatch(nil); out != nil {
			t.Errorf("nil in -> %v", out)
		}
		one := []ChangeEvent{mk(opInsert, 9)}
		if out := dedupeBatch(one); len(out) != 1 {
			t.Errorf("single in -> len %d", len(out))
		}
	})
}

func TestMatchNS(t *testing.T) {
	a := namespace.Namespace{DB: "d", Collection: "a"}
	b := namespace.Namespace{DB: "d", Collection: "b"}
	ev := func(n namespace.Namespace) ChangeEvent { return ChangeEvent{NS: n} }

	if !matchNS(ev(a), nil) {
		t.Error("empty allowed should match everything")
	}
	if !matchNS(ev(a), []namespace.Namespace{a, b}) {
		t.Error("a should match [a,b]")
	}
	if matchNS(ev(b), []namespace.Namespace{a}) {
		t.Error("b should not match [a]")
	}
}

func TestDistinctDBs(t *testing.T) {
	tests := []struct {
		name string
		in   []namespace.Namespace
		want []string
	}{
		{"empty", nil, nil},
		{
			"single db many colls",
			[]namespace.Namespace{{DB: "a", Collection: "x"}, {DB: "a", Collection: "y"}},
			[]string{"a"},
		},
		{
			"multi db sorted+deduped",
			[]namespace.Namespace{{DB: "c", Collection: "x"}, {DB: "a", Collection: "y"}, {DB: "c", Collection: "z"}, {DB: "b", Collection: "w"}},
			[]string{"a", "b", "c"},
		},
		{
			"skips empty db",
			[]namespace.Namespace{{DB: "", Collection: "x"}, {DB: "a", Collection: "y"}},
			[]string{"a"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := distinctDBs(tt.in)
			if len(got) != len(tt.want) {
				t.Fatalf("got %v, want %v", got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("got %v, want %v", got, tt.want)
				}
			}
		})
	}
}

func TestIsOnlyDuplicateKeyErrors(t *testing.T) {
	dup := mongo.BulkWriteException{WriteErrors: []mongo.BulkWriteError{
		{WriteError: mongo.WriteError{Index: 0, Code: codeDuplicateKey}},
	}}
	if !isOnlyDuplicateKeyErrors(dup) {
		t.Error("all-dup bulk exception should be tolerated")
	}
	mixed := mongo.BulkWriteException{WriteErrors: []mongo.BulkWriteError{
		{WriteError: mongo.WriteError{Code: codeDuplicateKey}},
		{WriteError: mongo.WriteError{Code: 121}},
	}}
	if isOnlyDuplicateKeyErrors(mixed) {
		t.Error("non-dup write error must not be tolerated")
	}
	if isOnlyDuplicateKeyErrors(mongo.BulkWriteException{}) {
		t.Error("empty write errors is not a dup-only case")
	}
}
