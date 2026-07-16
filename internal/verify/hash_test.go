package verify

import (
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// mustRaw marshals a bson.D into a bson.Raw, failing the test on error.
func mustRaw(t *testing.T, d bson.D) bson.Raw {
	t.Helper()
	b, err := bson.Marshal(d)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	return bson.Raw(b)
}

func TestHashDoc_KeyOrderInvariant(t *testing.T) {
	// Same content, different key order at both top level and in a sub-document.
	a := mustRaw(t, bson.D{
		{Key: "a", Value: int32(1)},
		{Key: "nested", Value: bson.D{{Key: "x", Value: "hello"}, {Key: "y", Value: int32(2)}}},
		{Key: "z", Value: true},
	})
	b := mustRaw(t, bson.D{
		{Key: "z", Value: true},
		{Key: "nested", Value: bson.D{{Key: "y", Value: int32(2)}, {Key: "x", Value: "hello"}}},
		{Key: "a", Value: int32(1)},
	})

	ha, err := hashDoc(a)
	if err != nil {
		t.Fatalf("hashDoc(a): %v", err)
	}
	hb, err := hashDoc(b)
	if err != nil {
		t.Fatalf("hashDoc(b): %v", err)
	}
	if ha != hb {
		t.Fatalf("expected equal hashes for key-reordered docs; got %x vs %x", ha, hb)
	}
}

func TestHashDoc_ValueChangeDetected(t *testing.T) {
	base := mustRaw(t, bson.D{{Key: "a", Value: int32(1)}, {Key: "b", Value: "x"}})
	changed := mustRaw(t, bson.D{{Key: "a", Value: int32(2)}, {Key: "b", Value: "x"}})

	hb, _ := hashDoc(base)
	hc, _ := hashDoc(changed)
	if hb == hc {
		t.Fatalf("expected different hashes when a value changes")
	}
}

func TestHashDoc_TypeChangeDetected(t *testing.T) {
	// Same numeric value, different BSON type must differ.
	i32 := mustRaw(t, bson.D{{Key: "n", Value: int32(1)}})
	i64 := mustRaw(t, bson.D{{Key: "n", Value: int64(1)}})

	h32, _ := hashDoc(i32)
	h64, _ := hashDoc(i64)
	if h32 == h64 {
		t.Fatalf("expected different hashes for int32(1) vs int64(1)")
	}
}

func TestHashDoc_ArrayOrderSignificant(t *testing.T) {
	forward := mustRaw(t, bson.D{{Key: "a", Value: bson.A{int32(1), int32(2), int32(3)}}})
	reversed := mustRaw(t, bson.D{{Key: "a", Value: bson.A{int32(3), int32(2), int32(1)}}})

	hf, _ := hashDoc(forward)
	hr, _ := hashDoc(reversed)
	if hf == hr {
		t.Fatalf("expected different hashes when array order changes")
	}
}

func TestHashDoc_ArrayOfDocsKeyOrderInvariant(t *testing.T) {
	// Array elements keep order, but each element document's keys are sorted.
	a := mustRaw(t, bson.D{{Key: "arr", Value: bson.A{
		bson.D{{Key: "p", Value: int32(1)}, {Key: "q", Value: int32(2)}},
	}}})
	b := mustRaw(t, bson.D{{Key: "arr", Value: bson.A{
		bson.D{{Key: "q", Value: int32(2)}, {Key: "p", Value: int32(1)}},
	}}})

	ha, _ := hashDoc(a)
	hb, _ := hashDoc(b)
	if ha != hb {
		t.Fatalf("expected equal hashes for array-of-docs with reordered sub-keys")
	}
}

func TestHashDoc_NestingCannotCollide(t *testing.T) {
	// {a: {b: 1}} must not hash the same as {a: 1, "a.b": 1}-style flattening.
	nested := mustRaw(t, bson.D{{Key: "a", Value: bson.D{{Key: "b", Value: int32(1)}}}})
	flat := mustRaw(t, bson.D{{Key: "a", Value: int32(1)}, {Key: "b", Value: int32(1)}})

	hn, _ := hashDoc(nested)
	hf, _ := hashDoc(flat)
	if hn == hf {
		t.Fatalf("nested and flat documents must not collide")
	}
}

func TestHashDoc_Empty(t *testing.T) {
	e1 := mustRaw(t, bson.D{})
	e2 := mustRaw(t, bson.D{})
	h1, err := hashDoc(e1)
	if err != nil {
		t.Fatalf("hashDoc(empty): %v", err)
	}
	h2, _ := hashDoc(e2)
	if h1 != h2 {
		t.Fatalf("empty documents must hash equally")
	}
	nonEmpty := mustRaw(t, bson.D{{Key: "a", Value: int32(1)}})
	hn, _ := hashDoc(nonEmpty)
	if h1 == hn {
		t.Fatalf("empty and non-empty documents must differ")
	}
}

func TestIDKey_DistinguishesTypes(t *testing.T) {
	raw32 := mustRaw(t, bson.D{{Key: "_id", Value: int32(1)}})
	raw64 := mustRaw(t, bson.D{{Key: "_id", Value: int64(1)}})
	k32 := idKey(raw32.Lookup("_id"))
	k64 := idKey(raw64.Lookup("_id"))
	if k32 == k64 {
		t.Fatalf("idKey must distinguish int32 and int64 _id values")
	}

	// Identical _id values yield identical keys (src/dst consistency).
	rawA := mustRaw(t, bson.D{{Key: "_id", Value: "abc"}})
	rawB := mustRaw(t, bson.D{{Key: "_id", Value: "abc"}})
	if idKey(rawA.Lookup("_id")) != idKey(rawB.Lookup("_id")) {
		t.Fatalf("identical _id values must yield identical keys")
	}
}

func TestCompareSamples(t *testing.T) {
	docA := mustRaw(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: "v", Value: "x"}})
	docAReordered := mustRaw(t, bson.D{{Key: "v", Value: "x"}, {Key: "_id", Value: int32(1)}})
	docADrift := mustRaw(t, bson.D{{Key: "_id", Value: int32(1)}, {Key: "v", Value: "DIFFERENT"}})
	docB := mustRaw(t, bson.D{{Key: "_id", Value: int32(2)}, {Key: "v", Value: "y"}})

	tests := []struct {
		name           string
		src, dst       map[string]bson.Raw
		wantChecked    int
		wantMismatches int
	}{
		{
			name:           "all match (key order ignored)",
			src:            map[string]bson.Raw{"1": docA, "2": docB},
			dst:            map[string]bson.Raw{"1": docAReordered, "2": docB},
			wantChecked:    2,
			wantMismatches: 0,
		},
		{
			name:           "value drift",
			src:            map[string]bson.Raw{"1": docA},
			dst:            map[string]bson.Raw{"1": docADrift},
			wantChecked:    1,
			wantMismatches: 1,
		},
		{
			name:           "missing destination doc",
			src:            map[string]bson.Raw{"1": docA, "2": docB},
			dst:            map[string]bson.Raw{"1": docA},
			wantChecked:    2,
			wantMismatches: 1,
		},
		{
			name:           "empty",
			src:            map[string]bson.Raw{},
			dst:            map[string]bson.Raw{},
			wantChecked:    0,
			wantMismatches: 0,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			checked, mismatches := compareSamples(tc.src, tc.dst)
			if checked != tc.wantChecked || mismatches != tc.wantMismatches {
				t.Fatalf("got checked=%d mismatches=%d; want %d/%d",
					checked, mismatches, tc.wantChecked, tc.wantMismatches)
			}
		})
	}
}
