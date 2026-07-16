package verify

import (
	"slices"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
)

func TestCanonicalizeIndex_Basic(t *testing.T) {
	spec := mustRaw(t, bson.D{
		{Key: "v", Value: int32(2)},
		{Key: "key", Value: bson.D{{Key: "email", Value: int32(1)}}},
		{Key: "name", Value: "email_1"},
		{Key: "unique", Value: true},
	})
	d, err := canonicalizeIndex(spec)
	if err != nil {
		t.Fatalf("canonicalizeIndex: %v", err)
	}
	if d.Name != "email_1" {
		t.Errorf("name = %q, want email_1", d.Name)
	}
	if d.Key != "email:1" {
		t.Errorf("key = %q, want email:1", d.Key)
	}
	if !d.Unique {
		t.Errorf("unique = false, want true")
	}
	if d.Sparse {
		t.Errorf("sparse = true, want false")
	}
	if d.HasExpire {
		t.Errorf("HasExpire = true, want false")
	}
}

func TestCanonicalizeIndex_CompoundOrderPreserved(t *testing.T) {
	ab := mustRaw(t, bson.D{
		{Key: "key", Value: bson.D{{Key: "a", Value: int32(1)}, {Key: "b", Value: int32(-1)}}},
		{Key: "name", Value: "a_1_b_-1"},
	})
	ba := mustRaw(t, bson.D{
		{Key: "key", Value: bson.D{{Key: "b", Value: int32(-1)}, {Key: "a", Value: int32(1)}}},
		{Key: "name", Value: "b_-1_a_1"},
	})
	dab, _ := canonicalizeIndex(ab)
	dba, _ := canonicalizeIndex(ba)
	if dab.Key != "a:1,b:-1" {
		t.Errorf("key = %q, want a:1,b:-1", dab.Key)
	}
	if dab.Signature() == dba.Signature() {
		t.Errorf("compound indexes with different key order must have different signatures")
	}
}

func TestCanonicalizeIndex_SpecialKeyTypes(t *testing.T) {
	spec := mustRaw(t, bson.D{
		{Key: "key", Value: bson.D{{Key: "loc", Value: "2dsphere"}, {Key: "h", Value: "hashed"}}},
		{Key: "name", Value: "loc_2dsphere_h_hashed"},
	})
	d, err := canonicalizeIndex(spec)
	if err != nil {
		t.Fatalf("canonicalizeIndex: %v", err)
	}
	if d.Key != "loc:2dsphere,h:hashed" {
		t.Errorf("key = %q, want loc:2dsphere,h:hashed", d.Key)
	}
}

func TestCanonicalizeIndex_TTL(t *testing.T) {
	spec := mustRaw(t, bson.D{
		{Key: "key", Value: bson.D{{Key: "createdAt", Value: int32(1)}}},
		{Key: "name", Value: "ttl"},
		{Key: "expireAfterSeconds", Value: int32(3600)},
	})
	d, err := canonicalizeIndex(spec)
	if err != nil {
		t.Fatalf("canonicalizeIndex: %v", err)
	}
	if !d.HasExpire || d.ExpireAfterSeconds != 3600 {
		t.Errorf("HasExpire=%v secs=%d, want true/3600", d.HasExpire, d.ExpireAfterSeconds)
	}
	// A different TTL must change the signature.
	spec2 := mustRaw(t, bson.D{
		{Key: "key", Value: bson.D{{Key: "createdAt", Value: int32(1)}}},
		{Key: "name", Value: "ttl"},
		{Key: "expireAfterSeconds", Value: int32(60)},
	})
	d2, _ := canonicalizeIndex(spec2)
	if d.Signature() == d2.Signature() {
		t.Errorf("different expireAfterSeconds must produce different signatures")
	}
}

func TestCanonicalizeIndex_PartialAndCollation(t *testing.T) {
	base := bson.D{
		{Key: "key", Value: bson.D{{Key: "a", Value: int32(1)}}},
		{Key: "name", Value: "a_1"},
	}
	withPartial := append(slices.Clone(base),
		bson.E{Key: "partialFilterExpression", Value: bson.D{{Key: "n", Value: bson.D{{Key: "$gt", Value: int32(5)}}}}})
	withCollation := append(slices.Clone(base),
		bson.E{Key: "collation", Value: bson.D{{Key: "locale", Value: "en"}, {Key: "strength", Value: int32(2)}}})

	dBase, _ := canonicalizeIndex(mustRaw(t, base))
	dPartial, _ := canonicalizeIndex(mustRaw(t, withPartial))
	dColl, _ := canonicalizeIndex(mustRaw(t, withCollation))

	if dPartial.PartialFilter == "" {
		t.Errorf("expected non-empty PartialFilter")
	}
	if dColl.CollationLocale != "en" {
		t.Errorf("CollationLocale = %q, want en", dColl.CollationLocale)
	}
	if dBase.Signature() == dPartial.Signature() {
		t.Errorf("partial filter must change signature")
	}
	if dBase.Signature() == dColl.Signature() {
		t.Errorf("collation must change signature")
	}

	// Equivalent partial filter written with reordered keys must match.
	withPartialReordered := append(slices.Clone(base),
		bson.E{Key: "partialFilterExpression", Value: bson.D{{Key: "n", Value: bson.D{{Key: "$gt", Value: int32(5)}}}}})
	dPartial2, _ := canonicalizeIndex(mustRaw(t, withPartialReordered))
	if dPartial.Signature() != dPartial2.Signature() {
		t.Errorf("equivalent partial filters must match")
	}
}

// TestSignatureDistinguishesRicherOptions covers the review fix: options beyond
// key/unique/sparse/ttl/partial/locale must participate in equivalence, so a
// genuinely different destination index cannot pass as a match.
func TestSignatureDistinguishesRicherOptions(t *testing.T) {
	base := func(extra ...bson.E) bson.Raw {
		d := bson.D{
			{Key: "key", Value: bson.D{{Key: "a", Value: int32(1)}}},
			{Key: "name", Value: "a_1"},
		}
		return mustRaw(t, append(d, extra...))
	}

	cases := []struct {
		name string
		a, b bson.Raw
	}{
		{
			"collation strength differs",
			base(bson.E{Key: "collation", Value: bson.D{{Key: "locale", Value: "en"}, {Key: "strength", Value: int32(1)}}}),
			base(bson.E{Key: "collation", Value: bson.D{{Key: "locale", Value: "en"}, {Key: "strength", Value: int32(2)}}}),
		},
		{
			"hidden differs",
			base(bson.E{Key: "hidden", Value: true}),
			base(),
		},
		{
			"wildcardProjection differs",
			base(bson.E{Key: "wildcardProjection", Value: bson.D{{Key: "x", Value: int32(1)}}}),
			base(bson.E{Key: "wildcardProjection", Value: bson.D{{Key: "y", Value: int32(1)}}}),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			da, err := canonicalizeIndex(tc.a)
			if err != nil {
				t.Fatalf("a: %v", err)
			}
			db, err := canonicalizeIndex(tc.b)
			if err != nil {
				t.Fatalf("b: %v", err)
			}
			if da.Signature() == db.Signature() {
				t.Errorf("signatures must differ:\n a=%s\n b=%s", da.Signature(), db.Signature())
			}
		})
	}
}

// TestCollationVersionIgnored covers the fix's guard against false mismatches:
// the same collation with a different server-assigned "version" must be equal.
func TestCollationVersionIgnored(t *testing.T) {
	mk := func(ver string) bson.Raw {
		return mustRaw(t, bson.D{
			{Key: "key", Value: bson.D{{Key: "a", Value: int32(1)}}},
			{Key: "name", Value: "a_1"},
			{Key: "collation", Value: bson.D{{Key: "locale", Value: "en"}, {Key: "strength", Value: int32(2)}, {Key: "version", Value: ver}}},
		})
	}
	d1, _ := canonicalizeIndex(mk("57.1"))
	d2, _ := canonicalizeIndex(mk("58.0"))
	if d1.Signature() != d2.Signature() {
		t.Errorf("collation version must not affect equivalence:\n %s\n %s", d1.Signature(), d2.Signature())
	}
	if d1.CollationLocale != "en" {
		t.Errorf("CollationLocale = %q, want en", d1.CollationLocale)
	}
}

func TestDiffIndexes(t *testing.T) {
	idx := func(name, key string, unique bool) IndexDescriptor {
		return IndexDescriptor{Name: name, Key: key, Unique: unique}
	}

	tests := []struct {
		name        string
		src, dst    []IndexDescriptor
		wantMissing []string
		wantExtra   []string
		wantMatch   bool
	}{
		{
			name:      "identical",
			src:       []IndexDescriptor{idx("_id_", "_id:1", false), idx("email_1", "email:1", true)},
			dst:       []IndexDescriptor{idx("_id_", "_id:1", false), idx("email_1", "email:1", true)},
			wantMatch: true,
		},
		{
			name:      "name-only difference matches",
			src:       []IndexDescriptor{idx("idx_a", "a:1", false)},
			dst:       []IndexDescriptor{idx("a_1", "a:1", false)},
			wantMatch: true,
		},
		{
			name:        "missing on destination",
			src:         []IndexDescriptor{idx("_id_", "_id:1", false), idx("email_1", "email:1", true)},
			dst:         []IndexDescriptor{idx("_id_", "_id:1", false)},
			wantMissing: []string{"email_1"},
			wantMatch:   false,
		},
		{
			name:      "extra on destination",
			src:       []IndexDescriptor{idx("_id_", "_id:1", false)},
			dst:       []IndexDescriptor{idx("_id_", "_id:1", false), idx("spurious", "junk:1", false)},
			wantExtra: []string{"spurious"},
			wantMatch: false,
		},
		{
			name:        "option difference is not name-only",
			src:         []IndexDescriptor{idx("a_1", "a:1", true)},
			dst:         []IndexDescriptor{idx("a_1", "a:1", false)},
			wantMissing: []string{"a_1"},
			wantExtra:   []string{"a_1"},
			wantMatch:   false,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			missing, extra, match := diffIndexes(tc.src, tc.dst)
			if match != tc.wantMatch {
				t.Errorf("match = %v, want %v", match, tc.wantMatch)
			}
			if !equalStrings(missing, tc.wantMissing) {
				t.Errorf("missing = %v, want %v", missing, tc.wantMissing)
			}
			if !equalStrings(extra, tc.wantExtra) {
				t.Errorf("extra = %v, want %v", extra, tc.wantExtra)
			}
		})
	}
}

func equalStrings(a, b []string) bool {
	if len(a) == 0 && len(b) == 0 {
		return true
	}
	return slices.Equal(a, b)
}
