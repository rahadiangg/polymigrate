package copier

import (
	"reflect"
	"testing"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// mustRaw marshals a bson.D into a bson.Raw or fails the test.
func mustRaw(t *testing.T, d bson.D) bson.Raw {
	t.Helper()
	b, err := bson.Marshal(d)
	if err != nil {
		t.Fatalf("marshal spec: %v", err)
	}
	return bson.Raw(b)
}

// materializeIndexOptions applies every setter recorded on the builder to a fresh
// IndexOptions so tests can inspect the resulting field values.
func materializeIndexOptions(t *testing.T, b *options.IndexOptionsBuilder) *options.IndexOptions {
	t.Helper()
	io := &options.IndexOptions{}
	if b == nil {
		return io
	}
	for _, f := range b.List() {
		if err := f(io); err != nil {
			t.Fatalf("apply index option: %v", err)
		}
	}
	return io
}

func TestBuildIndexModel_SkipsIDIndex(t *testing.T) {
	spec := mustRaw(t, bson.D{
		{Key: "v", Value: 2},
		{Key: "key", Value: bson.D{{Key: "_id", Value: 1}}},
		{Key: "name", Value: "_id_"},
	})
	_, skip, err := buildIndexModel(spec)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !skip {
		t.Fatalf("expected _id_ index to be skipped")
	}
}

func TestBuildIndexModel_MissingKey(t *testing.T) {
	spec := mustRaw(t, bson.D{{Key: "name", Value: "no_key"}})
	_, _, err := buildIndexModel(spec)
	if err == nil {
		t.Fatalf("expected error for missing key")
	}
}

func TestBuildIndexModel_EmptyKey(t *testing.T) {
	spec := mustRaw(t, bson.D{
		{Key: "key", Value: bson.D{}},
		{Key: "name", Value: "empty"},
	})
	_, _, err := buildIndexModel(spec)
	if err == nil {
		t.Fatalf("expected error for empty key")
	}
}

func TestBuildIndexModel_KeyNotDocument(t *testing.T) {
	spec := mustRaw(t, bson.D{
		{Key: "key", Value: "notadoc"},
		{Key: "name", Value: "bad"},
	})
	_, _, err := buildIndexModel(spec)
	if err == nil {
		t.Fatalf("expected error when key is not a document")
	}
}

func TestBuildIndexModel_KeyOrderPreserved(t *testing.T) {
	spec := mustRaw(t, bson.D{
		{Key: "key", Value: bson.D{
			{Key: "b", Value: int32(1)},
			{Key: "a", Value: int32(-1)},
			{Key: "c", Value: int32(1)},
		}},
		{Key: "name", Value: "b_1_a_-1_c_1"},
	})
	model, skip, err := buildIndexModel(spec)
	if err != nil || skip {
		t.Fatalf("unexpected skip=%v err=%v", skip, err)
	}
	keys, ok := model.Keys.(bson.D)
	if !ok {
		t.Fatalf("Keys is %T, want bson.D", model.Keys)
	}
	wantOrder := []string{"b", "a", "c"}
	if len(keys) != len(wantOrder) {
		t.Fatalf("got %d keys, want %d", len(keys), len(wantOrder))
	}
	for i, k := range wantOrder {
		if keys[i].Key != k {
			t.Errorf("key[%d]=%q, want %q", i, keys[i].Key, k)
		}
	}
}

func ptrDeref[T any](t *testing.T, p *T) T {
	t.Helper()
	if p == nil {
		var zero T
		t.Fatalf("expected non-nil *%T", zero)
	}
	return *p
}

func TestBuildIndexModel_ScalarOptions(t *testing.T) {
	spec := mustRaw(t, bson.D{
		{Key: "v", Value: int32(2)},
		{Key: "key", Value: bson.D{{Key: "email", Value: int32(1)}}},
		{Key: "name", Value: "email_unique"},
		{Key: "unique", Value: true},
		{Key: "sparse", Value: true},
		{Key: "hidden", Value: true},
		{Key: "expireAfterSeconds", Value: int32(3600)},
		{Key: "bits", Value: int32(26)},
	})
	model, skip, err := buildIndexModel(spec)
	if err != nil || skip {
		t.Fatalf("unexpected skip=%v err=%v", skip, err)
	}
	io := materializeIndexOptions(t, model.Options)

	if got := ptrDeref(t, io.Name); got != "email_unique" {
		t.Errorf("Name=%q", got)
	}
	if got := ptrDeref(t, io.Unique); !got {
		t.Errorf("Unique=%v", got)
	}
	if got := ptrDeref(t, io.Sparse); !got {
		t.Errorf("Sparse=%v", got)
	}
	if got := ptrDeref(t, io.Hidden); !got {
		t.Errorf("Hidden=%v", got)
	}
	if got := ptrDeref(t, io.ExpireAfterSeconds); got != 3600 {
		t.Errorf("ExpireAfterSeconds=%d", got)
	}
	if got := ptrDeref(t, io.Bits); got != 26 {
		t.Errorf("Bits=%d", got)
	}
}

// TestBuildIndexModel_NumericTypeCoercion verifies that numbers arriving as
// int64 or double (both legal on the wire) are coerced correctly.
func TestBuildIndexModel_NumericTypeCoercion(t *testing.T) {
	spec := mustRaw(t, bson.D{
		{Key: "key", Value: bson.D{{Key: "ts", Value: int32(1)}}},
		{Key: "name", Value: "ttl"},
		{Key: "expireAfterSeconds", Value: int64(120)},
		{Key: "textIndexVersion", Value: float64(3)},
		{Key: "2dsphereIndexVersion", Value: int64(3)},
	})
	model, _, err := buildIndexModel(spec)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	io := materializeIndexOptions(t, model.Options)
	if got := ptrDeref(t, io.ExpireAfterSeconds); got != 120 {
		t.Errorf("ExpireAfterSeconds=%d, want 120", got)
	}
	if got := ptrDeref(t, io.TextVersion); got != 3 {
		t.Errorf("TextVersion=%d, want 3", got)
	}
	if got := ptrDeref(t, io.SphereVersion); got != 3 {
		t.Errorf("SphereVersion=%d, want 3", got)
	}
}

func TestBuildIndexModel_GeoMinMax(t *testing.T) {
	spec := mustRaw(t, bson.D{
		{Key: "key", Value: bson.D{{Key: "loc", Value: "2d"}}},
		{Key: "name", Value: "loc_2d"},
		{Key: "bits", Value: int32(30)},
		{Key: "min", Value: float64(-180)},
		{Key: "max", Value: float64(180)},
	})
	model, _, err := buildIndexModel(spec)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	io := materializeIndexOptions(t, model.Options)
	if got := ptrDeref(t, io.Min); got != -180 {
		t.Errorf("Min=%v, want -180", got)
	}
	if got := ptrDeref(t, io.Max); got != 180 {
		t.Errorf("Max=%v, want 180", got)
	}
	if got := ptrDeref(t, io.Bits); got != 30 {
		t.Errorf("Bits=%d, want 30", got)
	}
	// The geo index key must round-trip as "2d".
	keys := model.Keys.(bson.D)
	if keys[0].Value != "2d" {
		t.Errorf("key value=%v, want 2d", keys[0].Value)
	}
}

func TestBuildIndexModel_TextIndex(t *testing.T) {
	spec := mustRaw(t, bson.D{
		{Key: "key", Value: bson.D{
			{Key: "_fts", Value: "text"},
			{Key: "_ftsx", Value: int32(1)},
		}},
		{Key: "name", Value: "title_text"},
		{Key: "weights", Value: bson.D{{Key: "title", Value: int32(10)}, {Key: "body", Value: int32(1)}}},
		{Key: "default_language", Value: "english"},
		{Key: "language_override", Value: "lang"},
		{Key: "textIndexVersion", Value: int32(3)},
	})
	model, _, err := buildIndexModel(spec)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	io := materializeIndexOptions(t, model.Options)
	if got := ptrDeref(t, io.DefaultLanguage); got != "english" {
		t.Errorf("DefaultLanguage=%q", got)
	}
	if got := ptrDeref(t, io.LanguageOverride); got != "lang" {
		t.Errorf("LanguageOverride=%q", got)
	}
	if got := ptrDeref(t, io.TextVersion); got != 3 {
		t.Errorf("TextVersion=%d", got)
	}
	if io.Weights == nil {
		t.Errorf("Weights not set")
	}
	weights, ok := io.Weights.(bson.Raw)
	if !ok {
		t.Fatalf("Weights is %T, want bson.Raw", io.Weights)
	}
	if v, ok := weights.Lookup("title").Int32OK(); !ok || v != 10 {
		t.Errorf("weights.title=%v ok=%v, want 10", v, ok)
	}
}

func TestBuildIndexModel_PartialAndWildcard(t *testing.T) {
	partial := bson.D{{Key: "active", Value: true}}
	wildcard := bson.D{{Key: "$**", Value: int32(1)}}
	spec := mustRaw(t, bson.D{
		{Key: "key", Value: bson.D{{Key: "$**", Value: int32(1)}}},
		{Key: "name", Value: "wildcard"},
		{Key: "partialFilterExpression", Value: partial},
		{Key: "wildcardProjection", Value: wildcard},
	})
	model, _, err := buildIndexModel(spec)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	io := materializeIndexOptions(t, model.Options)
	pfe, ok := io.PartialFilterExpression.(bson.Raw)
	if !ok {
		t.Fatalf("PartialFilterExpression is %T, want bson.Raw", io.PartialFilterExpression)
	}
	if v, ok := pfe.Lookup("active").BooleanOK(); !ok || !v {
		t.Errorf("partialFilter.active=%v ok=%v", v, ok)
	}
	if io.WildcardProjection == nil {
		t.Errorf("WildcardProjection not set")
	}
}

func TestBuildIndexModel_Collation(t *testing.T) {
	spec := mustRaw(t, bson.D{
		{Key: "key", Value: bson.D{{Key: "name", Value: int32(1)}}},
		{Key: "name", Value: "name_ci"},
		{Key: "collation", Value: bson.D{
			{Key: "locale", Value: "en_US"},
			{Key: "strength", Value: int32(2)},
			{Key: "caseLevel", Value: true},
			{Key: "caseFirst", Value: "upper"},
			{Key: "numericOrdering", Value: true},
			{Key: "alternate", Value: "shifted"},
			{Key: "maxVariable", Value: "punct"},
			{Key: "normalization", Value: true},
			{Key: "backwards", Value: true},
		}},
	})
	model, _, err := buildIndexModel(spec)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	io := materializeIndexOptions(t, model.Options)
	if io.Collation == nil {
		t.Fatalf("Collation not set")
	}
	want := &options.Collation{
		Locale:          "en_US",
		Strength:        2,
		CaseLevel:       true,
		CaseFirst:       "upper",
		NumericOrdering: true,
		Alternate:       "shifted",
		MaxVariable:     "punct",
		Normalization:   true,
		Backwards:       true,
	}
	if !reflect.DeepEqual(io.Collation, want) {
		t.Errorf("collation mismatch\n got: %+v\nwant: %+v", io.Collation, want)
	}
}

func TestBuildIndexModel_MinimalIndexNoOptions(t *testing.T) {
	// An index with only key+name should copy cleanly with just the name set.
	spec := mustRaw(t, bson.D{
		{Key: "key", Value: bson.D{{Key: "x", Value: int32(1)}}},
		{Key: "name", Value: "x_1"},
	})
	model, skip, err := buildIndexModel(spec)
	if err != nil || skip {
		t.Fatalf("unexpected skip=%v err=%v", skip, err)
	}
	io := materializeIndexOptions(t, model.Options)
	if got := ptrDeref(t, io.Name); got != "x_1" {
		t.Errorf("Name=%q", got)
	}
	if io.Unique != nil {
		t.Errorf("Unique should be unset, got %v", *io.Unique)
	}
	if io.Collation != nil {
		t.Errorf("Collation should be unset")
	}
}

// TestParseCollation_Partial verifies only present fields are populated.
func TestParseCollation_Partial(t *testing.T) {
	raw := mustRaw(t, bson.D{
		{Key: "locale", Value: "fr"},
		{Key: "strength", Value: int64(3)},
	})
	c := parseCollation(raw)
	if c.Locale != "fr" {
		t.Errorf("Locale=%q", c.Locale)
	}
	if c.Strength != 3 {
		t.Errorf("Strength=%d", c.Strength)
	}
	if c.CaseLevel || c.Backwards || c.CaseFirst != "" {
		t.Errorf("unexpected non-zero fields: %+v", c)
	}
}
