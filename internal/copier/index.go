package copier

import (
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// buildIndexModel reconstructs a mongo.IndexModel from a raw index specification
// as returned by listIndexes. It is a pure function so it can be exhaustively
// unit-tested without a live server.
//
// It reports skip=true for the implicit "_id_" index, which must never be
// recreated (MongoDB creates it automatically and rejects an explicit build).
// It returns an error only when the spec is structurally unusable — missing or
// empty "key". All recognized, present options are copied onto the returned
// model's Options; absent or wrongly-typed fields are silently ignored so an
// exotic index still copies its core shape.
//
// Recognized fields: key, name, unique, sparse, expireAfterSeconds,
// partialFilterExpression, collation, weights, default_language,
// textIndexVersion, 2dsphereIndexVersion, bits, min, max, wildcardProjection,
// hidden.
func buildIndexModel(spec bson.Raw) (model mongo.IndexModel, skip bool, err error) {
	name, _ := spec.Lookup("name").StringValueOK()
	if name == "_id_" {
		return mongo.IndexModel{}, true, nil
	}

	keyVal, kerr := spec.LookupErr("key")
	if kerr != nil {
		return mongo.IndexModel{}, false, fmt.Errorf("index %q: missing key: %w", name, kerr)
	}
	keyDoc, ok := keyVal.DocumentOK()
	if !ok {
		return mongo.IndexModel{}, false, fmt.Errorf("index %q: key is not a document", name)
	}
	keys, kerr := rawToD(keyDoc)
	if kerr != nil {
		return mongo.IndexModel{}, false, fmt.Errorf("index %q: decode key: %w", name, kerr)
	}
	if len(keys) == 0 {
		return mongo.IndexModel{}, false, fmt.Errorf("index %q: empty key", name)
	}

	opt := options.Index()
	if name != "" {
		opt.SetName(name)
	}
	if v, ok := spec.Lookup("unique").BooleanOK(); ok {
		opt.SetUnique(v)
	}
	if v, ok := spec.Lookup("sparse").BooleanOK(); ok {
		opt.SetSparse(v)
	}
	if v, ok := spec.Lookup("expireAfterSeconds").AsInt64OK(); ok {
		opt.SetExpireAfterSeconds(int32(v))
	}
	if v, ok := spec.Lookup("partialFilterExpression").DocumentOK(); ok {
		opt.SetPartialFilterExpression(v)
	}
	if v, ok := spec.Lookup("collation").DocumentOK(); ok {
		opt.SetCollation(parseCollation(v))
	}
	if v, ok := spec.Lookup("weights").DocumentOK(); ok {
		opt.SetWeights(v)
	}
	if v, ok := spec.Lookup("default_language").StringValueOK(); ok {
		opt.SetDefaultLanguage(v)
	}
	if v, ok := spec.Lookup("language_override").StringValueOK(); ok {
		opt.SetLanguageOverride(v)
	}
	if v, ok := spec.Lookup("textIndexVersion").AsInt64OK(); ok {
		opt.SetTextVersion(int32(v))
	}
	if v, ok := spec.Lookup("2dsphereIndexVersion").AsInt64OK(); ok {
		opt.SetSphereVersion(int32(v))
	}
	if v, ok := spec.Lookup("bits").AsInt64OK(); ok {
		opt.SetBits(int32(v))
	}
	if v, ok := spec.Lookup("min").AsFloat64OK(); ok {
		opt.SetMin(v)
	}
	if v, ok := spec.Lookup("max").AsFloat64OK(); ok {
		opt.SetMax(v)
	}
	if v, ok := spec.Lookup("wildcardProjection").DocumentOK(); ok {
		opt.SetWildcardProjection(v)
	}
	if v, ok := spec.Lookup("hidden").BooleanOK(); ok {
		opt.SetHidden(v)
	}

	return mongo.IndexModel{Keys: keys, Options: opt}, false, nil
}

// rawToD decodes a raw BSON document into an order-preserving bson.D, which is
// required for index keys (order is significant).
func rawToD(raw bson.Raw) (bson.D, error) {
	var d bson.D
	if err := bson.Unmarshal(raw, &d); err != nil {
		return nil, err
	}
	return d, nil
}

// parseCollation extracts the collation fields from a raw collation subdocument
// into *options.Collation. It reads fields explicitly (rather than via generic
// unmarshaling) because the server's camelCase keys do not round-trip through the
// driver's default field-name lowercasing.
func parseCollation(raw bson.Raw) *options.Collation {
	c := &options.Collation{}
	if v, ok := raw.Lookup("locale").StringValueOK(); ok {
		c.Locale = v
	}
	if v, ok := raw.Lookup("caseLevel").BooleanOK(); ok {
		c.CaseLevel = v
	}
	if v, ok := raw.Lookup("caseFirst").StringValueOK(); ok {
		c.CaseFirst = v
	}
	if v, ok := raw.Lookup("strength").AsInt64OK(); ok {
		c.Strength = int(v)
	}
	if v, ok := raw.Lookup("numericOrdering").BooleanOK(); ok {
		c.NumericOrdering = v
	}
	if v, ok := raw.Lookup("alternate").StringValueOK(); ok {
		c.Alternate = v
	}
	if v, ok := raw.Lookup("maxVariable").StringValueOK(); ok {
		c.MaxVariable = v
	}
	if v, ok := raw.Lookup("normalization").BooleanOK(); ok {
		c.Normalization = v
	}
	if v, ok := raw.Lookup("backwards").BooleanOK(); ok {
		c.Backwards = v
	}
	return c
}
