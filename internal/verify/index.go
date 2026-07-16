package verify

import (
	"encoding/hex"
	"fmt"
	"strconv"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// IndexDescriptor is a canonical, comparable summary of a MongoDB index. Two
// indexes are considered equivalent when their Signature values are equal;
// Signature deliberately excludes the index name so that a name-only difference
// (same key + options) does not register as a mismatch.
type IndexDescriptor struct {
	// Name is the index name, used only for reporting (never for equivalence).
	Name string
	// Key is the ordered key specification, e.g. "a:1,b:-1,loc:2dsphere".
	// Key order is significant and is preserved.
	Key string
	// Unique reports the unique option.
	Unique bool
	// Sparse reports the sparse option.
	Sparse bool
	// HasExpire reports whether an expireAfterSeconds (TTL) option is present.
	HasExpire bool
	// ExpireAfterSeconds is the TTL in seconds when HasExpire is true.
	ExpireAfterSeconds int64
	// PartialFilter is a canonical hex hash of the partialFilterExpression, or
	// "" when the index has no partial filter.
	PartialFilter string
	// CollationLocale is the collation locale, or "" when the index uses the
	// simple (binary) collation.
	CollationLocale string
}

// Signature returns the name-independent equivalence key for the index. Indexes
// with equal signatures are treated as the same index during diffing.
func (d IndexDescriptor) Signature() string {
	ttl := "none"
	if d.HasExpire {
		ttl = strconv.FormatInt(d.ExpireAfterSeconds, 10)
	}
	return fmt.Sprintf(
		"key=%s|unique=%t|sparse=%t|ttl=%s|partial=%s|collation=%s",
		d.Key, d.Unique, d.Sparse, ttl, d.PartialFilter, d.CollationLocale,
	)
}

// canonicalizeIndex converts a raw index specification (as returned by
// IndexView.List) into an IndexDescriptor. It reads the standard name, key,
// unique, sparse, expireAfterSeconds, partialFilterExpression, and collation
// fields; unknown fields are ignored.
func canonicalizeIndex(spec bson.Raw) (IndexDescriptor, error) {
	var d IndexDescriptor

	if name, ok := spec.Lookup("name").StringValueOK(); ok {
		d.Name = name
	}

	keyVal := spec.Lookup("key")
	keyDoc, ok := keyVal.DocumentOK()
	if !ok {
		return IndexDescriptor{}, fmt.Errorf("index %q: missing or malformed key", d.Name)
	}
	key, err := canonicalKey(keyDoc)
	if err != nil {
		return IndexDescriptor{}, fmt.Errorf("index %q: %w", d.Name, err)
	}
	d.Key = key

	if b, ok := spec.Lookup("unique").BooleanOK(); ok {
		d.Unique = b
	}
	if b, ok := spec.Lookup("sparse").BooleanOK(); ok {
		d.Sparse = b
	}
	if secs, ok := spec.Lookup("expireAfterSeconds").AsInt64OK(); ok {
		d.HasExpire = true
		d.ExpireAfterSeconds = secs
	}
	if pfe, ok := spec.Lookup("partialFilterExpression").DocumentOK(); ok {
		h, err := hashDoc(pfe)
		if err != nil {
			return IndexDescriptor{}, fmt.Errorf("index %q: partialFilterExpression: %w", d.Name, err)
		}
		d.PartialFilter = hex.EncodeToString(h[:])
	}
	if coll, ok := spec.Lookup("collation").DocumentOK(); ok {
		if locale, ok := coll.Lookup("locale").StringValueOK(); ok {
			d.CollationLocale = locale
		}
	}

	return d, nil
}

// canonicalKey renders an index key document as an ordered "field:dir" list,
// preserving field order. Direction/type values may be numeric (1, -1) or
// string ("2dsphere", "text", "hashed").
func canonicalKey(keyDoc bson.Raw) (string, error) {
	elems, err := keyDoc.Elements()
	if err != nil {
		return "", fmt.Errorf("read key elements: %w", err)
	}
	parts := make([]string, 0, len(elems))
	for _, e := range elems {
		k, err := e.KeyErr()
		if err != nil {
			return "", fmt.Errorf("read key field: %w", err)
		}
		v, err := e.ValueErr()
		if err != nil {
			return "", fmt.Errorf("read key value for %q: %w", k, err)
		}
		parts = append(parts, k+":"+renderIndexKeyValue(v))
	}
	return strings.Join(parts, ","), nil
}

// renderIndexKeyValue renders an index key direction/type value canonically.
func renderIndexKeyValue(v bson.RawValue) string {
	if s, ok := v.StringValueOK(); ok {
		return s
	}
	if n, ok := v.AsInt64OK(); ok {
		return strconv.FormatInt(n, 10)
	}
	if f, ok := v.AsFloat64OK(); ok {
		return strconv.FormatFloat(f, 'g', -1, 64)
	}
	return v.String()
}

// diffIndexes compares two sets of index descriptors by signature and returns
// the names of indexes present on the source but missing on the destination
// (missing) and present on the destination but absent on the source (extra).
// match is true only when both are empty. Duplicate signatures are matched by
// multiplicity.
func diffIndexes(src, dst []IndexDescriptor) (missing, extra []string, match bool) {
	dstRemaining := make(map[string]int, len(dst))
	for _, d := range dst {
		dstRemaining[d.Signature()]++
	}
	for _, d := range src {
		sig := d.Signature()
		if dstRemaining[sig] > 0 {
			dstRemaining[sig]--
		} else {
			missing = append(missing, d.Name)
		}
	}

	srcRemaining := make(map[string]int, len(src))
	for _, d := range src {
		srcRemaining[d.Signature()]++
	}
	for _, d := range dst {
		sig := d.Signature()
		if srcRemaining[sig] > 0 {
			srcRemaining[sig]--
		} else {
			extra = append(extra, d.Name)
		}
	}

	match = len(missing) == 0 && len(extra) == 0
	return missing, extra, match
}

// canonicalizeIndexes canonicalizes a batch of raw index specs, propagating the
// first error encountered.
func canonicalizeIndexes(specs []bson.Raw) ([]IndexDescriptor, error) {
	out := make([]IndexDescriptor, 0, len(specs))
	for _, s := range specs {
		d, err := canonicalizeIndex(s)
		if err != nil {
			return nil, err
		}
		out = append(out, d)
	}
	return out, nil
}
