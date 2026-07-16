// Package verify implements engine.Verifier: it proves that a migration
// destination faithfully reproduces its source by comparing document counts,
// index definitions, and a content-hashed sample of documents per namespace.
//
// Verification is where a migration is won or lost, so the interesting logic
// (canonical document hashing, index canonicalization and diffing, sample
// comparison) lives in small pure functions that are unit-tested directly,
// while all driver access sits behind a narrow store interface with an
// in-memory fake for orchestration tests.
package verify

import (
	"crypto/sha256"
	"fmt"
	"io"
	"sort"

	"go.mongodb.org/mongo-driver/v2/bson"
)

// hashDoc computes a canonical SHA-256 content hash of a BSON document.
//
// Canonicalization sorts the keys of every (sub-)document so that two documents
// that differ only in field order hash identically, while leaving array
// elements in their original order (array order is semantically significant).
// Any change to a value — including its BSON type — changes the hash.
func hashDoc(raw bson.Raw) ([32]byte, error) {
	h := sha256.New()
	if err := hashDocInto(h, raw); err != nil {
		return [32]byte{}, err
	}
	var out [32]byte
	copy(out[:], h.Sum(nil))
	return out, nil
}

// hashDocInto writes the canonical encoding of a document into h. Keys are
// sorted; the document is framed so that {a:{b:1}} and {a:1,b:1}-shaped inputs
// cannot collide.
func hashDocInto(h io.Writer, raw bson.Raw) error {
	elems, err := raw.Elements()
	if err != nil {
		return fmt.Errorf("read document elements: %w", err)
	}

	type kv struct {
		key string
		val bson.RawValue
	}
	kvs := make([]kv, 0, len(elems))
	for _, e := range elems {
		k, err := e.KeyErr()
		if err != nil {
			return fmt.Errorf("read element key: %w", err)
		}
		v, err := e.ValueErr()
		if err != nil {
			return fmt.Errorf("read element value for %q: %w", k, err)
		}
		kvs = append(kvs, kv{key: k, val: v})
	}
	sort.Slice(kvs, func(i, j int) bool { return kvs[i].key < kvs[j].key })

	_, _ = io.WriteString(h, "D{")
	for _, e := range kvs {
		_, _ = io.WriteString(h, e.key)
		_, _ = h.Write([]byte{0x00}) // key/value separator, illegal in BSON keys
		if err := hashValueInto(h, e.val); err != nil {
			return err
		}
	}
	_, _ = io.WriteString(h, "}")
	return nil
}

// hashValueInto writes the canonical encoding of a single value into h,
// recursing into embedded documents (keys sorted) and arrays (order preserved).
func hashValueInto(h io.Writer, v bson.RawValue) error {
	switch v.Type {
	case bson.TypeEmbeddedDocument:
		doc, ok := v.DocumentOK()
		if !ok {
			return fmt.Errorf("malformed embedded document")
		}
		return hashDocInto(h, doc)
	case bson.TypeArray:
		arr, ok := v.ArrayOK()
		if !ok {
			return fmt.Errorf("malformed array")
		}
		vals, err := arr.Values()
		if err != nil {
			return fmt.Errorf("read array values: %w", err)
		}
		_, _ = io.WriteString(h, "A[")
		for _, av := range vals {
			if err := hashValueInto(h, av); err != nil {
				return err
			}
			_, _ = h.Write([]byte{0x00})
		}
		_, _ = io.WriteString(h, "]")
		return nil
	default:
		// Leaf value: the BSON type byte plus the raw value bytes fully and
		// unambiguously determine the value, so include both.
		_, _ = h.Write([]byte{byte(v.Type)})
		_, _ = h.Write(v.Value)
		return nil
	}
}

// idKey derives a stable, collision-free map key from an _id value. The BSON
// type byte is prefixed so that values sharing a byte pattern under different
// types (e.g. int32 vs int64) do not collide. Identical _id values on source
// and destination always produce the same key.
func idKey(v bson.RawValue) string {
	b := make([]byte, 0, len(v.Value)+1)
	b = append(b, byte(v.Type))
	b = append(b, v.Value...)
	return string(b)
}

// copyRawValue returns a RawValue whose bytes are independent of any cursor or
// document backing array, so it is safe to retain after the source is reused.
func copyRawValue(rv bson.RawValue) bson.RawValue {
	b := make([]byte, len(rv.Value))
	copy(b, rv.Value)
	return bson.RawValue{Type: rv.Type, Value: b}
}

// compareSamples compares source documents against destination documents by
// canonical content hash, keyed by idKey. It returns the number of source
// documents checked and how many mismatched. A destination document that is
// absent, unreadable, or whose content differs counts as a mismatch.
func compareSamples(srcDocs, dstDocs map[string]bson.Raw) (checked, mismatches int) {
	for key, srcDoc := range srcDocs {
		checked++
		dstDoc, ok := dstDocs[key]
		if !ok {
			mismatches++
			continue
		}
		srcHash, err := hashDoc(srcDoc)
		if err != nil {
			mismatches++
			continue
		}
		dstHash, err := hashDoc(dstDoc)
		if err != nil {
			mismatches++
			continue
		}
		if srcHash != dstHash {
			mismatches++
		}
	}
	return checked, mismatches
}
