package cdc

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/rahadiangg/polymigrate/internal/namespace"
)

// parseEvent converts a raw change-stream event document into a ChangeEvent.
//
// It extracts operationType, ns.{db,coll}, documentKey._id, fullDocument, and
// clusterTime. Missing optional fields are tolerated (a delete has no
// fullDocument; an invalidate has no ns); only a missing or non-string
// operationType is an error.
//
// The returned ID and Full alias raw's backing bytes, so callers that reuse the
// buffer (like the driver's ChangeStream.Current) must pass a copy.
func parseEvent(raw bson.Raw) (ChangeEvent, error) {
	if len(raw) == 0 {
		return ChangeEvent{}, errors.New("cdc: empty change event")
	}

	var e ChangeEvent

	opVal, err := raw.LookupErr("operationType")
	if err != nil {
		return ChangeEvent{}, fmt.Errorf("cdc: change event missing operationType: %w", err)
	}
	op, ok := opVal.StringValueOK()
	if !ok {
		return ChangeEvent{}, fmt.Errorf("cdc: operationType has non-string type %v", opVal.Type)
	}
	e.OpType = op

	// The change event document's own _id IS this event's resume token. Capture its
	// raw document bytes so a resume can restart the stream exactly after it. Like
	// ID and Full, this aliases raw's backing bytes.
	if idVal, err := raw.LookupErr("_id"); err == nil {
		if doc, ok := idVal.DocumentOK(); ok {
			e.Token = []byte(doc)
		}
	}

	if nsVal, err := raw.LookupErr("ns"); err == nil {
		if nsDoc, ok := nsVal.DocumentOK(); ok {
			db, _ := nsDoc.Lookup("db").StringValueOK()
			coll, _ := nsDoc.Lookup("coll").StringValueOK()
			e.NS = namespace.Namespace{DB: db, Collection: coll}
		}
	}

	if dkVal, err := raw.LookupErr("documentKey"); err == nil {
		if dkDoc, ok := dkVal.DocumentOK(); ok {
			if id, err := dkDoc.LookupErr("_id"); err == nil {
				e.ID = id
			}
		}
	}

	if fdVal, err := raw.LookupErr("fullDocument"); err == nil {
		if doc, ok := fdVal.DocumentOK(); ok {
			e.Full = doc
		}
	}

	if ctVal, err := raw.LookupErr("clusterTime"); err == nil {
		if secs, _, ok := ctVal.TimestampOK(); ok {
			e.At = time.Unix(int64(secs), 0).UTC()
		}
	}

	return e, nil
}

// eventToModel maps a data change event to an idempotent bulk write model:
// insert/replace/update become an upserting ReplaceOne keyed by _id using the
// post-image, and delete becomes a DeleteOne keyed by _id. It returns
// (nil, false) for non-data events (DDL, invalidate) and for updates that carry
// no post-image (the document was deleted before the lookup could resolve it).
func eventToModel(e ChangeEvent) (mongo.WriteModel, bool) {
	switch e.OpType {
	case opInsert, opReplace, opUpdate:
		if len(e.Full) == 0 {
			return nil, false
		}
		filter := bson.D{{Key: "_id", Value: e.ID}}
		return mongo.NewReplaceOneModel().
			SetFilter(filter).
			SetReplacement(e.Full).
			SetUpsert(true), true
	case opDelete:
		filter := bson.D{{Key: "_id", Value: e.ID}}
		return mongo.NewDeleteOneModel().SetFilter(filter), true
	default:
		return nil, false
	}
}

// dedupeBatch collapses a batch to at most one event per (namespace, _id),
// keeping the latest event for each key (last-writer-wins): a delete following
// an insert wins, and an insert following a delete wins. Output order follows
// the first appearance of each key, which keeps the result deterministic.
//
// Events with a zero _id are never merged with one another (each is treated as
// distinct), since they carry no document key to deduplicate on.
func dedupeBatch(events []ChangeEvent) []ChangeEvent {
	if len(events) < 2 {
		return events
	}

	latest := make(map[string]int, len(events)) // key -> index into out
	out := make([]ChangeEvent, 0, len(events))
	for _, e := range events {
		if e.ID.IsZero() {
			out = append(out, e)
			continue
		}
		k := idKey(e)
		if idx, ok := latest[k]; ok {
			out[idx] = e
			continue
		}
		latest[k] = len(out)
		out = append(out, e)
	}
	return out
}

// idKey builds a dedup/grouping key from the namespace and the raw _id bytes so
// documents with the same _id in different collections never collide.
func idKey(e ChangeEvent) string {
	var b strings.Builder
	b.Grow(len(e.NS.DB) + len(e.NS.Collection) + len(e.ID.Value) + 3)
	b.WriteString(e.NS.DB)
	b.WriteByte(0)
	b.WriteString(e.NS.Collection)
	b.WriteByte(0)
	b.WriteByte(byte(e.ID.Type))
	b.Write(e.ID.Value)
	return b.String()
}

// matchNS reports whether an event's namespace is in the allowed set. An empty
// allowed set matches everything (whole-deployment tailing).
func matchNS(e ChangeEvent, allowed []namespace.Namespace) bool {
	if len(allowed) == 0 {
		return true
	}
	for _, n := range allowed {
		if e.NS == n {
			return true
		}
	}
	return false
}

// distinctDBs returns the sorted, de-duplicated set of databases referenced by
// the namespaces. It drives the change-stream scope decision: a single database
// can be watched with db.Watch, while zero or many require a client-wide watch.
func distinctDBs(ns []namespace.Namespace) []string {
	seen := make(map[string]struct{}, len(ns))
	var out []string
	for _, n := range ns {
		if n.DB == "" {
			continue
		}
		if _, ok := seen[n.DB]; ok {
			continue
		}
		seen[n.DB] = struct{}{}
		out = append(out, n.DB)
	}
	// Small n; insertion order is stable but sort for determinism.
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j-1] > out[j]; j-- {
			out[j-1], out[j] = out[j], out[j-1]
		}
	}
	return out
}
