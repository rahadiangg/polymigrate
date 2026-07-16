package copier

import (
	"context"
	"errors"
	"strings"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/weefer/mongomigrate/internal/mongox"
	"github.com/weefer/mongomigrate/internal/namespace"
)

// dupKeyCode is the MongoDB duplicate-key error code.
const dupKeyCode = 11000

// Index-creation conflict codes that mean "an index like this already exists".
// Recreating an identical index is a server-side no-op, but re-running against a
// partially-migrated destination can surface these; we tolerate them.
const (
	codeIndexOptionsConflict  = 85
	codeIndexKeySpecsConflict = 86
	codeIndexAlreadyExists    = 68
)

// mongoFactory is the production collFactory: it produces adapters over a pair of
// live *mongox.Client connections.
type mongoFactory struct {
	src, dst *mongox.Client
}

// newMongoFactory builds the real driver-backed factory.
func newMongoFactory(src, dst *mongox.Client) mongoFactory {
	return mongoFactory{src: src, dst: dst}
}

func (f mongoFactory) srcColl(ns namespace.Namespace) srcColl {
	return &realSrcColl{coll: f.src.Client.Database(ns.DB).Collection(ns.Collection)}
}

func (f mongoFactory) dstColl(ns namespace.Namespace) dstColl {
	return &realDstColl{coll: f.dst.Client.Database(ns.DB).Collection(ns.Collection)}
}

// realSrcColl adapts a *mongo.Collection to srcColl.
type realSrcColl struct {
	coll *mongo.Collection
}

func (s *realSrcColl) count(ctx context.Context) (int64, error) {
	return s.coll.CountDocuments(ctx, bson.D{})
}

func (s *realSrcColl) find(ctx context.Context, batch int) (docCursor, error) {
	fo := options.Find().SetSort(bson.D{{Key: "_id", Value: 1}})
	if batch > 0 {
		fo.SetBatchSize(int32(batch))
	}
	cur, err := s.coll.Find(ctx, bson.D{}, fo)
	if err != nil {
		return nil, err
	}
	return &realCursor{cur: cur}, nil
}

func (s *realSrcColl) listIndexes(ctx context.Context) ([]bson.Raw, error) {
	cur, err := s.coll.Indexes().List(ctx)
	if err != nil {
		return nil, err
	}
	var specs []bson.Raw
	if err := cur.All(ctx, &specs); err != nil {
		return nil, err
	}
	return specs, nil
}

// realCursor adapts a *mongo.Cursor to docCursor.
type realCursor struct {
	cur *mongo.Cursor
}

func (c *realCursor) Next(ctx context.Context) bool   { return c.cur.Next(ctx) }
func (c *realCursor) Decode(v *bson.Raw) error        { return c.cur.Decode(v) }
func (c *realCursor) Err() error                      { return c.cur.Err() }
func (c *realCursor) Close(ctx context.Context) error { return c.cur.Close(ctx) }

// realDstColl adapts a *mongo.Collection to dstColl.
type realDstColl struct {
	coll *mongo.Collection
}

func (d *realDstColl) drop(ctx context.Context) error {
	return d.coll.Drop(ctx)
}

func (d *realDstColl) count(ctx context.Context) (int64, error) {
	return d.coll.CountDocuments(ctx, bson.D{})
}

// writeBatch inserts docs unordered. A BulkWriteException is decomposed into
// duplicate-key hits (already present) and other write failures; neither aborts
// the collection, so the returned error is nil in that case. Any non-bulk error
// (connection loss, etc.) is fatal and returned with every doc counted failed.
func (d *realDstColl) writeBatch(ctx context.Context, docs []bson.Raw) (inserted, dupes, failed int, err error) {
	if len(docs) == 0 {
		return 0, 0, 0, nil
	}
	payload := make([]any, len(docs))
	for i := range docs {
		payload[i] = docs[i]
	}
	_, werr := d.coll.InsertMany(ctx, payload, options.InsertMany().SetOrdered(false))
	if werr == nil {
		return len(docs), 0, 0, nil
	}

	var bwe mongo.BulkWriteException
	if errors.As(werr, &bwe) {
		for _, we := range bwe.WriteErrors {
			if we.Code == dupKeyCode {
				dupes++
			} else {
				failed++
			}
		}
		inserted = len(docs) - dupes - failed
		if inserted < 0 {
			inserted = 0
		}
		return inserted, dupes, failed, nil
	}

	// Non-bulk (e.g. transport) error: treat the whole batch as failed and abort.
	return 0, 0, len(docs), werr
}

// createIndexes builds each index individually so that a single problematic
// index does not prevent the rest from being created. Already-existing indexes
// are ignored; any other per-index error is collected and reporting continues
// with the next index, so one un-buildable index (e.g. a source-only feature the
// destination rejects) cannot silently suppress every index after it. The joined
// error is returned at the end alongside the count actually created.
func (d *realDstColl) createIndexes(ctx context.Context, models []mongo.IndexModel) (int, error) {
	created := 0
	iv := d.coll.Indexes()
	var errs []error
	for _, m := range models {
		if _, err := iv.CreateMany(ctx, []mongo.IndexModel{m}); err != nil {
			if indexAlreadyExists(err) {
				continue
			}
			errs = append(errs, err)
			continue
		}
		created++
	}
	return created, errors.Join(errs...)
}

// indexAlreadyExists reports whether err indicates the index (or an equivalent)
// is already present on the destination.
func indexAlreadyExists(err error) bool {
	var ce mongo.CommandError
	if errors.As(err, &ce) {
		switch int(ce.Code) {
		case codeIndexOptionsConflict, codeIndexKeySpecsConflict, codeIndexAlreadyExists:
			return true
		}
	}
	return strings.Contains(err.Error(), "already exists")
}

// Compile-time assertions that the real adapters satisfy the in-package
// interfaces.
var (
	_ collFactory = mongoFactory{}
	_ srcColl     = (*realSrcColl)(nil)
	_ docCursor   = (*realCursor)(nil)
	_ dstColl     = (*realDstColl)(nil)
)
