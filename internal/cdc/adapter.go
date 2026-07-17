package cdc

import (
	"context"
	"errors"
	"fmt"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/rahadiangg/polymigrate/internal/engine"
	"github.com/rahadiangg/polymigrate/internal/mongox"
	"github.com/rahadiangg/polymigrate/internal/namespace"
	"github.com/rahadiangg/polymigrate/internal/retry"
)

// openChangeStream opens a change stream on the source with FullDocument set to
// updateLookup (so updates carry a post-image) and, when scoped to exactly one
// database, uses db.Watch; otherwise it uses a client-wide watch. When a resume
// token is supplied the stream resumes after it.
func openChangeStream(ctx context.Context, src *mongox.Client, opts engine.TailOptions) (eventSource, error) {
	if src == nil {
		return nil, errors.New("cdc: nil source client")
	}

	csOpts := options.ChangeStream().SetFullDocument(options.UpdateLookup)
	if !opts.Resume.IsZero() {
		csOpts = csOpts.SetResumeAfter(bson.Raw(opts.Resume))
	}

	var (
		cs  *mongo.ChangeStream
		err error
	)
	if dbs := distinctDBs(opts.Namespaces); len(dbs) == 1 {
		cs, err = src.Client.Database(dbs[0]).Watch(ctx, mongo.Pipeline{}, csOpts)
	} else {
		cs, err = src.Client.Watch(ctx, mongo.Pipeline{}, csOpts)
	}
	if err != nil {
		return nil, err
	}
	return &changeStreamSource{cs: cs}, nil
}

// changeStreamSource adapts *mongo.ChangeStream to the eventSource interface.
type changeStreamSource struct {
	cs *mongo.ChangeStream
}

// Next advances the stream and parses the current event. It copies the driver's
// transient Current buffer before parsing, since parseEvent aliases those bytes
// and the driver reuses the buffer on the next call.
func (s *changeStreamSource) Next(ctx context.Context) (ChangeEvent, bool, error) {
	if !s.cs.Next(ctx) {
		return ChangeEvent{}, false, s.cs.Err()
	}
	raw := make(bson.Raw, len(s.cs.Current))
	copy(raw, s.cs.Current)
	ev, err := parseEvent(raw)
	if err != nil {
		return ChangeEvent{}, false, err
	}
	return ev, true, nil
}

func (s *changeStreamSource) ResumeToken() []byte {
	return []byte(s.cs.ResumeToken())
}

func (s *changeStreamSource) Close(ctx context.Context) error {
	return s.cs.Close(ctx)
}

// bulkApplier applies batches to the destination with unordered BulkWrites,
// grouped per collection. It tolerates duplicate-key errors (benign on replay).
type bulkApplier struct {
	dst *mongox.Client
}

func newBulkApplier(dst *mongox.Client) *bulkApplier {
	return &bulkApplier{dst: dst}
}

type nsGroup struct {
	ns      namespace.Namespace
	models  []mongo.WriteModel
	inserts int
	deletes int
}

func (a *bulkApplier) apply(ctx context.Context, batch []ChangeEvent) (int, int, error) {
	if a.dst == nil {
		return 0, 0, errors.New("cdc: nil destination client")
	}

	// Group by namespace: a single BulkWrite targets one collection.
	order := make([]namespace.Namespace, 0)
	groups := make(map[namespace.Namespace]*nsGroup)
	for _, e := range batch {
		m, ok := eventToModel(e)
		if !ok {
			continue
		}
		g := groups[e.NS]
		if g == nil {
			g = &nsGroup{ns: e.NS}
			groups[e.NS] = g
			order = append(order, e.NS)
		}
		g.models = append(g.models, m)
		if e.OpType == opDelete {
			g.deletes++
		} else {
			g.inserts++
		}
	}

	var inserts, deletes int
	for _, ns := range order {
		g := groups[ns]
		if len(g.models) == 0 {
			continue
		}
		coll := a.dst.Client.Database(ns.DB).Collection(ns.Collection)
		// Retry transient failures; the batch is idempotent (ReplaceOne-upsert /
		// DeleteOne keyed by _id) so re-issuing it converges to the same state.
		err := retry.Do(ctx, retry.DefaultPolicy(), retry.MongoClassifier, func() error {
			_, e := coll.BulkWrite(ctx, g.models, options.BulkWrite().SetOrdered(false))
			return e
		})
		if err != nil && !isOnlyDuplicateKeyErrors(err) {
			return inserts, deletes, fmt.Errorf("cdc: bulk write %s: %w", ns, err)
		}
		inserts += g.inserts
		deletes += g.deletes
	}
	return inserts, deletes, nil
}

// codeChangeStreamHistoryLost is the MongoDB server error code (286) returned when
// the oplog no longer retains the change-stream resume point.
const codeChangeStreamHistoryLost = 286

// isChangeStreamHistoryLost reports whether err is (or wraps) a MongoDB
// ChangeStreamHistoryLost server error (code 286). Such an error is unrecoverable:
// the resume token predates the oldest available oplog entry, so the stream cannot
// resume and a fresh snapshot is required.
func isChangeStreamHistoryLost(err error) bool {
	if err == nil {
		return false
	}
	var se mongo.ServerError
	if errors.As(err, &se) {
		return se.HasErrorCode(codeChangeStreamHistoryLost)
	}
	return false
}

// isOnlyDuplicateKeyErrors reports whether err is a bulk-write failure whose
// every write error is a duplicate key (code 11000). Such errors are safe to
// ignore during an idempotent replay.
func isOnlyDuplicateKeyErrors(err error) bool {
	var bwe mongo.BulkWriteException
	if errors.As(err, &bwe) {
		if len(bwe.WriteErrors) == 0 || bwe.WriteConcernError != nil {
			return false
		}
		for _, we := range bwe.WriteErrors {
			if we.Code != codeDuplicateKey {
				return false
			}
		}
		return true
	}
	return mongo.IsDuplicateKeyError(err)
}
