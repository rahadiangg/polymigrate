// Package copier implements the native parallel snapshot copier: it streams
// every document of a fixed set of namespaces from a source deployment to a
// destination, several collections at a time, and optionally recreates the
// source indexes on the destination.
//
// The orchestration is written against three narrow, in-package interfaces
// (srcColl, docCursor, dstColl) obtained from a collFactory, so the full copy
// pipeline can be unit-tested with in-memory fakes and no live MongoDB. The real
// factory (newMongoFactory) is a thin adapter over the official driver and lives
// in adapter.go.
package copier

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/weefer/mongomigrate/internal/engine"
	"github.com/weefer/mongomigrate/internal/mongox"
	"github.com/weefer/mongomigrate/internal/namespace"
)

// defaultBatchSize is used when CopyOptions.BatchSize is not a positive number.
const defaultBatchSize = 1000

// -----------------------------------------------------------------------------
// Narrow driver-facing interfaces. Everything the copier does against MongoDB is
// funneled through these so tests can substitute in-memory fakes.
// -----------------------------------------------------------------------------

// srcColl is a read handle for one source collection.
type srcColl interface {
	// count returns the exact document count (used as the progress total).
	count(ctx context.Context) (int64, error)
	// find opens a cursor over all documents sorted by _id. batch is the source
	// cursor batch size; <=0 means the driver default.
	find(ctx context.Context, batch int) (docCursor, error)
	// listIndexes returns the raw index specifications for this collection.
	listIndexes(ctx context.Context) ([]bson.Raw, error)
}

// docCursor is a forward-only cursor over raw BSON documents. It mirrors the
// subset of *mongo.Cursor the copier needs.
type docCursor interface {
	Next(ctx context.Context) bool
	Decode(*bson.Raw) error
	Err() error
	Close(ctx context.Context) error
}

// dstColl is a write handle for one destination collection.
type dstColl interface {
	// drop removes the collection and all of its data and indexes.
	drop(ctx context.Context) error
	// count returns the exact document count.
	count(ctx context.Context) (int64, error)
	// writeBatch writes docs to the destination. It reports how many documents
	// were newly inserted, how many were already present (duplicate key, code
	// 11000), and how many failed for other write-level reasons. A non-nil error
	// signals a fatal problem (e.g. the connection dropped) that should abort the
	// collection; per-document write errors are reported via the failed count with
	// a nil error so the copy can continue.
	writeBatch(ctx context.Context, docs []bson.Raw) (inserted, dupes, failed int, err error)
	// createIndexes creates the given index models, tolerating any that already
	// exist, and returns how many were created.
	createIndexes(ctx context.Context, models []mongo.IndexModel) (int, error)
}

// collFactory hands out source and destination handles for a namespace. The real
// implementation wraps a pair of *mongox.Client; tests inject a fake.
type collFactory interface {
	srcColl(ns namespace.Namespace) srcColl
	dstColl(ns namespace.Namespace) dstColl
}

// -----------------------------------------------------------------------------
// Copier
// -----------------------------------------------------------------------------

// Copier performs the bulk snapshot copy of a fixed set of namespaces from a
// source deployment to a destination. It satisfies engine.Copier.
type Copier struct {
	factory  collFactory
	reporter engine.Reporter
	log      *slog.Logger
}

// New constructs a Copier that copies from src to dst. A nil reporter or logger
// is replaced with a no-op implementation, so callers need not guard them.
func New(src, dst *mongox.Client, reporter engine.Reporter, log *slog.Logger) *Copier {
	return newWithFactory(newMongoFactory(src, dst), reporter, log)
}

// newWithFactory is the injectable constructor used by tests to supply a fake
// factory. It is the single place defaults are applied.
func newWithFactory(f collFactory, reporter engine.Reporter, log *slog.Logger) *Copier {
	if reporter == nil {
		reporter = engine.NopReporter{}
	}
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Copier{factory: f, reporter: reporter, log: log}
}

// Copy copies every namespace in opts.Namespaces from source to destination,
// opts.Workers collections concurrently. Every namespace is attempted; a
// per-collection failure is recorded in its CollectionCopyResult.Err rather than
// aborting the whole run. Copy returns a non-nil error only for a fatal
// setup-level problem, currently just a cancelled context.
func (c *Copier) Copy(ctx context.Context, opts engine.CopyOptions) (engine.CopyResult, error) {
	start := time.Now()

	// A cancelled context is a fatal setup problem: bail before touching anything.
	if err := ctx.Err(); err != nil {
		return engine.CopyResult{Duration: time.Since(start)}, fmt.Errorf("copy: %w", err)
	}

	workers := opts.Workers
	if workers < 1 {
		workers = 1
	}
	batchSize := opts.BatchSize
	if batchSize < 1 {
		batchSize = defaultBatchSize
	}

	nss := opts.Namespaces
	results := make([]engine.CollectionCopyResult, len(nss))

	// Fan out namespaces over a fixed worker pool. Each worker writes to a
	// distinct results index, so no synchronization on the slice is needed.
	jobs := make(chan int)
	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				results[i] = c.copyCollection(ctx, nss[i], opts, batchSize)
			}
		}()
	}
	for i := range nss {
		jobs <- i
	}
	close(jobs)
	wg.Wait()

	res := engine.CopyResult{Collections: results}
	for _, r := range results {
		res.TotalDocs += r.DocsCopied
		res.TotalFailed += r.DocsFailed
	}
	res.Duration = time.Since(start)

	if err := ctx.Err(); err != nil {
		return res, fmt.Errorf("copy: %w", err)
	}
	return res, nil
}

// copyCollection copies a single namespace and returns its outcome. It never
// returns an error; failures are captured in the result's Err field.
func (c *Copier) copyCollection(ctx context.Context, ns namespace.Namespace, opts engine.CopyOptions, batchSize int) engine.CollectionCopyResult {
	start := time.Now()
	res := engine.CollectionCopyResult{Namespace: ns}

	finish := func() engine.CollectionCopyResult {
		res.Duration = time.Since(start)
		c.reporter.Event("collection finished",
			"ns", ns.String(),
			"copied", res.DocsCopied,
			"failed", res.DocsFailed,
			"indexes", res.IndexesCopied,
			"skipped", res.Skipped,
			"err", res.Err,
		)
		return res
	}

	if err := ctx.Err(); err != nil {
		res.Err = fmt.Errorf("copy %s: %w", ns, err).Error()
		return finish()
	}

	c.reporter.Event("collection started", "ns", ns.String())

	src := c.factory.srcColl(ns)
	dst := c.factory.dstColl(ns)

	// Prepare the destination: drop, or skip a non-empty target.
	if opts.DropTarget {
		if err := dst.drop(ctx); err != nil {
			res.Err = fmt.Errorf("drop %s: %w", ns, err).Error()
			return finish()
		}
	} else if opts.SkipExistingNonEmpty {
		n, err := dst.count(ctx)
		if err != nil {
			res.Err = fmt.Errorf("count destination %s: %w", ns, err).Error()
			return finish()
		}
		if n > 0 {
			res.Skipped = true
			c.reporter.Event("collection skipped (non-empty destination)", "ns", ns.String(), "existing", n)
			return finish()
		}
	}

	// Best-effort exact total for progress reporting; -1 when unavailable.
	total := int64(-1)
	if n, err := src.count(ctx); err == nil {
		total = n
	}

	if err := c.copyData(ctx, ns, src, dst, opts.ReadBatchSize, batchSize, total, &res); err != nil {
		// copyData already recorded res.Err; nothing more to do but finish.
		return finish()
	}

	if opts.CopyIndexes {
		c.copyIndexes(ctx, ns, src, dst, &res)
	}

	return finish()
}

// copyData streams the source cursor into batched destination writes, updating
// res as it goes. It returns a non-nil error (and sets res.Err) only for a fatal
// per-collection problem; in-batch document failures are accumulated into
// res.DocsFailed and are not returned as errors.
func (c *Copier) copyData(ctx context.Context, ns namespace.Namespace, src srcColl, dst dstColl, readBatch, batchSize int, total int64, res *engine.CollectionCopyResult) error {
	cur, err := src.find(ctx, readBatch)
	if err != nil {
		res.Err = fmt.Errorf("open source cursor %s: %w", ns, err).Error()
		return err
	}
	// Close on a background context so the cursor is released even when ctx is
	// already cancelled.
	defer func() { _ = cur.Close(context.Background()) }()

	batch := make([]bson.Raw, 0, batchSize)

	flush := func() error {
		if len(batch) == 0 {
			return nil
		}
		inserted, dupes, failed, werr := dst.writeBatch(ctx, batch)
		// Duplicates are treated as already-present documents, so they count as
		// copied — this keeps re-runs without --drop safe.
		res.DocsCopied += int64(inserted) + int64(dupes)
		res.DocsFailed += int64(failed)
		batch = batch[:0]
		if werr != nil {
			res.Err = fmt.Errorf("write batch %s: %w", ns, werr).Error()
			return werr
		}
		c.reporter.Progress(ns, res.DocsCopied, total)
		return nil
	}

	for cur.Next(ctx) {
		// Promptly honor cancellation between documents.
		if err := ctx.Err(); err != nil {
			res.Err = fmt.Errorf("copy %s: %w", ns, err).Error()
			return err
		}
		var raw bson.Raw
		if err := cur.Decode(&raw); err != nil {
			res.Err = fmt.Errorf("decode document in %s: %w", ns, err).Error()
			return err
		}
		batch = append(batch, raw)
		if len(batch) >= batchSize {
			if err := flush(); err != nil {
				return err
			}
		}
	}
	if err := cur.Err(); err != nil {
		res.Err = fmt.Errorf("read source %s: %w", ns, err).Error()
		return err
	}
	if err := flush(); err != nil {
		return err
	}
	return nil
}

// copyIndexes reads the source index specifications, reconstructs index models
// (always skipping the implicit _id_ index), and creates them on the
// destination. Failures are recorded in res.Err but do not abort other work.
func (c *Copier) copyIndexes(ctx context.Context, ns namespace.Namespace, src srcColl, dst dstColl, res *engine.CollectionCopyResult) {
	specs, err := src.listIndexes(ctx)
	if err != nil {
		if res.Err == "" {
			res.Err = fmt.Errorf("list indexes %s: %w", ns, err).Error()
		}
		return
	}
	models := make([]mongo.IndexModel, 0, len(specs))
	for _, spec := range specs {
		model, skip, err := buildIndexModel(spec)
		if err != nil {
			// A malformed spec should not sink the whole collection; skip it.
			c.log.Warn("skipping unparseable index spec", "ns", ns.String(), "err", err)
			continue
		}
		if skip {
			continue
		}
		models = append(models, model)
	}
	if len(models) == 0 {
		return
	}
	n, err := dst.createIndexes(ctx, models)
	res.IndexesCopied = n
	if err != nil && res.Err == "" {
		res.Err = fmt.Errorf("create indexes %s: %w", ns, err).Error()
	}
}

// Compile-time assertion that *Copier satisfies the engine contract.
var _ engine.Copier = (*Copier)(nil)
