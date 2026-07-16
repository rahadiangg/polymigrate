package verify

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"

	"github.com/weefer/mongomigrate/internal/engine"
	"github.com/weefer/mongomigrate/internal/mongox"
	"github.com/weefer/mongomigrate/internal/namespace"
)

// store is the narrow driver surface the verifier needs. It is defined here so
// the orchestration can be unit-tested with an in-memory fake; the real
// implementation (clientStore) wraps *mongox.Client.
type store interface {
	// countDocuments returns the exact document count of a namespace.
	countDocuments(ctx context.Context, ns namespace.Namespace) (int64, error)
	// listIndexes returns the raw index specifications of a namespace.
	listIndexes(ctx context.Context, ns namespace.Namespace) ([]bson.Raw, error)
	// sampleIDs returns up to n _id values from a namespace, ordered by _id.
	sampleIDs(ctx context.Context, ns namespace.Namespace, n int) ([]bson.RawValue, error)
	// fetchByIDs returns the documents whose _id is in ids, keyed by idKey.
	fetchByIDs(ctx context.Context, ns namespace.Namespace, ids []bson.RawValue) (map[string]bson.Raw, error)
}

// Verifier proves that a destination deployment matches its source by comparing
// counts, indexes, and sampled document content per namespace. It satisfies
// engine.Verifier.
type Verifier struct {
	src store
	dst store
	log *slog.Logger
}

var _ engine.Verifier = (*Verifier)(nil)

// New constructs a Verifier that reads the source and destination through the
// given mongox clients. A nil logger is replaced with a discarding one.
func New(src, dst *mongox.Client, log *slog.Logger) *Verifier {
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Verifier{
		src: &clientStore{cl: src},
		dst: &clientStore{cl: dst},
		log: log,
	}
}

// Verify runs the enabled checks across every namespace with opts.Workers
// collections verified concurrently. Every namespace is verified even if some
// fail; a per-collection failure becomes a failed verdict with an explanatory
// Note rather than aborting the run. An error is returned only for a fatal
// setup problem.
func (v *Verifier) Verify(ctx context.Context, opts engine.VerifyOptions) (engine.VerifyReport, error) {
	workers := opts.Workers
	if workers < 1 {
		workers = 1
	}
	if n := len(opts.Namespaces); workers > n && n > 0 {
		workers = n
	}

	verdicts := make([]engine.CollectionVerdict, len(opts.Namespaces))
	jobs := make(chan int)

	var wg sync.WaitGroup
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range jobs {
				verdicts[i] = v.verifyOne(ctx, opts, opts.Namespaces[i])
			}
		}()
	}

feed:
	for i := range opts.Namespaces {
		select {
		case <-ctx.Done():
			break feed
		case jobs <- i:
		}
	}
	close(jobs)
	wg.Wait()

	report := engine.VerifyReport{Collections: verdicts, Ok: true}
	for _, cv := range verdicts {
		if !cv.Ok {
			report.Ok = false
		}
	}
	return report, nil
}

// verifyOne runs the enabled checks for a single namespace and returns its
// verdict. It never returns an error: every failure is recorded on the verdict
// (Ok=false plus a Note) so the overall run continues.
func (v *Verifier) verifyOne(ctx context.Context, opts engine.VerifyOptions, ns namespace.Namespace) engine.CollectionVerdict {
	cv := engine.CollectionVerdict{Namespace: ns, Ok: true}

	if err := ctx.Err(); err != nil {
		cv.Ok = false
		cv.Notes = append(cv.Notes, fmt.Sprintf("skipped: %v", err))
		return cv
	}

	if opts.CheckCounts {
		v.checkCounts(ctx, ns, &cv)
	}
	if opts.CheckIndexes {
		v.checkIndexes(ctx, ns, &cv)
	}
	if opts.SampleSize > 0 {
		v.checkSample(ctx, ns, opts.SampleSize, &cv)
	}

	v.log.Debug("collection verified",
		"ns", ns.String(), "ok", cv.Ok,
		"srcCount", cv.SrcCount, "dstCount", cv.DstCount)
	return cv
}

func (v *Verifier) checkCounts(ctx context.Context, ns namespace.Namespace, cv *engine.CollectionVerdict) {
	srcN, err := v.src.countDocuments(ctx, ns)
	if err != nil {
		cv.Ok = false
		cv.Notes = append(cv.Notes, fmt.Sprintf("count source: %v", err))
		return
	}
	dstN, err := v.dst.countDocuments(ctx, ns)
	if err != nil {
		cv.Ok = false
		cv.Notes = append(cv.Notes, fmt.Sprintf("count destination: %v", err))
		return
	}
	cv.SrcCount = srcN
	cv.DstCount = dstN
	cv.CountsMatch = srcN == dstN
	if !cv.CountsMatch {
		cv.Ok = false
		cv.Notes = append(cv.Notes, fmt.Sprintf("count mismatch: src=%d dst=%d", srcN, dstN))
	}
}

func (v *Verifier) checkIndexes(ctx context.Context, ns namespace.Namespace, cv *engine.CollectionVerdict) {
	srcSpecs, err := v.src.listIndexes(ctx, ns)
	if err != nil {
		cv.Ok = false
		cv.Notes = append(cv.Notes, fmt.Sprintf("list source indexes: %v", err))
		return
	}
	dstSpecs, err := v.dst.listIndexes(ctx, ns)
	if err != nil {
		cv.Ok = false
		cv.Notes = append(cv.Notes, fmt.Sprintf("list destination indexes: %v", err))
		return
	}
	srcIdx, err := canonicalizeIndexes(srcSpecs)
	if err != nil {
		cv.Ok = false
		cv.Notes = append(cv.Notes, fmt.Sprintf("canonicalize source indexes: %v", err))
		return
	}
	dstIdx, err := canonicalizeIndexes(dstSpecs)
	if err != nil {
		cv.Ok = false
		cv.Notes = append(cv.Notes, fmt.Sprintf("canonicalize destination indexes: %v", err))
		return
	}

	missing, extra, match := diffIndexes(srcIdx, dstIdx)
	cv.MissingIndexes = missing
	cv.ExtraIndexes = extra
	cv.IndexesMatch = match
	if !match {
		cv.Ok = false
		if len(missing) > 0 {
			cv.Notes = append(cv.Notes, fmt.Sprintf("destination missing %d index(es): %s",
				len(missing), strings.Join(missing, ", ")))
		}
		if len(extra) > 0 {
			cv.Notes = append(cv.Notes, fmt.Sprintf("destination has %d extra index(es): %s",
				len(extra), strings.Join(extra, ", ")))
		}
	}
}

func (v *Verifier) checkSample(ctx context.Context, ns namespace.Namespace, sampleSize int, cv *engine.CollectionVerdict) {
	ids, err := v.src.sampleIDs(ctx, ns, sampleSize)
	if err != nil {
		cv.Ok = false
		cv.Notes = append(cv.Notes, fmt.Sprintf("sample source ids: %v", err))
		return
	}
	srcDocs, err := v.src.fetchByIDs(ctx, ns, ids)
	if err != nil {
		cv.Ok = false
		cv.Notes = append(cv.Notes, fmt.Sprintf("fetch source sample: %v", err))
		return
	}
	dstDocs, err := v.dst.fetchByIDs(ctx, ns, ids)
	if err != nil {
		cv.Ok = false
		cv.Notes = append(cv.Notes, fmt.Sprintf("fetch destination sample: %v", err))
		return
	}

	checked, mismatches := compareSamples(srcDocs, dstDocs)
	cv.SampleChecked = checked
	cv.SampleMismatches = mismatches
	cv.SampleMatch = mismatches == 0
	if !cv.SampleMatch {
		cv.Ok = false
		cv.Notes = append(cv.Notes, fmt.Sprintf("%d of %d sampled documents differ", mismatches, checked))
	}
}

// -----------------------------------------------------------------------------
// clientStore — the real driver adapter around *mongox.Client.
// -----------------------------------------------------------------------------

type clientStore struct {
	cl *mongox.Client
}

func (s *clientStore) countDocuments(ctx context.Context, ns namespace.Namespace) (int64, error) {
	coll := s.cl.Database(ns.DB).Collection(ns.Collection)
	n, err := coll.CountDocuments(ctx, bson.D{})
	if err != nil {
		return 0, fmt.Errorf("count %s: %w", ns, err)
	}
	return n, nil
}

func (s *clientStore) listIndexes(ctx context.Context, ns namespace.Namespace) ([]bson.Raw, error) {
	coll := s.cl.Database(ns.DB).Collection(ns.Collection)
	cur, err := coll.Indexes().List(ctx)
	if err != nil {
		return nil, fmt.Errorf("list indexes %s: %w", ns, err)
	}
	defer cur.Close(ctx)
	var specs []bson.Raw
	if err := cur.All(ctx, &specs); err != nil {
		return nil, fmt.Errorf("read indexes %s: %w", ns, err)
	}
	return specs, nil
}

func (s *clientStore) sampleIDs(ctx context.Context, ns namespace.Namespace, n int) ([]bson.RawValue, error) {
	if n <= 0 {
		return nil, nil
	}
	coll := s.cl.Database(ns.DB).Collection(ns.Collection)

	// Prefer a RANDOM sample via $sample so content drift anywhere in the
	// collection can be caught — a head-of-collection scan would only ever check
	// the lowest _ids and silently miss drift elsewhere. Fall back to a
	// deterministic sorted scan if $sample is unavailable (e.g. on a restricted
	// managed API).
	docs, err := readIDs(ctx, coll, mongo.Pipeline{
		{{Key: "$sample", Value: bson.D{{Key: "size", Value: int64(n)}}}},
		{{Key: "$project", Value: bson.D{{Key: "_id", Value: 1}}}},
	})
	if err != nil {
		cur, ferr := coll.Find(ctx, bson.D{}, options.Find().
			SetSort(bson.D{{Key: "_id", Value: 1}}).
			SetLimit(int64(n)).
			SetProjection(bson.D{{Key: "_id", Value: 1}}))
		if ferr != nil {
			return nil, fmt.Errorf("sample %s: %w", ns, ferr)
		}
		defer cur.Close(ctx)
		if ferr := cur.All(ctx, &docs); ferr != nil {
			return nil, fmt.Errorf("read sample %s: %w", ns, ferr)
		}
	}

	ids := make([]bson.RawValue, 0, len(docs))
	for _, d := range docs {
		rv, err := d.LookupErr("_id")
		if err != nil {
			return nil, fmt.Errorf("sample %s: document without _id: %w", ns, err)
		}
		ids = append(ids, copyRawValue(rv))
	}
	return ids, nil
}

// readIDs runs an aggregation pipeline and returns the raw result documents.
func readIDs(ctx context.Context, coll *mongo.Collection, pipe mongo.Pipeline) ([]bson.Raw, error) {
	cur, err := coll.Aggregate(ctx, pipe)
	if err != nil {
		return nil, err
	}
	defer cur.Close(ctx)
	var docs []bson.Raw
	if err := cur.All(ctx, &docs); err != nil {
		return nil, err
	}
	return docs, nil
}

func (s *clientStore) fetchByIDs(ctx context.Context, ns namespace.Namespace, ids []bson.RawValue) (map[string]bson.Raw, error) {
	out := make(map[string]bson.Raw, len(ids))
	if len(ids) == 0 {
		return out, nil
	}
	coll := s.cl.Database(ns.DB).Collection(ns.Collection)
	in := make(bson.A, 0, len(ids))
	for _, id := range ids {
		in = append(in, id)
	}
	filter := bson.D{{Key: "_id", Value: bson.D{{Key: "$in", Value: in}}}}
	cur, err := coll.Find(ctx, filter)
	if err != nil {
		return nil, fmt.Errorf("fetch by ids %s: %w", ns, err)
	}
	defer cur.Close(ctx)

	var docs []bson.Raw
	if err := cur.All(ctx, &docs); err != nil {
		return nil, fmt.Errorf("read fetch %s: %w", ns, err)
	}
	for _, d := range docs {
		rv, err := d.LookupErr("_id")
		if err != nil {
			return nil, fmt.Errorf("fetch %s: document without _id: %w", ns, err)
		}
		out[idKey(rv)] = d
	}
	return out, nil
}
