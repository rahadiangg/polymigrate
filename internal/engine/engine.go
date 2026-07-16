// Package engine defines the contracts that tie the migration components
// together: the Copier (bulk snapshot), the Tailer (change-stream CDC), and the
// Verifier (correctness proof), plus the option and result value types they
// exchange. Keeping these in one dependency-free package lets the copier, cdc,
// verify, and migrate packages be built and tested independently against a
// frozen API, and lets the orchestrator depend only on interfaces (so it can be
// unit-tested with fakes).
package engine

import (
	"context"
	"time"

	"github.com/weefer/mongomigrate/internal/namespace"
)

// -----------------------------------------------------------------------------
// Reporter — progress + phase notifications. Implementations must be safe for
// concurrent use; the copier and tailer call them from worker goroutines.
// -----------------------------------------------------------------------------

// Reporter receives structured progress events from the engine components.
type Reporter interface {
	// Phase announces a transition in the migration lifecycle.
	Phase(p Phase)
	// Event reports a discrete message with optional key/value context.
	Event(msg string, kv ...any)
	// Progress reports incremental progress for a namespace (done of total docs;
	// total may be -1 when unknown).
	Progress(ns namespace.Namespace, done, total int64)
}

// NopReporter is a Reporter that discards everything. Useful in tests.
type NopReporter struct{}

func (NopReporter) Phase(Phase)                                {}
func (NopReporter) Event(string, ...any)                       {}
func (NopReporter) Progress(namespace.Namespace, int64, int64) {}

// -----------------------------------------------------------------------------
// Phases
// -----------------------------------------------------------------------------

// Phase identifies a stage of the migration lifecycle.
type Phase string

const (
	PhaseInit     Phase = "init"
	PhaseTailOpen Phase = "tail-open" // change stream opened, buffering begins (T0)
	PhaseSnapshot Phase = "snapshot"  // bulk copy (T0->T1)
	PhaseApply    Phase = "apply"     // draining buffered change events (T1->T2)
	PhaseCutover  Phase = "cutover"   // writes frozen, waiting for lag ~ 0
	PhaseVerify   Phase = "verify"
	PhaseDone     Phase = "done"
	PhaseFailed   Phase = "failed"
)

// -----------------------------------------------------------------------------
// Copier contract
// -----------------------------------------------------------------------------

// CopyOptions configures a snapshot copy. A Copier is constructed with its
// source and destination handles; these options describe one Copy call.
type CopyOptions struct {
	// Namespaces is the resolved, already-filtered set of collections to copy.
	Namespaces []namespace.Namespace
	// Workers is the number of collections copied concurrently (>=1).
	Workers int
	// BatchSize is the number of documents per destination bulk write (>=1).
	BatchSize int
	// ReadBatchSize is the source cursor batch size (0 = driver default).
	ReadBatchSize int
	// DropTarget drops each destination collection before copying into it.
	DropTarget bool
	// CopyIndexes recreates source indexes on the destination after data load.
	CopyIndexes bool
	// SkipExistingNonEmpty skips a destination collection that already holds docs.
	SkipExistingNonEmpty bool

	// --- Resilience / resume (all optional; zero values = fresh, non-resumable copy) ---

	// SkipNamespaces lists namespaces (by ns.String()) already fully copied on a
	// prior run; the copier skips them entirely when resuming.
	SkipNamespaces map[string]bool
	// ResumeFrom maps a namespace (ns.String()) to the raw BSON of the last _id
	// already copied. The copier resumes that namespace at {_id: {$gt: lastID}}
	// (documents are copied sorted by _id) and uses idempotent upserts so the
	// boundary batch can be safely re-copied.
	ResumeFrom map[string][]byte
	// OnProgress, if non-nil, is invoked during and at the end of each collection
	// copy with the last _id copied so far (raw BSON) and the running doc count, so
	// the orchestrator can persist a checkpoint. It is called from worker
	// goroutines and MUST be safe for concurrent use.
	OnProgress func(ns namespace.Namespace, lastID []byte, docsCopied int64, done bool)
}

// CollectionCopyResult is the per-collection outcome of a snapshot copy.
type CollectionCopyResult struct {
	Namespace     namespace.Namespace
	DocsCopied    int64
	DocsFailed    int64
	IndexesCopied int
	Skipped       bool
	Duration      time.Duration
	// Err holds a human-readable error when the collection failed; empty on success.
	Err string
}

// CopyResult aggregates a snapshot copy.
type CopyResult struct {
	Collections []CollectionCopyResult
	TotalDocs   int64
	TotalFailed int64
	Duration    time.Duration
}

// Failed reports whether any collection failed.
func (r CopyResult) Failed() bool {
	for _, c := range r.Collections {
		if c.Err != "" {
			return true
		}
	}
	return false
}

// Copier performs the bulk snapshot copy of a fixed set of namespaces.
type Copier interface {
	Copy(ctx context.Context, opts CopyOptions) (CopyResult, error)
}

// -----------------------------------------------------------------------------
// Tailer contract — change-stream CDC
// -----------------------------------------------------------------------------

// ResumeToken is an opaque change-stream resume token (raw BSON document bytes).
// The empty token means "start from now".
type ResumeToken []byte

// IsZero reports whether the token is unset.
func (t ResumeToken) IsZero() bool { return len(t) == 0 }

// TailOptions configures the change-stream tailer.
type TailOptions struct {
	// Namespaces restricts the stream to these collections. Empty means watch the
	// whole deployment (all non-system databases).
	Namespaces []namespace.Namespace
	// Resume, when set, resumes the stream after this token instead of starting now.
	Resume ResumeToken
	// BufferSize bounds the in-memory event buffer (events); 0 selects a default.
	BufferSize int
	// ApplyBatchSize is the max number of change events applied per bulk write.
	ApplyBatchSize int

	// OnToken, if non-nil, is invoked (throttled by the tailer) with the resume
	// token of the LAST APPLIED event — never a token that has only been captured
	// but not yet applied. This ordering is critical: checkpointing a token ahead
	// of applied data would cause a resumed stream to skip unapplied events and
	// lose data. Called from the apply goroutine; must be safe for concurrent use.
	OnToken func(token ResumeToken)
}

// TailStats is a snapshot of tailer progress. It must be safe to read while the
// tailer runs.
type TailStats struct {
	Captured      int64         // events read from the change stream
	Applied       int64         // events written to the destination
	Inserts       int64         // applied inserts/replaces
	Deletes       int64         // applied deletes
	Buffered      int64         // events captured but not yet applied
	LastEventAt   time.Time     // cluster time of the last captured event (wall clock approximation)
	LastAppliedAt time.Time     // when the last event was applied
	Lag           time.Duration // estimated apply lag (Captured - Applied translated to time)
}

// Tailer captures change-stream events from the source and applies them
// idempotently to the destination. The lifecycle is:
//
//	Open   -> begins capturing (buffering) events from T0.
//	Apply  -> drains buffered + incoming events to the destination; blocks until
//	          the context is cancelled or Stop is called.
//	Stop   -> requests Apply to finish the current drain and return.
//	Close  -> releases the change stream and resources.
//
// Implementations must apply events idempotently (replace-with-upsert / delete)
// using fullDocument=updateLookup so re-runs and partial failures are safe.
type Tailer interface {
	Open(ctx context.Context, opts TailOptions) error
	Apply(ctx context.Context) error
	Stats() TailStats
	Token() ResumeToken
	Stop()
	Close(ctx context.Context) error
}

// -----------------------------------------------------------------------------
// Verifier contract
// -----------------------------------------------------------------------------

// VerifyOptions configures a verification pass.
type VerifyOptions struct {
	Namespaces   []namespace.Namespace
	CheckCounts  bool
	CheckIndexes bool
	// SampleSize is the number of documents sampled and hashed per collection for
	// content comparison; 0 disables sampling.
	SampleSize int
	// Workers is the number of collections verified concurrently (>=1).
	Workers int
}

// CollectionVerdict is the per-collection verification result.
type CollectionVerdict struct {
	Namespace        namespace.Namespace
	SrcCount         int64
	DstCount         int64
	CountsMatch      bool
	IndexesMatch     bool
	MissingIndexes   []string // present on source, absent on destination
	ExtraIndexes     []string // present on destination, absent on source
	SampleChecked    int
	SampleMismatches int
	SampleMatch      bool
	Notes            []string
	// Ok is the overall per-collection verdict across the enabled checks.
	Ok bool
}

// VerifyReport aggregates a verification pass.
type VerifyReport struct {
	Collections []CollectionVerdict
	Ok          bool
}

// Verifier proves a destination matches its source.
type Verifier interface {
	Verify(ctx context.Context, opts VerifyOptions) (VerifyReport, error)
}
