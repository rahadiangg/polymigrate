// Package migrate is the migration orchestrator. It ties the three engine
// components — the Copier (bulk snapshot), the Tailer (change-stream CDC), and
// the Verifier (correctness proof) — together through the engine interfaces
// only, so it carries no dependency on the MongoDB driver and is fully
// unit-testable with in-memory fakes.
//
// Two flows are supported:
//
//	Snapshot (Options.Live == false): a one-shot bulk copy, optionally verified.
//	Live     (Options.Live == true):  the change stream is opened first (so
//	  buffering begins before the snapshot), then the snapshot runs, then the
//	  buffered + incoming events are applied while the orchestrator watches the
//	  apply lag. Once the lag falls to the configured threshold it enters the
//	  cutover phase (waiting for either an automatic or an operator signal),
//	  drains the remaining lag to zero, stops the tailer, and optionally
//	  verifies.
package migrate

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/rahadiangg/polymigrate/internal/checkpoint"
	"github.com/rahadiangg/polymigrate/internal/engine"
	"github.com/rahadiangg/polymigrate/internal/namespace"
)

// defaultPollInterval is the lag-poll cadence used when Options.PollInterval is
// zero.
const defaultPollInterval = time.Second

// ErrCopyFailed is the sentinel wrapped when a snapshot completes but one or
// more collections failed to copy. Callers can test for it with errors.Is.
var ErrCopyFailed = errors.New("snapshot copy had collection failures")

// ErrVerifyFailed is the sentinel wrapped when the verifier's own verdict
// (VerifyReport.Ok) is false — i.e. the data does not match — even though the
// verifier returned no transport error. A migration whose verification fails is
// a failed migration. Callers can test for it with errors.Is.
var ErrVerifyFailed = errors.New("verification reported a mismatch")

// errApplyEndedEarly is returned when the tailer's Apply loop returns (without
// an error of its own) before the orchestrator reached the end of cutover. It
// signals that CDC stopped prematurely, which is a failure of the live flow.
var errApplyEndedEarly = errors.New("tailer apply ended before cutover completed")

// Options configures a single Run.
type Options struct {
	// Live selects the flow: false => snapshot only; true => snapshot + CDC + cutover.
	Live bool
	// Copy configures the snapshot copy.
	Copy engine.CopyOptions
	// Tail configures the change-stream tailer (live only).
	Tail engine.TailOptions
	// Verify configures the verification pass (when RunVerify is set).
	Verify engine.VerifyOptions
	// RunVerify runs the verifier after the data is in place.
	RunVerify bool
	// LagThreshold marks the deployment cutover-ready once Stats().Lag <= it.
	LagThreshold time.Duration
	// AutoCutover, when true, proceeds automatically the moment the deployment is
	// cutover-ready; when false, Run blocks until a value is received on Proceed.
	AutoCutover bool
	// Proceed is the manual cutover signal, used when Live && !AutoCutover.
	Proceed <-chan struct{}
	// PollInterval is the lag-poll cadence; defaults to 1s when zero.
	PollInterval time.Duration

	// --- Resilience / resume (optional) ---

	// Checkpoint, when non-nil, persists progress so the run can resume after a
	// crash. Copy progress and the applied CDC token are checkpointed as they
	// advance, and a final checkpoint is flushed even on a cancelled run.
	Checkpoint checkpoint.Store
	// RunID identifies this migration for the checkpoint store.
	RunID string
	// ConfigHash guards a resume against a changed configuration; a resumed run
	// whose stored ConfigHash differs is rejected.
	ConfigHash string
	// Resume, when true, loads an existing checkpoint for RunID and continues:
	// completed collections are skipped, an in-progress one resumes from its last
	// _id, and (live) the change stream resumes from the saved token.
	Resume bool
}

// Report is the aggregate outcome of a Run. It carries the last snapshot
// result, the tailer stats sampled at the end of the run, the verification
// report, and the ordered list of phases that were entered.
type Report struct {
	Copy   engine.CopyResult
	Tail   engine.TailStats
	Verify engine.VerifyReport
	Phases []engine.Phase
}

// Engine orchestrates a migration across the copier, tailer, and verifier.
// An Engine runs one migration at a time (Run is not safe for concurrent use on
// the same Engine); cp holds the current run's checkpointer, if any.
type Engine struct {
	copier   engine.Copier
	tailer   engine.Tailer
	verifier engine.Verifier
	reporter engine.Reporter
	log      *slog.Logger

	cp *checkpointer // set for the duration of a checkpointed Run
}

// New builds an Engine from its components. A nil reporter is replaced with a
// discarding one and a nil logger with a logger that discards output, so the
// returned Engine is always safe to call.
func New(c engine.Copier, t engine.Tailer, v engine.Verifier, r engine.Reporter, log *slog.Logger) *Engine {
	if r == nil {
		r = engine.NopReporter{}
	}
	if log == nil {
		log = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Engine{copier: c, tailer: t, verifier: v, reporter: r, log: log}
}

// Run executes the migration described by opts and returns a Report. The Report
// is populated even on the error paths so callers can inspect the partial
// outcome. Context cancellation stops the tailer, releases its resources, and
// returns the context error with the partial report.
func (e *Engine) Run(ctx context.Context, opts Options) (Report, error) {
	if opts.PollInterval <= 0 {
		opts.PollInterval = defaultPollInterval
	}

	var rep Report
	e.recordPhase(&rep, engine.PhaseInit)

	if e.copier == nil {
		e.recordPhase(&rep, engine.PhaseFailed)
		return rep, errors.New("migrate: nil copier")
	}
	if opts.RunVerify && e.verifier == nil {
		e.recordPhase(&rep, engine.PhaseFailed)
		return rep, errors.New("migrate: RunVerify set but verifier is nil")
	}

	// Set up checkpointing / resume before any data moves.
	if opts.Checkpoint != nil {
		if err := e.setupCheckpoint(ctx, &opts); err != nil {
			e.recordPhase(&rep, engine.PhaseFailed)
			return rep, err
		}
		// Guarantee a durable final checkpoint even if the run context is cancelled.
		defer func() {
			e.cp.Flush(context.WithoutCancel(ctx))
			e.cp = nil
		}()
	}

	if opts.Live {
		if e.tailer == nil {
			e.recordPhase(&rep, engine.PhaseFailed)
			return rep, errors.New("migrate: Live set but tailer is nil")
		}
		return e.runLive(ctx, opts, rep)
	}
	return e.runSnapshot(ctx, opts, rep)
}

// recordPhase appends p to the report's phase list, notifies the reporter, and
// (when checkpointing) records the phase durably.
func (e *Engine) recordPhase(rep *Report, p engine.Phase) {
	rep.Phases = append(rep.Phases, p)
	e.reporter.Phase(p)
	if e.cp != nil {
		e.cp.recordPhase(p)
	}
}

// setupCheckpoint loads any existing checkpoint (when resuming), rewrites opts to
// skip completed collections / resume in-progress ones / resume the change
// stream, installs the checkpointer, and wires the copier/tailer progress
// callbacks to it.
func (e *Engine) setupCheckpoint(ctx context.Context, opts *Options) error {
	mode := "snapshot"
	if opts.Live {
		mode = "live"
	}
	state := checkpoint.State{
		RunID:      opts.RunID,
		Mode:       mode,
		ConfigHash: opts.ConfigHash,
		Namespaces: map[string]checkpoint.NSProgress{},
	}

	if opts.Resume {
		prev, found, err := opts.Checkpoint.Load(ctx, opts.RunID)
		if err != nil {
			return fmt.Errorf("load checkpoint %q: %w", opts.RunID, err)
		}
		if found {
			if prev.ConfigHash != "" && opts.ConfigHash != "" && prev.ConfigHash != opts.ConfigHash {
				return fmt.Errorf("cannot resume run %q: configuration changed since the checkpoint was written", opts.RunID)
			}
			if prev.NonResumable {
				return fmt.Errorf("cannot resume run %q: %w (start a fresh migration)", opts.RunID, engine.ErrChangeStreamHistoryLost)
			}
			if prev.Phase == string(engine.PhaseDone) {
				return fmt.Errorf("run %q already completed successfully; clear its checkpoint (poly checkpoint clear) or use a new --run-id to migrate again", opts.RunID)
			}
			applyResume(opts, prev)
			state = prev
			if state.Namespaces == nil {
				state.Namespaces = map[string]checkpoint.NSProgress{}
			}
			e.reporter.Event("resuming from checkpoint", "runId", opts.RunID,
				"completed", countDone(prev), "token", len(prev.ResumeToken) > 0)
		}
	}

	e.cp = newCheckpointer(ctx, opts.Checkpoint, state, e.log)
	// Wire the copier/tailer callbacks to the checkpointer, preserving any caller
	// callback by chaining.
	callerProgress := opts.Copy.OnProgress
	opts.Copy.OnProgress = func(ns namespace.Namespace, lastID []byte, docsCopied int64, done bool) {
		e.cp.recordProgress(ns, lastID, docsCopied, done)
		if callerProgress != nil {
			callerProgress(ns, lastID, docsCopied, done)
		}
	}
	callerToken := opts.Tail.OnToken
	opts.Tail.OnToken = func(tok engine.ResumeToken) {
		e.cp.recordToken(tok)
		if callerToken != nil {
			callerToken(tok)
		}
	}
	return nil
}

// applyResume rewrites opts from a loaded checkpoint: completed namespaces are
// skipped, in-progress ones resume from their last _id, and the change stream
// resumes from the saved token.
func applyResume(opts *Options, prev checkpoint.State) {
	// Never drop on resume: the destination already holds the data copied so far,
	// and dropping would discard a completed (skipped) or in-progress collection.
	opts.Copy.DropTarget = false

	// A LIVE resume is only safe to fast-path (skip completed collections, resume
	// in-progress ones) when there is a saved CDC token: the change stream then
	// resumes from that token and covers every change since — including changes to
	// the completed collections that happened while poly was down. Without a token
	// (the snapshot phase never finished and CDC never checkpointed), a resumed
	// stream would start from "now" and miss the downtime changes to the skipped
	// collections. So in that case do NOT skip/resume snapshot state — re-copy
	// everything fresh under a newly-opened stream, which captures current state.
	if opts.Live && len(prev.ResumeToken) == 0 {
		return
	}

	if opts.Copy.SkipNamespaces == nil {
		opts.Copy.SkipNamespaces = map[string]bool{}
	}
	if opts.Copy.ResumeFrom == nil {
		opts.Copy.ResumeFrom = map[string][]byte{}
	}
	for ns, p := range prev.Namespaces {
		if p.Done {
			opts.Copy.SkipNamespaces[ns] = true
		} else if len(p.LastID) > 0 {
			opts.Copy.ResumeFrom[ns] = p.LastID
		}
	}
	if opts.Live && len(prev.ResumeToken) > 0 {
		opts.Tail.Resume = engine.ResumeToken(prev.ResumeToken)
	}
}

func countDone(s checkpoint.State) int {
	n := 0
	for _, p := range s.Namespaces {
		if p.Done {
			n++
		}
	}
	return n
}

// runSnapshot performs the snapshot-only flow:
//
//	Init -> Snapshot -> [Verify] -> Done
//
// A failing copy records Failed and returns an error wrapping the failure.
func (e *Engine) runSnapshot(ctx context.Context, opts Options, rep Report) (Report, error) {
	e.recordPhase(&rep, engine.PhaseSnapshot)
	cr, err := e.copier.Copy(ctx, opts.Copy)
	rep.Copy = cr
	if err != nil || cr.Failed() {
		e.recordPhase(&rep, engine.PhaseFailed)
		return rep, copyError(cr, err)
	}

	if opts.RunVerify {
		if verr := e.verify(ctx, opts, &rep); verr != nil {
			return rep, verr
		}
	}

	e.recordPhase(&rep, engine.PhaseDone)
	return rep, nil
}

// verify runs the verifier, records its report, and returns an error when the
// verifier failed to run (transport error) OR when it ran successfully but its
// verdict is a mismatch (VerifyReport.Ok == false). A migration that does not
// verify is not a successful migration, so both cases record the Failed phase.
func (e *Engine) verify(ctx context.Context, opts Options, rep *Report) error {
	e.recordPhase(rep, engine.PhaseVerify)
	vr, verr := e.verifier.Verify(ctx, opts.Verify)
	rep.Verify = vr
	if verr != nil {
		e.recordPhase(rep, engine.PhaseFailed)
		return fmt.Errorf("verify: %w", verr)
	}
	if !vr.Ok {
		e.recordPhase(rep, engine.PhaseFailed)
		return fmt.Errorf("verify: %w", ErrVerifyFailed)
	}
	return nil
}

// applyHandle tracks the background Apply goroutine. All of its fields are
// touched only by the orchestrator goroutine; the sole cross-goroutine
// synchronisation point is the done channel (written once by the Apply
// goroutine, read once by join).
type applyHandle struct {
	done     chan error
	cancel   context.CancelFunc
	returned bool
	err      error
}

// join waits for the Apply goroutine to return (at most once) and reports its
// error. It is safe to call repeatedly; only the first call blocks.
func (h *applyHandle) join() error {
	if !h.returned {
		h.err = <-h.done
		h.returned = true
	}
	return h.err
}

// runLive performs the live flow:
//
//	Init -> TailOpen -> Snapshot -> Apply -> Cutover -> [Verify] -> Done
//
// The tailer is opened before the snapshot so events are buffered from T0. The
// Apply loop runs in a tracked goroutine that is always joined before Run
// returns (no goroutine leak). The tailer is always Closed once opened.
func (e *Engine) runLive(ctx context.Context, opts Options, rep Report) (out Report, err error) {
	// Open the change stream first so buffering starts before the snapshot (T0).
	e.recordPhase(&rep, engine.PhaseTailOpen)
	if oerr := e.tailer.Open(ctx, opts.Tail); oerr != nil {
		e.recordPhase(&rep, engine.PhaseFailed)
		return rep, fmt.Errorf("tail open: %w", oerr)
	}
	// From here the tailer is open: snapshot the final stats and release it on
	// every exit path. The named return out is assigned by each return statement
	// before this runs, so out.Tail always reflects the end-of-run stats.
	defer func() {
		out.Tail = e.tailer.Stats()
		if cerr := e.tailer.Close(ctx); cerr != nil {
			e.log.Debug("tailer close", "err", cerr)
		}
	}()

	// Snapshot (T0 -> T1) while the tailer buffers changes.
	e.recordPhase(&rep, engine.PhaseSnapshot)
	cr, cerr := e.copier.Copy(ctx, opts.Copy)
	rep.Copy = cr
	if cerr != nil || cr.Failed() {
		e.recordPhase(&rep, engine.PhaseFailed)
		e.tailer.Stop()
		return rep, copyError(cr, cerr)
	}
	if ce := ctx.Err(); ce != nil {
		e.tailer.Stop()
		return rep, ce
	}

	// Launch Apply (T1 -> T2) in a tracked goroutine.
	applyCtx, applyCancel := context.WithCancel(ctx)
	defer applyCancel()
	ah := &applyHandle{done: make(chan error, 1), cancel: applyCancel}
	e.recordPhase(&rep, engine.PhaseApply)
	go func() {
		ah.done <- e.tailer.Apply(applyCtx)
	}()

	report := func(s engine.TailStats) {
		e.reporter.Event("tail-lag", "lag", s.Lag, "captured", s.Captured,
			"applied", s.Applied, "buffered", s.Buffered)
	}

	// Monitor the apply lag until the deployment is cutover-ready.
	ready, werr := e.pollUntil(ctx, ah, opts.PollInterval, report, func(s engine.TailStats) bool {
		return s.Lag <= opts.LagThreshold
	})
	if werr != nil {
		return rep, e.abort(ah, &rep, werr)
	}
	_ = ready // pollUntil only returns false with a non-nil error, handled above.

	// Cutover: writes are (to be) frozen; wait for the go-ahead.
	e.recordPhase(&rep, engine.PhaseCutover)
	e.reporter.Event("cutover-ready")
	if !opts.AutoCutover {
		if perr := e.waitProceed(ctx, ah, opts.Proceed); perr != nil {
			return rep, e.abort(ah, &rep, perr)
		}
	}

	// Final drain: apply the remaining buffered events until the backlog is truly
	// empty. Gate on Buffered==0 AND Lag<=0 (not lag alone) so a transient lag
	// reading can never let the cutover stop the reader with events still pending.
	if _, derr := e.pollUntil(ctx, ah, opts.PollInterval, report, func(s engine.TailStats) bool {
		return s.Buffered == 0 && s.Lag <= 0
	}); derr != nil {
		return rep, e.abort(ah, &rep, derr)
	}

	// Stop the tailer and join the Apply goroutine.
	e.tailer.Stop()
	if aerr := ah.join(); aerr != nil {
		e.markIfHistoryLost(aerr)
		e.recordPhase(&rep, engine.PhaseFailed)
		return rep, fmt.Errorf("apply: %w", aerr)
	}

	if opts.RunVerify {
		if verr := e.verify(ctx, opts, &rep); verr != nil {
			return rep, verr
		}
	}

	e.recordPhase(&rep, engine.PhaseDone)
	return rep, nil
}

// pollUntil samples the tailer stats immediately and then every interval,
// reporting each sample, until pred is satisfied. It returns (true, nil) when
// pred holds. It returns (false, err) when the context is cancelled (err is the
// context error) or when Apply returns before pred is satisfied (err wraps the
// apply error, or errApplyEndedEarly when Apply returned without one). The
// caller must only invoke pollUntil while the Apply goroutine is still running.
func (e *Engine) pollUntil(ctx context.Context, ah *applyHandle, interval time.Duration, report func(engine.TailStats), pred func(engine.TailStats) bool) (bool, error) {
	// Sample immediately so an already-ready deployment cuts over without
	// waiting a full interval.
	if s := e.tailer.Stats(); pred(s) {
		if report != nil {
			report(s)
		}
		return true, nil
	} else if report != nil {
		report(s)
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case aerr := <-ah.done:
			ah.returned = true
			ah.err = aerr
			// Prefer the context error if cancellation raced with Apply returning.
			if ce := ctx.Err(); ce != nil {
				return false, ce
			}
			if aerr != nil {
				return false, fmt.Errorf("apply: %w", aerr)
			}
			return false, errApplyEndedEarly
		case <-ticker.C:
			s := e.tailer.Stats()
			if report != nil {
				report(s)
			}
			if pred(s) {
				return true, nil
			}
		}
	}
}

// waitProceed blocks until the operator signals cutover on proceed, the context
// is cancelled, or Apply returns early. A nil proceed channel blocks until one
// of the other two events occurs.
func (e *Engine) waitProceed(ctx context.Context, ah *applyHandle, proceed <-chan struct{}) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case aerr := <-ah.done:
		ah.returned = true
		ah.err = aerr
		if ce := ctx.Err(); ce != nil {
			return ce
		}
		if aerr != nil {
			return fmt.Errorf("apply: %w", aerr)
		}
		return errApplyEndedEarly
	case <-proceed:
		return nil
	}
}

// abort tears down the tailer after a live-flow error: it stops the tailer,
// cancels and joins the Apply goroutine, and records the Failed phase unless
// the cause is a context cancellation (a cancellation is not a failure). The
// original cause is returned unchanged.
func (e *Engine) abort(ah *applyHandle, rep *Report, cause error) error {
	e.tailer.Stop()
	if ah.cancel != nil {
		ah.cancel()
	}
	ah.join()
	e.markIfHistoryLost(cause)
	if isCancel(cause) {
		return cause
	}
	e.recordPhase(rep, engine.PhaseFailed)
	return cause
}

// markIfHistoryLost flags the checkpoint non-resumable when err is a lost
// change-stream history, so a subsequent --resume fails fast instead of silently
// starting a change stream that would miss data.
func (e *Engine) markIfHistoryLost(err error) {
	if e.cp != nil && errors.Is(err, engine.ErrChangeStreamHistoryLost) {
		e.cp.markNonResumable()
	}
}

// isCancel reports whether err is a context cancellation or deadline error.
func isCancel(err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)
}

// copyError builds the error returned when a snapshot copy did not fully
// succeed. A transport-level error from Copy is wrapped directly; per-collection
// failures (reported through CopyResult even when Copy returned nil) are
// aggregated and wrapped around ErrCopyFailed.
func copyError(cr engine.CopyResult, err error) error {
	if err != nil {
		return fmt.Errorf("snapshot copy: %w", err)
	}
	var failed []string
	for _, c := range cr.Collections {
		if c.Err != "" {
			failed = append(failed, fmt.Sprintf("%s: %s", c.Namespace, c.Err))
		}
	}
	if len(failed) == 0 {
		// Defensive: Failed() was true but no per-collection error text was found.
		return fmt.Errorf("snapshot copy: %w", ErrCopyFailed)
	}
	return fmt.Errorf("snapshot copy: %d collection(s) failed [%s]: %w",
		len(failed), strings.Join(failed, "; "), ErrCopyFailed)
}
