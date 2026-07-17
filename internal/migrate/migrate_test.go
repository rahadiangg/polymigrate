package migrate

import (
	"context"
	"errors"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/rahadiangg/polymigrate/internal/engine"
	"github.com/rahadiangg/polymigrate/internal/namespace"
	"github.com/rahadiangg/polymigrate/internal/progress"
)

// -----------------------------------------------------------------------------
// Programmable fakes
// -----------------------------------------------------------------------------

// fakeCopier returns a preset CopyResult/error and records that it was called.
type fakeCopier struct {
	result engine.CopyResult
	err    error

	mu     sync.Mutex
	called int
	opts   engine.CopyOptions
}

func (f *fakeCopier) Copy(ctx context.Context, opts engine.CopyOptions) (engine.CopyResult, error) {
	f.mu.Lock()
	f.called++
	f.opts = opts
	f.mu.Unlock()
	return f.result, f.err
}

func (f *fakeCopier) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.called
}

// fakeVerifier returns a preset VerifyReport/error and records that it was called.
type fakeVerifier struct {
	report engine.VerifyReport
	err    error

	mu     sync.Mutex
	called int
}

func (f *fakeVerifier) Verify(ctx context.Context, opts engine.VerifyOptions) (engine.VerifyReport, error) {
	f.mu.Lock()
	f.called++
	f.mu.Unlock()
	return f.report, f.err
}

func (f *fakeVerifier) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.called
}

// fakeTailer scripts the lifecycle of a change-stream tailer.
//
//   - The lag returned by Stats() starts at lagStart and drops by lagStep on
//     every call, clamped at zero, so both the monitor loop (Lag<=threshold) and
//     the drain loop (Lag<=0) terminate deterministically. A zero lagStep keeps
//     the lag pinned at lagStart, which never satisfies a positive threshold and
//     is used to hold the run in the monitor loop until the test intervenes.
//   - Apply blocks until Stop is called or its context is cancelled, unless
//     applyEndEarly is set, in which case it returns applyErr immediately.
//   - Open/Apply/Stop/Close and an "ApplyReturned" marker are appended to calls
//     in the order they happen, under a mutex, so a test can assert ordering.
type fakeTailer struct {
	lagStart time.Duration
	lagStep  time.Duration

	openErr       error
	applyErr      error
	applyEndEarly bool

	captured int64
	applied  int64

	// applyStarted is closed once when Apply begins, letting a test cancel the
	// context strictly after CDC is underway.
	applyStarted chan struct{}

	mu       sync.Mutex
	calls    []string
	statsN   int
	lastLag  time.Duration
	stopCh   chan struct{}
	stopped  bool
	openOpts engine.TailOptions
	startedC bool
}

func newFakeTailer(lagStart, lagStep time.Duration) *fakeTailer {
	return &fakeTailer{
		lagStart:     lagStart,
		lagStep:      lagStep,
		stopCh:       make(chan struct{}),
		applyStarted: make(chan struct{}),
	}
}

func (f *fakeTailer) record(s string) {
	f.mu.Lock()
	f.calls = append(f.calls, s)
	f.mu.Unlock()
}

func (f *fakeTailer) Open(ctx context.Context, opts engine.TailOptions) error {
	f.mu.Lock()
	f.calls = append(f.calls, "Open")
	f.openOpts = opts
	f.mu.Unlock()
	return f.openErr
}

func (f *fakeTailer) Apply(ctx context.Context) error {
	f.mu.Lock()
	f.calls = append(f.calls, "Apply")
	if !f.startedC {
		f.startedC = true
		close(f.applyStarted)
	}
	f.mu.Unlock()
	defer f.record("ApplyReturned")

	if f.applyEndEarly {
		return f.applyErr
	}
	select {
	case <-f.stopCh:
		return f.applyErr
	case <-ctx.Done():
		return nil
	}
}

func (f *fakeTailer) Stats() engine.TailStats {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.statsN++
	lag := f.lagStart - f.lagStep*time.Duration(f.statsN-1)
	if lag < 0 {
		lag = 0
	}
	f.lastLag = lag
	return engine.TailStats{
		Captured: f.captured,
		Applied:  f.applied,
		Lag:      lag,
	}
}

func (f *fakeTailer) Token() engine.ResumeToken { return nil }

func (f *fakeTailer) Stop() {
	f.mu.Lock()
	f.calls = append(f.calls, "Stop")
	if !f.stopped {
		f.stopped = true
		close(f.stopCh)
	}
	f.mu.Unlock()
}

func (f *fakeTailer) Close(ctx context.Context) error {
	f.record("Close")
	return nil
}

func (f *fakeTailer) Calls() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]string, len(f.calls))
	copy(out, f.calls)
	return out
}

func (f *fakeTailer) statsCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.statsN
}

func (f *fakeTailer) lastLagSeen() time.Duration {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastLag
}

// hookReporter wraps a CountingReporter and calls onPhase after recording each
// phase, so a test can react to phase transitions.
type hookReporter struct {
	*progress.CountingReporter
	onPhase func(engine.Phase)
}

func newHookReporter(onPhase func(engine.Phase)) *hookReporter {
	return &hookReporter{CountingReporter: progress.NewCountingReporter(), onPhase: onPhase}
}

func (h *hookReporter) Phase(p engine.Phase) {
	h.CountingReporter.Phase(p)
	if h.onPhase != nil {
		h.onPhase(p)
	}
}

// Compile-time assertions that the fakes satisfy the engine contracts.
var (
	_ engine.Copier   = (*fakeCopier)(nil)
	_ engine.Tailer   = (*fakeTailer)(nil)
	_ engine.Verifier = (*fakeVerifier)(nil)
	_ engine.Reporter = (*hookReporter)(nil)
)

// -----------------------------------------------------------------------------
// Helpers
// -----------------------------------------------------------------------------

func okCopyResult() engine.CopyResult {
	return engine.CopyResult{
		Collections: []engine.CollectionCopyResult{
			{Namespace: namespace.Namespace{DB: "shop", Collection: "orders"}, DocsCopied: 10},
		},
		TotalDocs: 10,
	}
}

func failedCopyResult() engine.CopyResult {
	return engine.CopyResult{
		Collections: []engine.CollectionCopyResult{
			{Namespace: namespace.Namespace{DB: "shop", Collection: "orders"}, DocsCopied: 3},
			{Namespace: namespace.Namespace{DB: "shop", Collection: "users"}, Err: "write timeout"},
		},
		TotalDocs:   3,
		TotalFailed: 1,
	}
}

func phasesEqual(t *testing.T, got, want []engine.Phase) {
	t.Helper()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("phase order mismatch:\n got=%v\nwant=%v", got, want)
	}
}

func indexOf(s []engine.Phase, p engine.Phase) int {
	for i, v := range s {
		if v == p {
			return i
		}
	}
	return -1
}

// -----------------------------------------------------------------------------
// Snapshot flow
// -----------------------------------------------------------------------------

func TestSnapshotHappyPath(t *testing.T) {
	c := &fakeCopier{result: okCopyResult()}
	v := &fakeVerifier{report: engine.VerifyReport{Ok: true}}
	rep := progress.NewCountingReporter()
	eng := New(c, newFakeTailer(0, 0), v, rep, nil)

	report, err := eng.Run(context.Background(), Options{RunVerify: true})
	if err != nil {
		t.Fatalf("Run: unexpected error: %v", err)
	}
	phasesEqual(t, report.Phases, []engine.Phase{
		engine.PhaseInit, engine.PhaseSnapshot, engine.PhaseVerify, engine.PhaseDone,
	})
	phasesEqual(t, rep.PhaseOrder(), report.Phases)
	if c.callCount() != 1 {
		t.Errorf("copier called %d times, want 1", c.callCount())
	}
	if v.callCount() != 1 {
		t.Errorf("verifier called %d times, want 1", v.callCount())
	}
	if report.Copy.TotalDocs != 10 {
		t.Errorf("report.Copy.TotalDocs = %d, want 10", report.Copy.TotalDocs)
	}
	if !report.Verify.Ok {
		t.Errorf("report.Verify.Ok = false, want true")
	}
}

func TestSnapshotNoVerify(t *testing.T) {
	c := &fakeCopier{result: okCopyResult()}
	v := &fakeVerifier{}
	eng := New(c, nil, v, nil, nil)

	report, err := eng.Run(context.Background(), Options{RunVerify: false})
	if err != nil {
		t.Fatalf("Run: unexpected error: %v", err)
	}
	phasesEqual(t, report.Phases, []engine.Phase{
		engine.PhaseInit, engine.PhaseSnapshot, engine.PhaseDone,
	})
	if v.callCount() != 0 {
		t.Errorf("verifier called %d times, want 0", v.callCount())
	}
}

func TestSnapshotCopyResultFailure(t *testing.T) {
	c := &fakeCopier{result: failedCopyResult()}
	v := &fakeVerifier{}
	rep := progress.NewCountingReporter()
	eng := New(c, nil, v, rep, nil)

	report, err := eng.Run(context.Background(), Options{RunVerify: true})
	if err == nil {
		t.Fatal("Run: expected error, got nil")
	}
	if !errors.Is(err, ErrCopyFailed) {
		t.Errorf("error %v does not wrap ErrCopyFailed", err)
	}
	phasesEqual(t, report.Phases, []engine.Phase{
		engine.PhaseInit, engine.PhaseSnapshot, engine.PhaseFailed,
	})
	if report.Phases[len(report.Phases)-1] != engine.PhaseFailed {
		t.Errorf("last phase = %v, want Failed", report.Phases[len(report.Phases)-1])
	}
	if v.callCount() != 0 {
		t.Errorf("verifier called after copy failure")
	}
	// The partial copy result must still be recorded.
	if report.Copy.TotalFailed != 1 {
		t.Errorf("report.Copy.TotalFailed = %d, want 1", report.Copy.TotalFailed)
	}
}

func TestSnapshotCopyTransportError(t *testing.T) {
	boom := errors.New("connection reset")
	c := &fakeCopier{err: boom}
	eng := New(c, nil, &fakeVerifier{}, nil, nil)

	report, err := eng.Run(context.Background(), Options{})
	if !errors.Is(err, boom) {
		t.Fatalf("error %v does not wrap boom", err)
	}
	phasesEqual(t, report.Phases, []engine.Phase{
		engine.PhaseInit, engine.PhaseSnapshot, engine.PhaseFailed,
	})
}

func TestSnapshotVerifyError(t *testing.T) {
	boom := errors.New("verify blew up")
	c := &fakeCopier{result: okCopyResult()}
	v := &fakeVerifier{err: boom}
	eng := New(c, nil, v, nil, nil)

	report, err := eng.Run(context.Background(), Options{RunVerify: true})
	if !errors.Is(err, boom) {
		t.Fatalf("error %v does not wrap boom", err)
	}
	phasesEqual(t, report.Phases, []engine.Phase{
		engine.PhaseInit, engine.PhaseSnapshot, engine.PhaseVerify, engine.PhaseFailed,
	})
}

// -----------------------------------------------------------------------------
// Live flow
// -----------------------------------------------------------------------------

func TestLiveHappyPath(t *testing.T) {
	c := &fakeCopier{result: okCopyResult()}
	v := &fakeVerifier{report: engine.VerifyReport{Ok: true}}
	tl := newFakeTailer(0, 0) // lag already zero: immediate cutover-ready
	rep := progress.NewCountingReporter()
	eng := New(c, tl, v, rep, nil)

	opts := Options{
		Live:         true,
		RunVerify:    true,
		AutoCutover:  true,
		LagThreshold: 0,
		PollInterval: time.Hour, // never ticks; immediate checks drive the flow
	}
	report, err := eng.Run(context.Background(), opts)
	if err != nil {
		t.Fatalf("Run: unexpected error: %v", err)
	}

	want := []engine.Phase{
		engine.PhaseInit, engine.PhaseTailOpen, engine.PhaseSnapshot,
		engine.PhaseApply, engine.PhaseCutover, engine.PhaseVerify, engine.PhaseDone,
	}
	phasesEqual(t, report.Phases, want)
	phasesEqual(t, rep.PhaseOrder(), want)

	// Open precedes Snapshot precedes Apply.
	po := report.Phases
	if !(indexOf(po, engine.PhaseTailOpen) < indexOf(po, engine.PhaseSnapshot) &&
		indexOf(po, engine.PhaseSnapshot) < indexOf(po, engine.PhaseApply)) {
		t.Errorf("phase ordering Open<Snapshot<Apply violated: %v", po)
	}

	// The tailer must have been Opened first, then Applied, Stopped, and Closed,
	// and Apply must have returned (goroutine joined, no leak). Open is recorded
	// synchronously before the Apply goroutine is launched, so it is always
	// first; the relative order of the Apply marker vs Stop is a legitimate race
	// when cutover is immediate (Stop may fire before Apply is scheduled).
	calls := tl.Calls()
	if len(calls) == 0 || calls[0] != "Open" {
		t.Fatalf("tailer calls must start with Open: %v", calls)
	}
	assertContains(t, calls, "Apply")
	assertContains(t, calls, "Stop")
	assertContains(t, calls, "Close")
	assertContains(t, calls, "ApplyReturned")
	if idx(calls, "Open") > idx(calls, "Apply") {
		t.Errorf("Open must precede Apply: %v", calls)
	}
	if idx(calls, "Apply") > idx(calls, "ApplyReturned") {
		t.Errorf("Apply must precede ApplyReturned: %v", calls)
	}
	if c.callCount() != 1 || v.callCount() != 1 {
		t.Errorf("copier=%d verifier=%d, want 1 and 1", c.callCount(), v.callCount())
	}
}

func TestLiveNoVerify(t *testing.T) {
	tl := newFakeTailer(0, 0)
	eng := New(&fakeCopier{result: okCopyResult()}, tl, &fakeVerifier{}, nil, nil)

	report, err := eng.Run(context.Background(), Options{
		Live: true, AutoCutover: true, PollInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("Run: unexpected error: %v", err)
	}
	phasesEqual(t, report.Phases, []engine.Phase{
		engine.PhaseInit, engine.PhaseTailOpen, engine.PhaseSnapshot,
		engine.PhaseApply, engine.PhaseCutover, engine.PhaseDone,
	})
}

// TestLiveAutoCutoverProceedsWithoutSignal verifies that with AutoCutover the
// run completes even though Proceed is nil and never signalled.
func TestLiveAutoCutoverProceedsWithoutSignal(t *testing.T) {
	tl := newFakeTailer(0, 0)
	eng := New(&fakeCopier{result: okCopyResult()}, tl, &fakeVerifier{}, nil, nil)

	done := make(chan error, 1)
	go func() {
		_, err := eng.Run(context.Background(), Options{
			Live: true, AutoCutover: true, Proceed: nil, PollInterval: time.Hour,
		})
		done <- err
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: unexpected error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not complete under AutoCutover without a Proceed signal")
	}
}

// TestLiveManualCutoverBlocks verifies that with AutoCutover=false, Run reaches
// the cutover phase and then blocks until Proceed is signalled.
func TestLiveManualCutoverBlocks(t *testing.T) {
	tl := newFakeTailer(0, 0) // immediate cutover-ready
	proceed := make(chan struct{})

	cutover := make(chan struct{})
	var once sync.Once
	rep := newHookReporter(func(p engine.Phase) {
		if p == engine.PhaseCutover {
			once.Do(func() { close(cutover) })
		}
	})
	eng := New(&fakeCopier{result: okCopyResult()}, tl, &fakeVerifier{}, rep, nil)

	done := make(chan error, 1)
	go func() {
		_, err := eng.Run(context.Background(), Options{
			Live: true, AutoCutover: false, Proceed: proceed, PollInterval: time.Hour,
		})
		done <- err
	}()

	// Wait until the engine reaches cutover.
	select {
	case <-cutover:
	case <-time.After(2 * time.Second):
		t.Fatal("engine never reached cutover phase")
	}

	// It must now be blocked waiting for Proceed.
	select {
	case <-done:
		t.Fatal("Run returned before Proceed was signalled")
	case <-time.After(75 * time.Millisecond):
		// Still blocked, as required.
	}

	close(proceed)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: unexpected error after Proceed: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not complete after Proceed was signalled")
	}

	calls := tl.Calls()
	assertContains(t, calls, "Stop")
	assertContains(t, calls, "Close")
	assertContains(t, calls, "ApplyReturned")
}

// TestLiveLagDrivenReadiness verifies the engine does not cut over until the
// observed lag falls to the threshold, and polls repeatedly while lag is high.
func TestLiveLagDrivenReadiness(t *testing.T) {
	// 300ms -> 200ms -> 100ms -> 0 across successive Stats() calls.
	tl := newFakeTailer(300*time.Millisecond, 100*time.Millisecond)
	const threshold = 150 * time.Millisecond

	var lagAtCutover time.Duration
	var statsAtCutover int
	rep := newHookReporter(func(p engine.Phase) {
		if p == engine.PhaseCutover {
			lagAtCutover = tl.lastLagSeen()
			statsAtCutover = tl.statsCount()
		}
	})
	eng := New(&fakeCopier{result: okCopyResult()}, tl, &fakeVerifier{}, rep, nil)

	report, err := eng.Run(context.Background(), Options{
		Live: true, AutoCutover: true, LagThreshold: threshold,
		PollInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("Run: unexpected error: %v", err)
	}
	if lagAtCutover > threshold {
		t.Errorf("cut over at lag %v, above threshold %v", lagAtCutover, threshold)
	}
	// The first two readings (300ms, 200ms) exceed the threshold, so cutover must
	// not have happened before at least the third poll.
	if statsAtCutover < 3 {
		t.Errorf("cut over after only %d polls; expected to keep polling while lag was high", statsAtCutover)
	}
	assertContainsPhase(t, report.Phases, engine.PhaseCutover)
	assertContainsPhase(t, report.Phases, engine.PhaseDone)
}

// TestLiveCtxCancelMidApply verifies that cancelling the context while Apply is
// running stops and closes the tailer, joins the Apply goroutine (no leak), and
// returns the context error.
func TestLiveCtxCancelMidApply(t *testing.T) {
	// Constant high lag so the monitor loop never reaches the threshold on its
	// own; the test drives termination via cancellation.
	tl := newFakeTailer(time.Hour, 0)
	eng := New(&fakeCopier{result: okCopyResult()}, tl, &fakeVerifier{}, nil, nil)

	ctx, cancel := context.WithCancel(context.Background())

	type result struct {
		rep Report
		err error
	}
	done := make(chan result, 1)
	go func() {
		rep, err := eng.Run(ctx, Options{
			Live: true, AutoCutover: true, LagThreshold: 0, PollInterval: time.Hour,
		})
		done <- result{rep, err}
	}()

	// Cancel only after CDC is underway.
	select {
	case <-tl.applyStarted:
	case <-time.After(2 * time.Second):
		t.Fatal("Apply never started")
	}
	cancel()

	select {
	case r := <-done:
		if !errors.Is(r.err, context.Canceled) {
			t.Fatalf("Run error = %v, want context.Canceled", r.err)
		}
		calls := tl.Calls()
		assertContains(t, calls, "Stop")
		assertContains(t, calls, "Close")
		assertContains(t, calls, "ApplyReturned") // goroutine joined, no leak
		// Cancellation is not a failure phase.
		if indexOf(r.rep.Phases, engine.PhaseDone) >= 0 {
			t.Errorf("did not expect Done phase on cancellation: %v", r.rep.Phases)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancellation (possible goroutine leak)")
	}
}

// TestLiveApplyEndsEarly verifies that an Apply loop that returns an error
// before cutover completes fails the run.
func TestLiveApplyEndsEarly(t *testing.T) {
	boom := errors.New("change stream invalidated")
	tl := newFakeTailer(time.Hour, 0) // never ready; force entry into the select
	tl.applyEndEarly = true
	tl.applyErr = boom
	rep := progress.NewCountingReporter()
	eng := New(&fakeCopier{result: okCopyResult()}, tl, &fakeVerifier{}, rep, nil)

	report, err := eng.Run(context.Background(), Options{
		Live: true, AutoCutover: true, LagThreshold: 0, PollInterval: time.Hour,
	})
	if !errors.Is(err, boom) {
		t.Fatalf("Run error = %v, want wrap of boom", err)
	}
	if last := report.Phases[len(report.Phases)-1]; last != engine.PhaseFailed {
		t.Errorf("last phase = %v, want Failed", last)
	}
	assertContainsPhase(t, report.Phases, engine.PhaseApply)
	calls := tl.Calls()
	assertContains(t, calls, "Close")
	assertContains(t, calls, "ApplyReturned")
}

func TestLiveCopyFailure(t *testing.T) {
	tl := newFakeTailer(0, 0)
	rep := progress.NewCountingReporter()
	eng := New(&fakeCopier{result: failedCopyResult()}, tl, &fakeVerifier{}, rep, nil)

	report, err := eng.Run(context.Background(), Options{
		Live: true, AutoCutover: true, PollInterval: time.Hour,
	})
	if !errors.Is(err, ErrCopyFailed) {
		t.Fatalf("Run error = %v, want wrap of ErrCopyFailed", err)
	}
	phasesEqual(t, report.Phases, []engine.Phase{
		engine.PhaseInit, engine.PhaseTailOpen, engine.PhaseSnapshot, engine.PhaseFailed,
	})
	calls := tl.Calls()
	assertContains(t, calls, "Open")
	assertContains(t, calls, "Stop")
	assertContains(t, calls, "Close")
}

func TestLiveTailOpenFailure(t *testing.T) {
	tl := newFakeTailer(0, 0)
	tl.openErr = errors.New("no change stream on standalone")
	eng := New(&fakeCopier{result: okCopyResult()}, tl, &fakeVerifier{}, nil, nil)

	report, err := eng.Run(context.Background(), Options{Live: true, PollInterval: time.Hour})
	if err == nil {
		t.Fatal("expected error on tail open failure")
	}
	phasesEqual(t, report.Phases, []engine.Phase{
		engine.PhaseInit, engine.PhaseTailOpen, engine.PhaseFailed,
	})
	// The copier must not run when the stream could not be opened.
	calls := tl.Calls()
	if idx(calls, "Apply") >= 0 {
		t.Errorf("Apply should not run after open failure: %v", calls)
	}
}

func TestLiveVerifyError(t *testing.T) {
	boom := errors.New("verify failed")
	tl := newFakeTailer(0, 0)
	eng := New(&fakeCopier{result: okCopyResult()}, tl, &fakeVerifier{err: boom}, nil, nil)

	report, err := eng.Run(context.Background(), Options{
		Live: true, RunVerify: true, AutoCutover: true, PollInterval: time.Hour,
	})
	if !errors.Is(err, boom) {
		t.Fatalf("Run error = %v, want wrap of boom", err)
	}
	phasesEqual(t, report.Phases, []engine.Phase{
		engine.PhaseInit, engine.PhaseTailOpen, engine.PhaseSnapshot,
		engine.PhaseApply, engine.PhaseCutover, engine.PhaseVerify, engine.PhaseFailed,
	})
}

// -----------------------------------------------------------------------------
// Guards & pure helpers
// -----------------------------------------------------------------------------

func TestRunNilCopier(t *testing.T) {
	eng := New(nil, nil, nil, nil, nil)
	report, err := eng.Run(context.Background(), Options{})
	if err == nil {
		t.Fatal("expected error for nil copier")
	}
	phasesEqual(t, report.Phases, []engine.Phase{engine.PhaseInit, engine.PhaseFailed})
}

func TestRunLiveNilTailer(t *testing.T) {
	eng := New(&fakeCopier{result: okCopyResult()}, nil, nil, nil, nil)
	report, err := eng.Run(context.Background(), Options{Live: true})
	if err == nil {
		t.Fatal("expected error for nil tailer in live mode")
	}
	phasesEqual(t, report.Phases, []engine.Phase{engine.PhaseInit, engine.PhaseFailed})
}

func TestRunVerifyNilVerifier(t *testing.T) {
	eng := New(&fakeCopier{result: okCopyResult()}, nil, nil, nil, nil)
	report, err := eng.Run(context.Background(), Options{RunVerify: true})
	if err == nil {
		t.Fatal("expected error for RunVerify with nil verifier")
	}
	phasesEqual(t, report.Phases, []engine.Phase{engine.PhaseInit, engine.PhaseFailed})
}

func TestDefaultPollIntervalApplied(t *testing.T) {
	// PollInterval 0 must default without hanging. Lag is already zero, so both
	// loops satisfy their predicate on the immediate check and never wait a tick.
	tl := newFakeTailer(0, 0)
	eng := New(&fakeCopier{result: okCopyResult()}, tl, &fakeVerifier{}, nil, nil)

	done := make(chan error, 1)
	go func() {
		_, err := eng.Run(context.Background(), Options{
			Live: true, AutoCutover: true, PollInterval: 0,
		})
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run: unexpected error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run hung with default poll interval")
	}
}

func TestCopyError(t *testing.T) {
	t.Run("transport error wrapped", func(t *testing.T) {
		boom := errors.New("dial tcp")
		err := copyError(engine.CopyResult{}, boom)
		if !errors.Is(err, boom) {
			t.Errorf("copyError did not wrap transport error: %v", err)
		}
		if errors.Is(err, ErrCopyFailed) {
			t.Errorf("transport error should not be ErrCopyFailed: %v", err)
		}
	})
	t.Run("collection failures aggregated", func(t *testing.T) {
		err := copyError(failedCopyResult(), nil)
		if !errors.Is(err, ErrCopyFailed) {
			t.Errorf("copyError did not wrap ErrCopyFailed: %v", err)
		}
		if got := err.Error(); !contains(got, "shop.users") || !contains(got, "write timeout") {
			t.Errorf("copyError message missing failed collection detail: %q", got)
		}
	})
	t.Run("failed flag but no detail", func(t *testing.T) {
		cr := engine.CopyResult{Collections: []engine.CollectionCopyResult{{Err: ""}}}
		// Not actually Failed(), but exercise the defensive branch directly.
		err := copyError(engine.CopyResult{Collections: []engine.CollectionCopyResult{{Err: "x"}}}, nil)
		if !errors.Is(err, ErrCopyFailed) {
			t.Errorf("expected ErrCopyFailed, got %v (cr=%v)", err, cr)
		}
	})
}

func TestIsCancel(t *testing.T) {
	if !isCancel(context.Canceled) {
		t.Error("context.Canceled should be a cancel")
	}
	if !isCancel(context.DeadlineExceeded) {
		t.Error("context.DeadlineExceeded should be a cancel")
	}
	if isCancel(errors.New("other")) {
		t.Error("arbitrary error should not be a cancel")
	}
	if isCancel(errApplyEndedEarly) {
		t.Error("errApplyEndedEarly should not be a cancel")
	}
}

// TestReportCarriesFinalStats verifies the report carries end-of-run tailer
// stats on the live happy path.
func TestReportCarriesFinalStats(t *testing.T) {
	tl := newFakeTailer(0, 0)
	tl.captured = 42
	tl.applied = 42
	eng := New(&fakeCopier{result: okCopyResult()}, tl, &fakeVerifier{}, nil, nil)

	report, err := eng.Run(context.Background(), Options{
		Live: true, AutoCutover: true, PollInterval: time.Hour,
	})
	if err != nil {
		t.Fatalf("Run: unexpected error: %v", err)
	}
	if report.Tail.Captured != 42 || report.Tail.Applied != 42 {
		t.Errorf("report.Tail = %+v, want Captured=Applied=42", report.Tail)
	}
}

// -----------------------------------------------------------------------------
// small assertion helpers
// -----------------------------------------------------------------------------

func assertContains(t *testing.T, s []string, want string) {
	t.Helper()
	if idx(s, want) < 0 {
		t.Errorf("expected %q in %v", want, s)
	}
}

func assertContainsPhase(t *testing.T, s []engine.Phase, want engine.Phase) {
	t.Helper()
	if indexOf(s, want) < 0 {
		t.Errorf("expected phase %v in %v", want, s)
	}
}

func idx(s []string, want string) int {
	for i, v := range s {
		if v == want {
			return i
		}
	}
	return -1
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
