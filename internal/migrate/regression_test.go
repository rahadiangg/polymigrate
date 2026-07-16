package migrate

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/weefer/mongomigrate/internal/engine"
)

// TestVerifyMismatchFails_Snapshot covers the review fix: a verifier that runs
// cleanly but reports Ok=false (a data mismatch) must make the run fail, not
// succeed. Previously only the verifier's transport error was checked.
func TestVerifyMismatchFails_Snapshot(t *testing.T) {
	c := &fakeCopier{result: engine.CopyResult{TotalDocs: 10}}
	v := &fakeVerifier{report: engine.VerifyReport{Ok: false}}
	eng := New(c, nil, v, nil, nil)

	rep, err := eng.Run(context.Background(), Options{Live: false, RunVerify: true})
	if !errors.Is(err, ErrVerifyFailed) {
		t.Fatalf("err = %v, want ErrVerifyFailed", err)
	}
	if last := rep.Phases[len(rep.Phases)-1]; last != engine.PhaseFailed {
		t.Errorf("last phase = %v, want failed", last)
	}
}

// TestVerifyMismatchFails_Live covers the same fix on the live path.
func TestVerifyMismatchFails_Live(t *testing.T) {
	c := &fakeCopier{result: engine.CopyResult{TotalDocs: 10}}
	tl := newFakeTailer(0, 0) // already drained
	v := &fakeVerifier{report: engine.VerifyReport{Ok: false}}
	eng := New(c, tl, v, nil, nil)

	rep, err := eng.Run(context.Background(), Options{
		Live: true, AutoCutover: true, LagThreshold: 0, RunVerify: true, PollInterval: time.Millisecond,
	})
	if !errors.Is(err, ErrVerifyFailed) {
		t.Fatalf("err = %v, want ErrVerifyFailed", err)
	}
	if last := rep.Phases[len(rep.Phases)-1]; last != engine.PhaseFailed {
		t.Errorf("last phase = %v, want failed", last)
	}
}

// bufferedTailer reports Lag=0 throughout but keeps Buffered>0 for the first
// `pending` drain polls, then 0. It proves the orchestrator waits for the buffer
// to truly empty (Buffered==0) instead of trusting Lag alone.
type bufferedTailer struct {
	mu        sync.Mutex
	statsN    int
	pending   int
	drainedAt int // the stats call number at which Buffered first read 0
	stopCh    chan struct{}
}

func (b *bufferedTailer) Open(context.Context, engine.TailOptions) error { return nil }
func (b *bufferedTailer) Apply(ctx context.Context) error {
	<-b.stopCh
	return nil
}
func (b *bufferedTailer) Stats() engine.TailStats {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.statsN++
	var buffered int64
	if b.statsN <= b.pending {
		buffered = 5
	} else if b.drainedAt == 0 {
		b.drainedAt = b.statsN
	}
	return engine.TailStats{Lag: 0, Buffered: buffered}
}
func (b *bufferedTailer) Token() engine.ResumeToken   { return nil }
func (b *bufferedTailer) Stop()                       { close(b.stopCh) }
func (b *bufferedTailer) Close(context.Context) error { return nil }

func TestLiveDrainWaitsForBufferEmpty(t *testing.T) {
	c := &fakeCopier{result: engine.CopyResult{TotalDocs: 1}}
	tl := &bufferedTailer{pending: 3, stopCh: make(chan struct{})}
	v := &fakeVerifier{report: engine.VerifyReport{Ok: true}}
	eng := New(c, tl, v, nil, nil)

	rep, err := eng.Run(context.Background(), Options{
		Live: true, AutoCutover: true, LagThreshold: 0, RunVerify: true, PollInterval: time.Millisecond,
	})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	// The tailer must not have been Stopped (cutover completed) until Buffered hit 0.
	if tl.drainedAt == 0 {
		t.Fatal("buffer never reported drained")
	}
	if last := rep.Phases[len(rep.Phases)-1]; last != engine.PhaseDone {
		t.Errorf("last phase = %v, want done", last)
	}
}
