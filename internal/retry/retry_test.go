package retry

import (
	"context"
	"errors"
	"testing"
	"time"
)

// errRetry and errFatal are sentinel errors used to drive the injected
// classifier deterministically; their identity is asserted with errors.Is.
var (
	errRetry = errors.New("transient boom")
	errFatal = errors.New("fatal boom")
)

// alwaysRetryable and alwaysFatal are trivial classifiers for the orchestration
// tests, keeping the MongoDB-specific classification out of this file.
func alwaysRetryable(error) Class { return Retryable }
func alwaysFatal(error) Class     { return Fatal }

// fixedNow returns a now() that always reports the same instant. It is used when
// a test exercises no MaxElapsed budget so the clock must not influence control
// flow.
func fixedNow() func() time.Time {
	t := time.Unix(0, 0)
	return func() time.Time { return t }
}

// fakeClock is a manually advanced clock. sleepAdvance wires it into Policy.sleep
// so injected backoff "sleeps" move virtual time forward without ever waiting.
type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }
func (c *fakeClock) sleepAdvance(_ context.Context, d time.Duration) error {
	c.advance(d)
	return nil
}

// recordSleep records every requested backoff duration instead of waiting, so a
// test can assert the exact backoff schedule deterministically.
type recordSleep struct{ delays []time.Duration }

func (r *recordSleep) sleep(_ context.Context, d time.Duration) error {
	r.delays = append(r.delays, d)
	return nil
}

func TestDoSuccessFirstTry(t *testing.T) {
	calls := 0
	err := Do(context.Background(), Policy{
		MaxAttempts: 3,
		BaseDelay:   time.Millisecond,
		now:         fixedNow(),
		sleep:       (&recordSleep{}).sleep,
	}, alwaysRetryable, func() error {
		calls++
		return nil
	})
	if err != nil {
		t.Fatalf("want nil error, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("want fn called once, got %d", calls)
	}
}

func TestDoRetryableThenSuccess(t *testing.T) {
	rec := &recordSleep{}
	calls := 0
	err := Do(context.Background(), Policy{
		MaxAttempts: 5,
		BaseDelay:   time.Millisecond,
		now:         fixedNow(),
		sleep:       rec.sleep,
	}, alwaysRetryable, func() error {
		calls++
		if calls < 3 {
			return errRetry
		}
		return nil
	})
	if err != nil {
		t.Fatalf("want nil error after recovery, got %v", err)
	}
	if calls != 3 {
		t.Fatalf("want fn called 3 times, got %d", calls)
	}
	if len(rec.delays) != 2 {
		t.Fatalf("want 2 backoff sleeps, got %d (%v)", len(rec.delays), rec.delays)
	}
}

func TestDoFatalImmediate(t *testing.T) {
	rec := &recordSleep{}
	calls := 0
	err := Do(context.Background(), Policy{
		MaxAttempts: 5,
		BaseDelay:   time.Millisecond,
		now:         fixedNow(),
		sleep:       rec.sleep,
	}, alwaysFatal, func() error {
		calls++
		return errFatal
	})
	if !errors.Is(err, errFatal) {
		t.Fatalf("want errFatal, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("want fn called once on fatal, got %d", calls)
	}
	if len(rec.delays) != 0 {
		t.Fatalf("want no backoff on fatal, got %v", rec.delays)
	}
}

func TestDoExhaustMaxAttempts(t *testing.T) {
	const maxAttempts = 4
	rec := &recordSleep{}
	calls := 0
	err := Do(context.Background(), Policy{
		MaxAttempts: maxAttempts,
		BaseDelay:   time.Millisecond,
		now:         fixedNow(),
		sleep:       rec.sleep,
	}, alwaysRetryable, func() error {
		calls++
		return errRetry
	})
	if !errors.Is(err, errRetry) {
		t.Fatalf("want last error errRetry, got %v", err)
	}
	if calls != maxAttempts {
		t.Fatalf("want fn called exactly %d times, got %d", maxAttempts, calls)
	}
	// One fewer backoff than attempts: no sleep after the final failed attempt.
	if len(rec.delays) != maxAttempts-1 {
		t.Fatalf("want %d backoffs, got %d (%v)", maxAttempts-1, len(rec.delays), rec.delays)
	}
}

func TestDoMaxElapsedStopsEarly(t *testing.T) {
	clock := &fakeClock{t: time.Unix(0, 0)}
	calls := 0
	// BaseDelay 1s, doubling, MaxElapsed 3s. Virtual time advances by each slept
	// backoff: attempt1 elapsed 0+1<=3 (sleep -> 1s), attempt2 elapsed 1+2<=3
	// (sleep -> 3s), attempt3 elapsed 3+4>3 -> stop. fn runs 3 times.
	err := Do(context.Background(), Policy{
		MaxAttempts: 100,
		BaseDelay:   time.Second,
		MaxElapsed:  3 * time.Second,
		now:         clock.now,
		sleep:       clock.sleepAdvance,
	}, alwaysRetryable, func() error {
		calls++
		return errRetry
	})
	if !errors.Is(err, errRetry) {
		t.Fatalf("want errRetry after elapsed cap, got %v", err)
	}
	if calls != 3 {
		t.Fatalf("want fn called 3 times before elapsed cap, got %d", calls)
	}
}

func TestDoCtxCancelDuringBackoff(t *testing.T) {
	calls := 0
	// sleep simulates the context being cancelled while we back off: it returns
	// context.Canceled, which Do must surface joined with the last attempt error.
	sleepCancel := func(context.Context, time.Duration) error { return context.Canceled }
	err := Do(context.Background(), Policy{
		MaxAttempts: 5,
		BaseDelay:   time.Millisecond,
		now:         fixedNow(),
		sleep:       sleepCancel,
	}, alwaysRetryable, func() error {
		calls++
		return errRetry
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want joined error to contain context.Canceled, got %v", err)
	}
	if !errors.Is(err, errRetry) {
		t.Fatalf("want joined error to contain errRetry, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("want fn called once before cancel, got %d", calls)
	}
}

func TestDoCtxAlreadyCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	calls := 0
	err := Do(ctx, Policy{
		MaxAttempts: 5,
		BaseDelay:   time.Millisecond,
		now:         fixedNow(),
		sleep:       (&recordSleep{}).sleep,
	}, alwaysRetryable, func() error {
		calls++
		return errRetry
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("want context.Canceled, got %v", err)
	}
	if calls != 0 {
		t.Fatalf("want fn never called when ctx is pre-cancelled, got %d", calls)
	}
}

func TestDoNilClassifierNoRetry(t *testing.T) {
	rec := &recordSleep{}
	calls := 0
	err := Do(context.Background(), Policy{
		MaxAttempts: 5,
		BaseDelay:   time.Millisecond,
		now:         fixedNow(),
		sleep:       rec.sleep,
	}, nil, func() error {
		calls++
		return errRetry
	})
	if !errors.Is(err, errRetry) {
		t.Fatalf("want errRetry, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("want fn called once with nil classifier, got %d", calls)
	}
	if len(rec.delays) != 0 {
		t.Fatalf("want no backoff with nil classifier, got %v", rec.delays)
	}
}

func TestDoBackoffDoubling(t *testing.T) {
	rec := &recordSleep{}
	_ = Do(context.Background(), Policy{
		MaxAttempts: 4,
		BaseDelay:   10 * time.Millisecond,
		MaxDelay:    0, // no cap so doubling is observable
		Jitter:      false,
		now:         fixedNow(),
		sleep:       rec.sleep,
	}, alwaysRetryable, func() error {
		return errRetry
	})
	want := []time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 40 * time.Millisecond}
	if len(rec.delays) != len(want) {
		t.Fatalf("want %d backoffs %v, got %d %v", len(want), want, len(rec.delays), rec.delays)
	}
	for i := range want {
		if rec.delays[i] != want[i] {
			t.Fatalf("backoff[%d] = %v, want %v (full schedule %v)", i, rec.delays[i], want[i], rec.delays)
		}
	}
}

func TestDoJitterRespected(t *testing.T) {
	rec := &recordSleep{}
	var jitterInputs []time.Duration
	// A deterministic jitter that quarters the delay, so we can prove Do passes
	// the pre-jitter backoff to jitterFn and then sleeps the jittered result.
	jitter := func(d time.Duration) time.Duration {
		jitterInputs = append(jitterInputs, d)
		return d / 4
	}
	_ = Do(context.Background(), Policy{
		MaxAttempts: 3,
		BaseDelay:   100 * time.Millisecond,
		MaxDelay:    0,
		Jitter:      true,
		now:         fixedNow(),
		sleep:       rec.sleep,
		jitterFn:    jitter,
	}, alwaysRetryable, func() error {
		return errRetry
	})
	wantIn := []time.Duration{100 * time.Millisecond, 200 * time.Millisecond}
	wantSlept := []time.Duration{25 * time.Millisecond, 50 * time.Millisecond}
	if len(jitterInputs) != len(wantIn) {
		t.Fatalf("jitter called %d times %v, want %v", len(jitterInputs), jitterInputs, wantIn)
	}
	for i := range wantIn {
		if jitterInputs[i] != wantIn[i] {
			t.Fatalf("jitter input[%d] = %v, want %v (pre-jitter backoff)", i, jitterInputs[i], wantIn[i])
		}
		if rec.delays[i] != wantSlept[i] {
			t.Fatalf("slept[%d] = %v, want jittered %v", i, rec.delays[i], wantSlept[i])
		}
	}
}
