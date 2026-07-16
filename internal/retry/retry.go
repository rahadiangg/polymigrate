// Package retry provides bounded exponential backoff with jitter for the
// transient failures a migration hits mid-flight (network blips, primary
// step-downs, write-concern timeouts). It is deliberately small and
// dependency-free at its core: the MongoDB-specific error classification lives
// in classify.go so this file stays unit-testable with an injected clock.
package retry

import (
	"context"
	"errors"
	"math/rand/v2"
	"time"
)

// Class is the retry disposition of an error.
type Class int

const (
	// Fatal errors are not retried (auth failures, invalid namespaces, lost
	// change-stream history, etc.).
	Fatal Class = iota
	// Retryable errors are transient and worth retrying (sockets, timeouts,
	// not-primary, pool churn).
	Retryable
)

// Classifier decides whether an error is worth retrying. A nil classifier treats
// every non-nil error as Fatal (i.e. no retries).
type Classifier func(error) Class

// Policy bounds a retry loop.
type Policy struct {
	// MaxAttempts is the total number of attempts (>=1). 0 means a default of 5.
	MaxAttempts int
	// BaseDelay is the first backoff delay; it doubles each attempt.
	BaseDelay time.Duration
	// MaxDelay caps a single backoff delay (0 = no cap).
	MaxDelay time.Duration
	// MaxElapsed caps the total time across all attempts (0 = no cap).
	MaxElapsed time.Duration
	// Jitter, when true, randomises each delay in [BaseDelay/2, BaseDelay] style
	// full jitter to avoid thundering herds. The jitter source is injectable via
	// jitterFn for deterministic tests.
	Jitter bool

	// sleep and now are injectable for tests; nil uses the real clock.
	sleep    func(context.Context, time.Duration) error
	now      func() time.Time
	jitterFn func(time.Duration) time.Duration
}

// DefaultPolicy returns a sensible retry policy for network operations.
func DefaultPolicy() Policy {
	return Policy{
		MaxAttempts: 5,
		BaseDelay:   250 * time.Millisecond,
		MaxDelay:    10 * time.Second,
		MaxElapsed:  2 * time.Minute,
		Jitter:      true,
	}
}

// Do runs fn, retrying while classify reports Retryable and the policy budget
// (attempts and elapsed time) is not exhausted. It returns nil on the first
// success, the last error when retries are exhausted, ctx.Err() on cancellation,
// or the error immediately when classify reports Fatal. classify may be nil
// (no retries).
func Do(ctx context.Context, p Policy, classify Classifier, fn func() error) error {
	p = p.withDefaults()
	start := p.now()
	delay := p.BaseDelay
	var lastErr error

	for attempt := 1; ; attempt++ {
		if err := ctx.Err(); err != nil {
			if lastErr != nil {
				return errors.Join(lastErr, err)
			}
			return err
		}

		lastErr = fn()
		if lastErr == nil {
			return nil
		}
		if classify == nil || classify(lastErr) == Fatal {
			return lastErr
		}
		if attempt >= p.MaxAttempts {
			return lastErr
		}

		wait := delay
		if p.Jitter {
			wait = p.jitterFn(wait)
		}
		if p.MaxDelay > 0 && wait > p.MaxDelay {
			wait = p.MaxDelay
		}
		if p.MaxElapsed > 0 {
			elapsed := p.now().Sub(start)
			if elapsed+wait > p.MaxElapsed {
				return lastErr
			}
		}
		if err := p.sleep(ctx, wait); err != nil {
			return errors.Join(lastErr, err)
		}

		delay *= 2
		if p.MaxDelay > 0 && delay > p.MaxDelay {
			delay = p.MaxDelay
		}
	}
}

func (p Policy) withDefaults() Policy {
	if p.MaxAttempts <= 0 {
		p.MaxAttempts = 5
	}
	if p.BaseDelay <= 0 {
		p.BaseDelay = 250 * time.Millisecond
	}
	if p.now == nil {
		p.now = time.Now
	}
	if p.sleep == nil {
		p.sleep = sleepCtx
	}
	if p.jitterFn == nil {
		p.jitterFn = fullJitter
	}
	return p
}

// fullJitter randomises a backoff delay to [d/2, d] ("equal jitter"), spreading
// retries so concurrent workers do not synchronise. It is only used when
// Policy.Jitter is set and is replaced by a deterministic function in tests.
func fullJitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	half := d / 2
	return half + time.Duration(rand.Int64N(int64(d-half)+1))
}

// sleepCtx sleeps for d or returns early with ctx.Err() if the context is done.
func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}
