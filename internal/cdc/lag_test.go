package cdc

import (
	"testing"
	"time"
)

// TestComputeLag locks in the invariant that Lag is strictly positive whenever
// events are buffered — the guard against a premature cutover dropping data.
func TestComputeLag(t *testing.T) {
	now := time.Unix(1000, 0)

	// Nothing buffered -> exactly zero.
	if got := computeLag(0, time.Time{}, time.Time{}, now); got != 0 {
		t.Errorf("empty buffer lag = %v, want 0", got)
	}
	if got := computeLag(0, now.Add(-time.Hour), now.Add(-time.Hour), now); got != 0 {
		t.Errorf("empty buffer with timestamps lag = %v, want 0", got)
	}

	// Buffered with a known head -> age of the head.
	if got := computeLag(5, now.Add(-2*time.Second), time.Time{}, now); got != 2*time.Second {
		t.Errorf("lag = %v, want 2s", got)
	}

	// Buffered, head reset between batches (oldestPendingAt zero) -> fall back to
	// lastEventAt; MUST be > 0. This is the exact between-batches window that used
	// to report 0.
	if got := computeLag(5, time.Time{}, now.Add(-3*time.Second), now); got != 3*time.Second {
		t.Errorf("between-batches lag = %v, want 3s", got)
	}

	// Buffered but no timestamps at all -> sentinel, still strictly positive.
	if got := computeLag(1, time.Time{}, time.Time{}, now); got <= 0 {
		t.Errorf("sentinel lag = %v, want > 0", got)
	}

	// Backwards clock must not produce a non-positive lag while buffered.
	if got := computeLag(1, now.Add(time.Second), time.Time{}, now); got <= 0 {
		t.Errorf("backwards-clock lag = %v, want > 0", got)
	}
}
