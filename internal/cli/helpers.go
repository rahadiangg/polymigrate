package cli

import (
	"context"
	"time"
)

// contextWithTimeout derives a timeout context from parent. A non-positive
// timeout yields a plain cancel context (no deadline).
func contextWithTimeout(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return context.WithCancel(parent)
	}
	return context.WithTimeout(parent, timeout)
}
