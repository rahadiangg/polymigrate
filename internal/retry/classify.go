package retry

import (
	"context"
	"errors"

	"go.mongodb.org/mongo-driver/v2/mongo"
)

// MongoClassifier classifies a MongoDB driver error as Retryable or Fatal for
// use as the Classifier in Do. It is deliberately conservative: only errors we
// positively recognise as transient are retried; every other error — including
// any we do not recognise — is treated as Fatal so a genuinely broken migration
// fails fast instead of hammering the server.
//
// Ordering matters. Context cancellation is checked first so that a caller
// cancelling or timing out its own context always wins: those errors are Fatal
// here and the retry loop stops immediately rather than fighting the caller's
// lifecycle. (The driver would otherwise report context.DeadlineExceeded as a
// timeout via mongo.IsTimeout, which we must not retry.)
func MongoClassifier(err error) Class {
	// Defensive: Do never calls a classifier with a nil error, but if it did we
	// must not report "retry" for the absence of a failure.
	if err == nil {
		return Fatal
	}

	// Caller-driven cancellation / deadline: never retry, let ctx handling win.
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return Fatal
	}

	// Transient transport failures: dropped sockets, resets and EOFs the driver
	// has tagged with the "NetworkError" label.
	if mongo.IsNetworkError(err) {
		return Retryable
	}

	// Server-selection / socket / maxTimeMS timeouts (the caller's own context
	// deadline is excluded above and stays Fatal).
	if mongo.IsTimeout(err) {
		return Retryable
	}

	// Server command errors: inspect retryable labels first, then the known
	// not-primary / step-down / shutdown codes.
	var ce mongo.CommandError
	if errors.As(err, &ce) {
		if ce.HasErrorLabel("RetryableWriteError") || ce.HasErrorLabel("TransientTransactionError") {
			return Retryable
		}
		if isRetryableCode(int(ce.Code)) {
			return Retryable
		}
		// Recognised-fatal codes — auth (13, 18), invalid namespace (73),
		// ChangeStreamHistoryLost (286) — and every unrecognised code fall
		// through to the conservative Fatal default below.
	}

	// Conservative default: never retry the unknown.
	return Fatal
}

// isRetryableCode reports whether a MongoDB server error code denotes a
// transient condition worth retrying — chiefly the "not primary" / step-down /
// shutdown family raised while a replica set is electing a new primary, plus
// the low-level socket/host codes and the write-concern timeout. These are safe
// to retry because the operations we wrap (idempotent upserts/deletes keyed by
// _id, index creation and connect) can be re-issued without corrupting data.
func isRetryableCode(code int) bool {
	switch code {
	case 10107, // NotWritablePrimary: wrote to a node that is no longer primary
		13435, // NotPrimaryNoSecondaryOk: read/write rejected by a secondary
		11600, // InterruptedAtShutdown: node is shutting down
		11602, // InterruptedDueToReplStateChange: replset state changed mid-op
		13436, // NotPrimaryOrSecondary: node not in a serving state (recovering)
		189,   // PrimarySteppedDown: primary stepped down during the operation
		91,    // ShutdownInProgress: server is shutting down
		9001,  // SocketException: low-level socket failure
		7,     // HostNotFound: could not resolve/reach a replset member
		6,     // HostUnreachable: transient connectivity loss to a member
		89,    // NetworkTimeout: network operation timed out
		64:    // WriteConcernFailed: write concern not satisfied (e.g. w timeout)
		return true
	default:
		return false
	}
}
