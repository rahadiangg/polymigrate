package retry

import (
	"context"
	"errors"
	"fmt"
	"net"
	"testing"

	"go.mongodb.org/mongo-driver/v2/mongo"
)

// fakeNetErr is a synthetic net.Error. mongo.IsTimeout inspects the error chain
// for a net.Error and returns its Timeout(), which lets us exercise the timeout
// branch of MongoClassifier without a live socket.
type fakeNetErr struct{ timeout bool }

func (e fakeNetErr) Error() string   { return "synthetic net error" }
func (e fakeNetErr) Timeout() bool   { return e.timeout }
func (e fakeNetErr) Temporary() bool { return false }

// fakeNetErr must satisfy net.Error for mongo.IsTimeout to inspect its Timeout().
var _ net.Error = fakeNetErr{}

// cmdErr builds a CommandError with the given code and labels.
func cmdErr(code int32, labels ...string) mongo.CommandError {
	return mongo.CommandError{Code: code, Message: "synthetic", Labels: labels}
}

func TestMongoClassifier(t *testing.T) {
	// netErr carries the "NetworkError" label so mongo.IsNetworkError matches it;
	// CommandError satisfies the driver's LabeledError interface used internally.
	netLabelled := cmdErr(0, "NetworkError")

	tests := []struct {
		name string
		err  error
		want Class
	}{
		// Defensive and context cases.
		{"nil defensive", nil, Fatal},
		{"context canceled", context.Canceled, Fatal},
		{"context deadline exceeded", context.DeadlineExceeded, Fatal},
		{"wrapped context canceled", fmt.Errorf("op: %w", context.Canceled), Fatal},
		{"wrapped deadline exceeded", fmt.Errorf("op: %w", context.DeadlineExceeded), Fatal},

		// Transport / timeout branches.
		{"network label", netLabelled, Retryable},
		{"wrapped network label", fmt.Errorf("write: %w", netLabelled), Retryable},
		{"net timeout", fakeNetErr{timeout: true}, Retryable},
		{"wrapped net timeout", fmt.Errorf("dial: %w", fakeNetErr{timeout: true}), Retryable},
		{"net non-timeout falls through to fatal", fakeNetErr{timeout: false}, Fatal},

		// Retryable command labels.
		{"retryable write label", cmdErr(1, "RetryableWriteError"), Retryable},
		{"transient txn label", cmdErr(1, "TransientTransactionError"), Retryable},

		// Retryable not-primary / step-down / shutdown / socket codes.
		{"NotWritablePrimary 10107", cmdErr(10107), Retryable},
		{"NotPrimaryNoSecondaryOk 13435", cmdErr(13435), Retryable},
		{"InterruptedAtShutdown 11600", cmdErr(11600), Retryable},
		{"InterruptedDueToReplStateChange 11602", cmdErr(11602), Retryable},
		{"NotPrimaryOrSecondary 13436", cmdErr(13436), Retryable},
		{"PrimarySteppedDown 189", cmdErr(189), Retryable},
		{"ShutdownInProgress 91", cmdErr(91), Retryable},
		{"SocketException 9001", cmdErr(9001), Retryable},
		{"HostNotFound 7", cmdErr(7), Retryable},
		{"HostUnreachable 6", cmdErr(6), Retryable},
		{"NetworkTimeout 89", cmdErr(89), Retryable},
		{"WriteConcernFailed 64", cmdErr(64), Retryable},
		{"wrapped retryable code", fmt.Errorf("bulk: %w", cmdErr(10107)), Retryable},

		// Fatal command codes.
		{"Unauthorized 13", cmdErr(13), Fatal},
		{"AuthenticationFailed 18", cmdErr(18), Fatal},
		{"InvalidNamespace 73", cmdErr(73), Fatal},
		{"ChangeStreamHistoryLost 286", cmdErr(286), Fatal},
		{"unknown command code", cmdErr(12345), Fatal},

		// Unknown non-command errors.
		{"plain error", errors.New("boom"), Fatal},
		{"wrapped plain error", fmt.Errorf("ctx: %w", errors.New("boom")), Fatal},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := MongoClassifier(tt.err); got != tt.want {
				t.Fatalf("MongoClassifier(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

func TestIsRetryableCode(t *testing.T) {
	retryable := []int{10107, 13435, 11600, 11602, 13436, 189, 91, 9001, 7, 6, 89, 64}
	for _, c := range retryable {
		if !isRetryableCode(c) {
			t.Errorf("isRetryableCode(%d) = false, want true", c)
		}
	}
	fatal := []int{0, 13, 18, 73, 286, 1, 12345, -1}
	for _, c := range fatal {
		if isRetryableCode(c) {
			t.Errorf("isRetryableCode(%d) = true, want false", c)
		}
	}
}
