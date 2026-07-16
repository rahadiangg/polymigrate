// Package checkpoint persists migration progress so a run can resume after the
// source, destination, or the mm process itself dies. A checkpoint records the
// per-namespace snapshot progress and the last APPLIED change-stream resume
// token; on resume the copier skips completed collections and continues an
// in-progress one from its last _id, and the tailer resumes the change stream
// from the saved token.
//
// The store is the source of truth for progress. It is written atomically and
// frequently enough that a crash loses at most one batch of idempotent work.
package checkpoint

import (
	"context"
	"sync"
	"time"
)

// NSProgress is the snapshot-copy progress for a single namespace.
type NSProgress struct {
	// Done is true once the namespace has been fully copied.
	Done bool `json:"done" bson:"done"`
	// LastID is the raw BSON of the last _id copied, used as the resume boundary
	// ({_id: {$gt: LastID}}). Empty means "not started".
	LastID []byte `json:"lastId,omitempty" bson:"lastId,omitempty"`
	// DocsCopied is the running count of documents copied for this namespace.
	DocsCopied int64 `json:"docsCopied" bson:"docsCopied"`
}

// State is the full persisted migration state for one run.
type State struct {
	// RunID identifies the migration run; a resumed run loads the State with the
	// same RunID.
	RunID string `json:"runId" bson:"_id"`
	// Mode is "snapshot" or "live".
	Mode string `json:"mode" bson:"mode"`
	// ConfigHash guards against resuming a run whose configuration (endpoints,
	// filter) has changed underneath it; a mismatch must be treated as an error.
	ConfigHash string `json:"configHash" bson:"configHash"`
	// Phase is the last-entered engine.Phase, for reporting.
	Phase string `json:"phase" bson:"phase"`
	// Namespaces holds per-namespace snapshot progress, keyed by ns.String().
	Namespaces map[string]NSProgress `json:"namespaces" bson:"namespaces"`
	// ResumeToken is the last APPLIED change-stream token (raw BSON). It must only
	// ever be advanced after the corresponding events are applied to the
	// destination — see engine.TailOptions.OnToken.
	ResumeToken []byte `json:"resumeToken,omitempty" bson:"resumeToken,omitempty"`
	// NonResumable is set when the run can no longer be resumed (e.g. the change
	// stream history was lost); a resumed run must then start fresh.
	NonResumable bool `json:"nonResumable,omitempty" bson:"nonResumable,omitempty"`
	// UpdatedAt is the wall-clock time of the last save.
	UpdatedAt time.Time `json:"updatedAt" bson:"updatedAt"`
}

// Clone returns a deep copy so callers can mutate without racing a store that
// retains a reference.
func (s State) Clone() State {
	out := s
	if s.Namespaces != nil {
		out.Namespaces = make(map[string]NSProgress, len(s.Namespaces))
		for k, v := range s.Namespaces {
			if len(v.LastID) > 0 {
				id := make([]byte, len(v.LastID))
				copy(id, v.LastID)
				v.LastID = id
			}
			out.Namespaces[k] = v
		}
	}
	if len(s.ResumeToken) > 0 {
		tok := make([]byte, len(s.ResumeToken))
		copy(tok, s.ResumeToken)
		out.ResumeToken = tok
	}
	return out
}

// Store persists and loads migration State.
type Store interface {
	// Save writes the State atomically. Concurrent Saves for the same RunID must
	// not corrupt the stored value.
	Save(ctx context.Context, s State) error
	// Load returns the State for runID. found is false (with a nil error) when no
	// checkpoint exists yet.
	Load(ctx context.Context, runID string) (state State, found bool, err error)
	// Delete removes the checkpoint for runID. Deleting a missing checkpoint is
	// not an error.
	Delete(ctx context.Context, runID string) error
}

// MemStore is an in-memory Store for tests. It is safe for concurrent use.
type MemStore struct {
	mu     sync.Mutex
	states map[string]State
	// SaveHook, if set, is called on every Save (e.g. to inject a fault in tests).
	SaveHook func(State) error
}

// NewMemStore builds an empty in-memory store.
func NewMemStore() *MemStore {
	return &MemStore{states: make(map[string]State)}
}

func (m *MemStore) Save(_ context.Context, s State) error {
	if m.SaveHook != nil {
		if err := m.SaveHook(s); err != nil {
			return err
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.states[s.RunID] = s.Clone()
	return nil
}

func (m *MemStore) Load(_ context.Context, runID string) (State, bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	s, ok := m.states[runID]
	if !ok {
		return State{}, false, nil
	}
	return s.Clone(), true, nil
}

func (m *MemStore) Delete(_ context.Context, runID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.states, runID)
	return nil
}

var _ Store = (*MemStore)(nil)
