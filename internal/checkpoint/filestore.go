package checkpoint

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// ErrCorruptCheckpoint is returned (wrapped) by FileStore.Load when a checkpoint
// file exists but cannot be parsed. The caller can use errors.Is to distinguish
// a corrupt/partial checkpoint from a genuine I/O failure and choose to start a
// fresh run rather than aborting.
var ErrCorruptCheckpoint = errors.New("checkpoint: corrupt or partial checkpoint file")

// filePerm and dirPerm are the permissions used for checkpoint files and the
// containing directory.
const (
	filePerm = 0o644
	dirPerm  = 0o755
)

// maxNamePrefix bounds the readable portion of a checkpoint filename so an
// arbitrarily long runID cannot produce an unusable path.
const maxNamePrefix = 100

// FileStore persists State as atomically-written JSON files under a directory,
// one file per runID. It is safe for concurrent Save of different runIDs, and a
// Save of the same runID never leaves a corrupt file: each Save writes to a
// uniquely-named temp file in the same directory and atomically renames it over
// the destination, so a reader always observes either the old or the new
// complete file, never a partial one.
type FileStore struct {
	dir string
}

// NewFileStore builds a FileStore rooted at dir. The directory is created lazily
// on the first Save, so passing a not-yet-existing path is fine.
func NewFileStore(dir string) *FileStore {
	return &FileStore{dir: dir}
}

// Save marshals s to JSON and writes it atomically to dir/<runID>.json.
func (fst *FileStore) Save(_ context.Context, s State) error {
	if err := os.MkdirAll(fst.dir, dirPerm); err != nil {
		return fmt.Errorf("checkpoint: create dir %q: %w", fst.dir, err)
	}

	data, err := json.Marshal(s)
	if err != nil {
		return fmt.Errorf("checkpoint: marshal state %q: %w", s.RunID, err)
	}

	tmp, err := os.CreateTemp(fst.dir, "ckpt-*.tmp")
	if err != nil {
		return fmt.Errorf("checkpoint: create temp file: %w", err)
	}
	tmpName := tmp.Name()
	// Best-effort cleanup: on any error before the rename succeeds, remove the
	// temp file so it does not accumulate. After a successful rename tmpName no
	// longer exists and the Remove is a harmless no-op.
	defer func() { _ = os.Remove(tmpName) }()

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("checkpoint: write temp file: %w", err)
	}
	// fsync so a crash right after the rename cannot leave a zero-length file that
	// the directory entry points at.
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return fmt.Errorf("checkpoint: sync temp file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("checkpoint: close temp file: %w", err)
	}

	final := fst.path(s.RunID)
	if err := os.Rename(tmpName, final); err != nil {
		return fmt.Errorf("checkpoint: rename %q: %w", final, err)
	}
	return nil
}

// Load reads and unmarshals the checkpoint for runID. A missing file yields
// (State{}, false, nil). A file that exists but cannot be parsed yields an error
// wrapping ErrCorruptCheckpoint.
func (fst *FileStore) Load(_ context.Context, runID string) (State, bool, error) {
	path := fst.path(runID)
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return State{}, false, nil
		}
		return State{}, false, fmt.Errorf("checkpoint: read %q: %w", path, err)
	}

	var s State
	if err := json.Unmarshal(data, &s); err != nil {
		return State{}, false, errors.Join(
			fmt.Errorf("checkpoint: parse %q: %w", path, ErrCorruptCheckpoint), err)
	}
	return s, true, nil
}

// Delete removes the checkpoint file for runID. A missing file is not an error.
func (fst *FileStore) Delete(_ context.Context, runID string) error {
	if err := os.Remove(fst.path(runID)); err != nil && !errors.Is(err, fs.ErrNotExist) {
		return fmt.Errorf("checkpoint: delete %q: %w", fst.path(runID), err)
	}
	return nil
}

// path returns the absolute file path for a runID's checkpoint.
func (fst *FileStore) path(runID string) string {
	return filepath.Join(fst.dir, safeFileName(runID))
}

// safeFileName maps an arbitrary runID to a filesystem-safe filename. It keeps a
// readable, sanitized prefix of the runID and appends a hash of the original so
// that distinct runIDs never collide even if they sanitize to the same prefix
// (and so that runIDs like "", ".", ".." or ones containing path separators
// cannot produce a dangerous name).
func safeFileName(runID string) string {
	var b strings.Builder
	for _, r := range runID {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
		if b.Len() >= maxNamePrefix {
			break
		}
	}
	sum := sha256.Sum256([]byte(runID))
	prefix := b.String()
	if prefix != "" {
		prefix += "-"
	}
	return prefix + hex.EncodeToString(sum[:8]) + ".json"
}

var _ Store = (*FileStore)(nil)
