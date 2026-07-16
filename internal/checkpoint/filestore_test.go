package checkpoint

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"sync"
	"testing"
)

func TestFileStore_SaveLoadRoundTrip(t *testing.T) {
	dir := t.TempDir()
	fs := NewFileStore(dir)
	ctx := context.Background()

	want := sampleState("run-roundtrip")
	if err := fs.Save(ctx, want); err != nil {
		t.Fatalf("Save: %v", err)
	}

	got, found, err := fs.Load(ctx, want.RunID)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if !found {
		t.Fatal("Load: found = false after Save")
	}
	assertStateEqual(t, want, got)
}

func TestFileStore_SaveCreatesDirLazily(t *testing.T) {
	// The directory does not exist yet; Save must create it.
	dir := filepath.Join(t.TempDir(), "nested", "checkpoints")
	fs := NewFileStore(dir)
	if err := fs.Save(context.Background(), sampleState("run-x")); err != nil {
		t.Fatalf("Save into missing dir: %v", err)
	}
	if _, err := os.Stat(dir); err != nil {
		t.Fatalf("dir not created: %v", err)
	}
}

func TestFileStore_LoadMissing(t *testing.T) {
	fs := NewFileStore(t.TempDir())
	got, found, err := fs.Load(context.Background(), "never-saved")
	if err != nil {
		t.Fatalf("Load missing: unexpected error %v", err)
	}
	if found {
		t.Error("Load missing: found = true")
	}
	if !reflect.DeepEqual(got, State{}) {
		t.Errorf("Load missing: state = %+v, want zero", got)
	}
}

func TestFileStore_LoadCorrupt(t *testing.T) {
	dir := t.TempDir()
	fs := NewFileStore(dir)
	runID := "run-corrupt"

	// Write a garbage file where the checkpoint should be.
	if err := os.WriteFile(fs.path(runID), []byte("{ this is not json"), filePerm); err != nil {
		t.Fatalf("seed corrupt file: %v", err)
	}

	_, found, err := fs.Load(context.Background(), runID)
	if found {
		t.Error("Load corrupt: found = true")
	}
	if !errors.Is(err, ErrCorruptCheckpoint) {
		t.Fatalf("Load corrupt: err = %v, want wrapping ErrCorruptCheckpoint", err)
	}
}

func TestFileStore_LoadEmptyFileIsCorrupt(t *testing.T) {
	dir := t.TempDir()
	fs := NewFileStore(dir)
	runID := "run-empty"
	if err := os.WriteFile(fs.path(runID), nil, filePerm); err != nil {
		t.Fatalf("seed empty file: %v", err)
	}
	_, found, err := fs.Load(context.Background(), runID)
	if found || !errors.Is(err, ErrCorruptCheckpoint) {
		t.Fatalf("empty file: found=%v err=%v, want corrupt", found, err)
	}
}

func TestFileStore_Delete(t *testing.T) {
	dir := t.TempDir()
	fs := NewFileStore(dir)
	ctx := context.Background()

	// Deleting a missing checkpoint is a no-op.
	if err := fs.Delete(ctx, "missing"); err != nil {
		t.Fatalf("Delete missing: %v", err)
	}

	if err := fs.Save(ctx, sampleState("run-del")); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := fs.Delete(ctx, "run-del"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	_, found, err := fs.Load(ctx, "run-del")
	if err != nil || found {
		t.Fatalf("after Delete: found=%v err=%v, want not found", found, err)
	}
}

func TestFileStore_OverwriteSameRunID(t *testing.T) {
	dir := t.TempDir()
	fs := NewFileStore(dir)
	ctx := context.Background()

	first := sampleState("run-ow")
	first.Phase = "snapshot"
	if err := fs.Save(ctx, first); err != nil {
		t.Fatalf("Save first: %v", err)
	}
	second := sampleState("run-ow")
	second.Phase = "apply"
	second.Namespaces["shop.orders"] = NSProgress{Done: true, DocsCopied: 100}
	if err := fs.Save(ctx, second); err != nil {
		t.Fatalf("Save second: %v", err)
	}

	got, _, err := fs.Load(ctx, "run-ow")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if got.Phase != "apply" || got.Namespaces["shop.orders"].DocsCopied != 100 {
		t.Errorf("overwrite not reflected: %+v", got)
	}

	// Overwriting must not leave stray temp files behind.
	assertNoTempFiles(t, dir)
}

func TestFileStore_ConcurrentSaveDifferentRunIDs(t *testing.T) {
	dir := t.TempDir()
	fs := NewFileStore(dir)
	ctx := context.Background()

	const n = 30
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s := sampleState("run-" + strconv.Itoa(i))
			s.Namespaces = map[string]NSProgress{"db.c": {DocsCopied: int64(i)}}
			if err := fs.Save(ctx, s); err != nil {
				t.Errorf("Save run-%d: %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	for i := 0; i < n; i++ {
		got, found, err := fs.Load(ctx, "run-"+strconv.Itoa(i))
		if err != nil || !found {
			t.Fatalf("Load run-%d: found=%v err=%v", i, found, err)
		}
		if got.Namespaces["db.c"].DocsCopied != int64(i) {
			t.Errorf("run-%d: DocsCopied=%d, want %d", i, got.Namespaces["db.c"].DocsCopied, i)
		}
	}
	assertNoTempFiles(t, dir)
}

func TestFileStore_ConcurrentSaveSameRunIDNeverCorrupt(t *testing.T) {
	dir := t.TempDir()
	fs := NewFileStore(dir)
	ctx := context.Background()

	const n = 40
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			s := sampleState("run-shared")
			s.Namespaces = map[string]NSProgress{"db.c": {DocsCopied: int64(i)}}
			if err := fs.Save(ctx, s); err != nil {
				t.Errorf("Save %d: %v", i, err)
			}
		}(i)
	}
	wg.Wait()

	// Whichever writer won the last rename, the file must be a complete, parseable
	// checkpoint — never a partial/corrupt one.
	got, found, err := fs.Load(ctx, "run-shared")
	if err != nil {
		t.Fatalf("Load after concurrent same-runID saves: %v", err)
	}
	if !found {
		t.Fatal("not found after concurrent saves")
	}
	if got.RunID != "run-shared" {
		t.Errorf("RunID = %q, want run-shared", got.RunID)
	}
	assertNoTempFiles(t, dir)
}

func TestSafeFileName(t *testing.T) {
	tests := []struct {
		name  string
		runID string
	}{
		{"simple", "run-123"},
		{"empty", ""},
		{"dot", "."},
		{"dotdot", ".."},
		{"path-separators", "../../etc/passwd"},
		{"spaces-and-symbols", "a b/c:d*e"},
		{"unicode", "café-日本"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := safeFileName(tt.runID)
			// Must be a single path element (no separators) and not "." / "..".
			if filepath.Base(got) != got {
				t.Errorf("safeFileName(%q) = %q contains a path separator", tt.runID, got)
			}
			if got == "." || got == ".." {
				t.Errorf("safeFileName(%q) = %q is a dot name", tt.runID, got)
			}
		})
	}

	// Distinct runIDs that sanitize to the same readable prefix must not collide,
	// because the hash suffix differs.
	if safeFileName("a/b") == safeFileName("a b") {
		t.Error("distinct runIDs collided to the same filename")
	}
}

// assertNoTempFiles fails if any leftover temp file remains in dir.
func assertNoTempFiles(t *testing.T, dir string) {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("ReadDir: %v", err)
	}
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".tmp" {
			t.Errorf("leftover temp file: %s", e.Name())
		}
	}
}
