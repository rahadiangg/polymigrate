//go:build integration

// Package checkpoint integration test. Run against a live MongoDB (any topology,
// a standalone is fine for the checkpoint store):
//
//	MONGO_DST_URI=mongodb://localhost:27018 \
//	go test -tags integration ./internal/checkpoint/... -run TestIntegration -v
package checkpoint

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/weefer/mongomigrate/internal/mongox"
)

func TestIntegration_MongoStoreRoundTrip(t *testing.T) {
	uri := os.Getenv("MONGO_DST_URI")
	if uri == "" {
		t.Skip("set MONGO_DST_URI to run the checkpoint MongoStore integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	cl, err := mongox.Connect(ctx, uri, mongox.ConnectOptions{AppName: "ckpt-it"})
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer cl.Close(context.Background())

	// Use a throwaway db/coll so the test is self-contained.
	store := NewMongoStore(cl, "_mongomigrate_it", "checkpoints")
	runID := "it-run-1"
	t.Cleanup(func() { _ = store.Delete(context.Background(), runID) })

	// Missing checkpoint.
	if _, found, err := store.Load(ctx, runID); err != nil || found {
		t.Fatalf("Load missing: found=%v err=%v", found, err)
	}

	want := sampleState(runID)
	if err := store.Save(ctx, want); err != nil {
		t.Fatalf("Save: %v", err)
	}
	// Overwrite (upsert) with an advanced state to confirm ReplaceOne semantics.
	want.Phase = "apply"
	want.Namespaces["shop.orders"] = NSProgress{Done: true, DocsCopied: 500}
	if err := store.Save(ctx, want); err != nil {
		t.Fatalf("Save overwrite: %v", err)
	}

	got, found, err := store.Load(ctx, runID)
	if err != nil || !found {
		t.Fatalf("Load: found=%v err=%v", found, err)
	}
	assertStateEqual(t, want, got)

	if err := store.Delete(ctx, runID); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, found, _ := store.Load(ctx, runID); found {
		t.Error("checkpoint still present after Delete")
	}
}
