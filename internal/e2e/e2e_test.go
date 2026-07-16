//go:build integration

// Package e2e drives the whole tool against two real MongoDB replica sets. Run
// with: MM_SRC_URI=... MM_DST_URI=... go test -tags=integration ./internal/e2e/...
//
// The two endpoints must be distinct replica sets (change streams require one).
// The test seeds the source itself, so any empty pair of clusters works. See
// scratchpad/mongo-testbed.sh for a ready-made pair on :27018/:27019.
package e2e

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/weefer/mongomigrate/internal/cdc"
	"github.com/weefer/mongomigrate/internal/copier"
	"github.com/weefer/mongomigrate/internal/engine"
	"github.com/weefer/mongomigrate/internal/logx"
	"github.com/weefer/mongomigrate/internal/migrate"
	"github.com/weefer/mongomigrate/internal/mongox"
	"github.com/weefer/mongomigrate/internal/namespace"
	"github.com/weefer/mongomigrate/internal/progress"
	"github.com/weefer/mongomigrate/internal/verify"
)

func uris(t *testing.T) (string, string) {
	src, dst := os.Getenv("MM_SRC_URI"), os.Getenv("MM_DST_URI")
	if src == "" || dst == "" {
		t.Skip("set MM_SRC_URI and MM_DST_URI to run e2e tests")
	}
	return src, dst
}

func connect(t *testing.T, ctx context.Context, uri string) *mongox.Client {
	t.Helper()
	c, err := mongox.Connect(ctx, uri, mongox.ConnectOptions{})
	if err != nil {
		t.Fatalf("connect %s: %v", uri, err)
	}
	t.Cleanup(func() { _ = c.Close(context.Background()) })
	return c
}

// seed loads a deterministic dataset into db "e2e" on src and drops it on dst.
func seed(t *testing.T, ctx context.Context, src, dst *mongox.Client) []namespace.Namespace {
	t.Helper()
	for _, c := range []*mongox.Client{src, dst} {
		if err := c.Database("e2e").Drop(ctx); err != nil {
			t.Fatalf("drop: %v", err)
		}
	}
	orders := src.Database("e2e").Collection("orders")
	docs := make([]any, 0, 2000)
	for i := 0; i < 2000; i++ {
		docs = append(docs, bson.D{{Key: "_id", Value: i}, {Key: "sku", Value: fmt.Sprintf("s-%d", i%40)}, {Key: "qty", Value: i % 9}})
	}
	if _, err := orders.InsertMany(ctx, docs); err != nil {
		t.Fatalf("seed insert: %v", err)
	}
	if _, err := orders.Indexes().CreateOne(ctx, mongo.IndexModel{Keys: bson.D{{Key: "sku", Value: 1}}}); err != nil {
		t.Fatalf("seed index: %v", err)
	}
	return []namespace.Namespace{{DB: "e2e", Collection: "orders"}}
}

func newEngine(src, dst *mongox.Client) *migrate.Engine {
	log := logx.Discard()
	rep := progress.NewLogReporter(log)
	return migrate.New(
		copier.New(src, dst, rep, log),
		cdc.New(src, dst, rep, log),
		verify.New(src, dst, log),
		rep, log,
	)
}

func TestSnapshotAndVerify(t *testing.T) {
	srcURI, dstURI := uris(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	src := connect(t, ctx, srcURI)
	dst := connect(t, ctx, dstURI)
	nss := seed(t, ctx, src, dst)

	eng := newEngine(src, dst)
	rep, err := eng.Run(ctx, migrate.Options{
		Live:      false,
		Copy:      engine.CopyOptions{Namespaces: nss, Workers: 4, BatchSize: 500, DropTarget: true, CopyIndexes: true},
		Verify:    engine.VerifyOptions{Namespaces: nss, CheckCounts: true, CheckIndexes: true, SampleSize: 100, Workers: 4},
		RunVerify: true,
	})
	if err != nil {
		t.Fatalf("run: %v", err)
	}
	if rep.Copy.TotalDocs != 2000 {
		t.Errorf("copied %d docs, want 2000", rep.Copy.TotalDocs)
	}
	if !rep.Verify.Ok {
		t.Errorf("verify failed: %+v", rep.Verify)
	}
}

func TestLiveCDC(t *testing.T) {
	srcURI, dstURI := uris(t)
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	src := connect(t, ctx, srcURI)
	dst := connect(t, ctx, dstURI)
	if !src.Info.SupportsChangeStreams {
		t.Skip("source does not support change streams")
	}
	nss := seed(t, ctx, src, dst)

	log := logx.Discard()
	rep := progress.NewLogReporter(log)
	tl := cdc.New(src, dst, rep, log)

	// Snapshot first so both sides start equal.
	cp := copier.New(src, dst, rep, log)
	if _, err := cp.Copy(ctx, engine.CopyOptions{Namespaces: nss, Workers: 4, BatchSize: 500, DropTarget: true, CopyIndexes: true}); err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	// Open the stream, then generate a delta, then drain.
	if err := tl.Open(ctx, engine.TailOptions{Namespaces: nss, BufferSize: 10000, ApplyBatchSize: 200}); err != nil {
		t.Fatalf("open: %v", err)
	}
	applyDone := make(chan error, 1)
	go func() { applyDone <- tl.Apply(ctx) }()

	orders := src.Database("e2e").Collection("orders")
	for i := 5000; i < 5300; i++ {
		if _, err := orders.InsertOne(ctx, bson.D{{Key: "_id", Value: i}, {Key: "sku", Value: "live"}, {Key: "qty", Value: 1}}); err != nil {
			t.Fatalf("live insert: %v", err)
		}
	}
	if _, err := orders.DeleteOne(ctx, bson.D{{Key: "_id", Value: 0}}); err != nil {
		t.Fatalf("live delete: %v", err)
	}

	// Wait for the tailer to drain the 301 events.
	deadline := time.Now().Add(30 * time.Second)
	for {
		s := tl.Stats()
		if s.Applied >= 301 && s.Buffered == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("cdc did not catch up: %+v", s)
		}
		time.Sleep(200 * time.Millisecond)
	}
	tl.Stop()
	if err := <-applyDone; err != nil {
		t.Fatalf("apply: %v", err)
	}
	_ = tl.Close(ctx)

	// Verify equality after the delta.
	vf := verify.New(src, dst, log)
	vr, err := vf.Verify(ctx, engine.VerifyOptions{Namespaces: nss, CheckCounts: true, CheckIndexes: true, SampleSize: 200, Workers: 4})
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if !vr.Ok {
		t.Errorf("post-CDC verify failed: %+v", vr.Collections)
	}
	sc, _ := src.Database("e2e").Collection("orders").CountDocuments(ctx, bson.D{})
	if sc != 2299 { // 2000 + 300 inserts - 1 delete
		t.Errorf("source count = %d, want 2299", sc)
	}
}
