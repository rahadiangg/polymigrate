//go:build integration

// Package cdc integration test. Run with a live replica-set/sharded MongoDB:
//
//	MONGO_SRC_URI=mongodb://localhost:27017/?replicaSet=rs0 \
//	MONGO_DST_URI=mongodb://localhost:27018/?replicaSet=rs1 \
//	go test -tags integration ./internal/cdc/... -run TestIntegration -v
//
// The source must support change streams (a replica set or sharded cluster).
package cdc

import (
	"context"
	"os"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"

	"github.com/weefer/mongomigrate/internal/engine"
	"github.com/weefer/mongomigrate/internal/mongox"
	"github.com/weefer/mongomigrate/internal/namespace"
)

func TestIntegration_TailInsertUpdateDelete(t *testing.T) {
	srcURI := os.Getenv("MONGO_SRC_URI")
	dstURI := os.Getenv("MONGO_DST_URI")
	if srcURI == "" || dstURI == "" {
		t.Skip("set MONGO_SRC_URI and MONGO_DST_URI to run the CDC integration test")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	src, err := mongox.Connect(ctx, srcURI, mongox.ConnectOptions{AppName: "cdc-it-src"})
	if err != nil {
		t.Fatalf("connect src: %v", err)
	}
	defer src.Close(context.Background())
	dst, err := mongox.Connect(ctx, dstURI, mongox.ConnectOptions{AppName: "cdc-it-dst"})
	if err != nil {
		t.Fatalf("connect dst: %v", err)
	}
	defer dst.Close(context.Background())

	if !src.Info.SupportsChangeStreams {
		t.Skipf("source does not support change streams: %s", src.Info)
	}

	ns := namespace.Namespace{DB: "cdc_it", Collection: "docs"}
	srcColl := src.Client.Database(ns.DB).Collection(ns.Collection)
	dstColl := dst.Client.Database(ns.DB).Collection(ns.Collection)
	_ = srcColl.Drop(ctx)
	_ = dstColl.Drop(ctx)

	tl := New(src, dst, engine.NopReporter{}, nil)
	if err := tl.Open(ctx, engine.TailOptions{Namespaces: []namespace.Namespace{ns}, ApplyBatchSize: 50}); err != nil {
		t.Fatalf("Open: %v", err)
	}
	applyDone := make(chan error, 1)
	go func() { applyDone <- tl.Apply(ctx) }()

	// Generate a workload on the source.
	if _, err := srcColl.InsertOne(ctx, bson.D{{Key: "_id", Value: 1}, {Key: "v", Value: "a"}}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if _, err := srcColl.InsertOne(ctx, bson.D{{Key: "_id", Value: 2}, {Key: "v", Value: "b"}}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if _, err := srcColl.UpdateByID(ctx, 1, bson.D{{Key: "$set", Value: bson.D{{Key: "v", Value: "a2"}}}}); err != nil {
		t.Fatalf("update: %v", err)
	}
	if _, err := srcColl.DeleteOne(ctx, bson.D{{Key: "_id", Value: 2}}); err != nil {
		t.Fatalf("delete: %v", err)
	}

	// Wait for the destination to converge: doc 1 present with v="a2", doc 2 gone.
	deadline := time.Now().Add(20 * time.Second)
	for {
		n, _ := dstColl.CountDocuments(ctx, bson.D{})
		var got bson.M
		_ = dstColl.FindOne(ctx, bson.D{{Key: "_id", Value: 1}}).Decode(&got)
		if n == 1 && got["v"] == "a2" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("destination did not converge: count=%d doc1=%v", n, got)
		}
		time.Sleep(100 * time.Millisecond)
	}

	tl.Stop()
	if err := <-applyDone; err != nil {
		t.Fatalf("Apply: %v", err)
	}
	if err := tl.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if tl.Token().IsZero() {
		t.Error("expected a non-zero resume token after tailing")
	}
}
