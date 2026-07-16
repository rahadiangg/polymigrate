//go:build integration

// These integration tests run against real MongoDB deployments and are a bonus
// on top of the unit tests. Enable with: go test -tags integration ./internal/verify/...
// Provide connection strings via MONGO_SRC_URI and MONGO_DST_URI; the tests are
// skipped when either is unset. The tests write into a throwaway database and
// drop it on completion.
package verify

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

func integrationClients(t *testing.T) (*mongox.Client, *mongox.Client) {
	t.Helper()
	srcURI := os.Getenv("MONGO_SRC_URI")
	dstURI := os.Getenv("MONGO_DST_URI")
	if srcURI == "" || dstURI == "" {
		t.Skip("set MONGO_SRC_URI and MONGO_DST_URI to run integration tests")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	src, err := mongox.Connect(ctx, srcURI, mongox.ConnectOptions{})
	if err != nil {
		t.Fatalf("connect src: %v", err)
	}
	dst, err := mongox.Connect(ctx, dstURI, mongox.ConnectOptions{})
	if err != nil {
		t.Fatalf("connect dst: %v", err)
	}
	return src, dst
}

func TestIntegration_VerifyMatching(t *testing.T) {
	src, dst := integrationClients(t)
	ctx := context.Background()
	defer src.Close(ctx)
	defer dst.Close(ctx)

	ns := namespace.Namespace{DB: "mongomigrate_verify_it", Collection: "docs"}
	seed := func(c *mongox.Client) {
		coll := c.Database(ns.DB).Collection(ns.Collection)
		_ = c.Database(ns.DB).Drop(ctx)
		docs := make([]any, 0, 100)
		for i := 0; i < 100; i++ {
			docs = append(docs, bson.D{{Key: "_id", Value: i}, {Key: "v", Value: i * 2}})
		}
		if _, err := coll.InsertMany(ctx, docs); err != nil {
			t.Fatalf("seed: %v", err)
		}
	}
	seed(src)
	seed(dst)
	defer src.Database(ns.DB).Drop(ctx)
	defer dst.Database(ns.DB).Drop(ctx)

	v := New(src, dst, nil)
	report, err := v.Verify(ctx, engine.VerifyOptions{
		Namespaces:   []namespace.Namespace{ns},
		CheckCounts:  true,
		CheckIndexes: true,
		SampleSize:   50,
		Workers:      2,
	})
	if err != nil {
		t.Fatalf("Verify: %v", err)
	}
	if !report.Ok {
		t.Fatalf("expected Ok, got %+v", report.Collections)
	}
}
