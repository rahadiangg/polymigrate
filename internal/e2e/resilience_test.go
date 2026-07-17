//go:build integration

// Fault-injection integration tests for the resilience tier. They drive the real
// testbed replica sets (source :27018, dest :27019) and prove:
//   - a snapshot interrupted mid-flight resumes without redoing completed work;
//   - a transient destination outage is ridden out by retry/backoff;
//   - a CDC stream resumes from its checkpointed token after a simulated crash.
//
// Run: POLY_SRC_URI=... POLY_DST_URI=... go test -tags=integration ./internal/e2e/...
package e2e

import (
	"context"
	"fmt"
	"os/exec"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"

	"github.com/rahadiangg/polymigrate/internal/cdc"
	"github.com/rahadiangg/polymigrate/internal/checkpoint"
	"github.com/rahadiangg/polymigrate/internal/copier"
	"github.com/rahadiangg/polymigrate/internal/engine"
	"github.com/rahadiangg/polymigrate/internal/logx"
	"github.com/rahadiangg/polymigrate/internal/migrate"
	"github.com/rahadiangg/polymigrate/internal/mongox"
	"github.com/rahadiangg/polymigrate/internal/namespace"
	"github.com/rahadiangg/polymigrate/internal/progress"
	"github.com/rahadiangg/polymigrate/internal/verify"
)

func docker(t *testing.T, args ...string) {
	t.Helper()
	out, err := exec.Command("docker", args...).CombinedOutput()
	if err != nil {
		t.Fatalf("docker %v: %v\n%s", args, err, out)
	}
}

func waitDstPrimary(t *testing.T, uri string) *mongox.Client {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		c, err := mongox.Connect(ctx, uri, mongox.ConnectOptions{})
		cancel()
		if err == nil && c.Info.IsWritablePrimary {
			return c
		}
		if c != nil {
			_ = c.Close(context.Background())
		}
		if time.Now().After(deadline) {
			t.Fatalf("dst did not become primary: %v", err)
		}
		time.Sleep(time.Second)
	}
}

// seedMulti loads three collections into db "res" on src and drops it on dst.
func seedMulti(t *testing.T, ctx context.Context, src, dst *mongox.Client) []namespace.Namespace {
	t.Helper()
	for _, c := range []*mongox.Client{src, dst} {
		if err := c.Database("res").Drop(ctx); err != nil {
			t.Fatalf("drop: %v", err)
		}
	}
	for _, spec := range []struct {
		coll string
		n    int
	}{{"aaa", 1500}, {"bbb", 1500}, {"ccc", 1500}} {
		docs := make([]any, spec.n)
		for i := range docs {
			docs[i] = bson.D{{Key: "_id", Value: fmt.Sprintf("%s-%05d", spec.coll, i)}, {Key: "v", Value: i}}
		}
		if _, err := src.Database("res").Collection(spec.coll).InsertMany(ctx, docs); err != nil {
			t.Fatalf("seed %s: %v", spec.coll, err)
		}
	}
	return []namespace.Namespace{
		{DB: "res", Collection: "aaa"}, {DB: "res", Collection: "bbb"}, {DB: "res", Collection: "ccc"},
	}
}

func newEng(src, dst *mongox.Client) *migrate.Engine {
	log := logx.Discard()
	rep := progress.NewLogReporter(log)
	return migrate.New(copier.New(src, dst, rep, log), cdc.New(src, dst, rep, log), verify.New(src, dst, log), rep, log)
}

// cancelOnFirstDone cancels the run the moment the first collection is marked
// complete, simulating an interruption (SIGINT / crash) partway through the
// snapshot while guaranteeing at least one collection is already checkpointed.
type cancelOnFirstDone struct {
	checkpoint.Store
	cancel context.CancelFunc
	fired  bool
}

func (c *cancelOnFirstDone) Save(ctx context.Context, s checkpoint.State) error {
	err := c.Store.Save(ctx, s)
	if !c.fired {
		for _, p := range s.Namespaces {
			if p.Done {
				c.fired = true
				c.cancel()
				break
			}
		}
	}
	return err
}

// Acceptance criterion 1 + 6: an interrupted snapshot resumes, skipping the
// collection already completed, and the destination ends identical to the source.
func TestResumeAfterInterruptedSnapshot(t *testing.T) {
	srcURI, dstURI := uris(t)
	bg := context.Background()
	src := connect(t, bg, srcURI)
	dst := connect(t, bg, dstURI)
	nss := seedMulti(t, bg, src, dst)

	dir := t.TempDir()
	const runID = "res-A"
	file := checkpoint.NewFileStore(dir)

	// Run 1: interrupt after the first collection completes.
	ctx, cancel := context.WithCancel(bg)
	hooked := &cancelOnFirstDone{Store: file, cancel: cancel}
	_, err := newEng(src, dst).Run(ctx, migrate.Options{
		Copy:       engine.CopyOptions{Namespaces: nss, Workers: 1, BatchSize: 300, DropTarget: true, CopyIndexes: true},
		Checkpoint: hooked, RunID: runID, ConfigHash: "h",
	})
	if err == nil {
		t.Fatal("expected the interrupted run to return an error")
	}

	// The checkpoint must show at least one completed collection.
	st, found, _ := file.Load(bg, runID)
	if !found {
		t.Fatal("no checkpoint written")
	}
	if countDoneT(st) == 0 {
		t.Fatal("expected at least one collection marked done in the checkpoint")
	}

	// Run 2: resume. Completed collections must be skipped; verify must pass.
	rep, err := newEng(src, dst).Run(bg, migrate.Options{
		Copy:       engine.CopyOptions{Namespaces: nss, Workers: 3, BatchSize: 300, CopyIndexes: true},
		Verify:     engine.VerifyOptions{Namespaces: nss, CheckCounts: true, CheckIndexes: true, SampleSize: 100, Workers: 3},
		RunVerify:  true,
		Checkpoint: file, RunID: runID, ConfigHash: "h", Resume: true,
	})
	if err != nil {
		t.Fatalf("resume run: %v", err)
	}
	skipped := 0
	for _, c := range rep.Copy.Collections {
		if c.Skipped {
			skipped++
		}
	}
	if skipped == 0 {
		t.Error("resume did not skip any already-completed collection")
	}
	if !rep.Verify.Ok {
		t.Errorf("post-resume verify failed: %+v", rep.Verify.Collections)
	}
}

// Acceptance criterion 3: a transient destination outage (pause) is absorbed by
// retry/backoff so the snapshot still completes and verifies.
func TestTransientOutageRetry(t *testing.T) {
	srcURI, dstURI := uris(t)
	bg := context.Background()
	src := connect(t, bg, srcURI)
	dst := connect(t, bg, dstURI)
	nss := seedMulti(t, bg, src, dst) // also drops res on dst, so it starts empty

	// Pause the destination BEFORE the copy runs, so every write in the first
	// window hits an unresponsive server; unpause after several seconds. Retry/
	// backoff (or the driver riding out the stall) must let the copy complete.
	// DropTarget is off because the destination is already empty and drop is not
	// on the retry path.
	docker(t, "pause", "poly-dst")
	unpaused := make(chan struct{})
	go func() {
		time.Sleep(4 * time.Second)
		docker(t, "unpause", "poly-dst")
		close(unpaused)
	}()
	t.Cleanup(func() { _ = exec.Command("docker", "unpause", "poly-dst").Run() })

	rep, err := newEng(src, dst).Run(bg, migrate.Options{
		Copy:      engine.CopyOptions{Namespaces: nss, Workers: 2, BatchSize: 200, DropTarget: false, CopyIndexes: true},
		Verify:    engine.VerifyOptions{Namespaces: nss, CheckCounts: true, SampleSize: 100, Workers: 3},
		RunVerify: true,
	})
	<-unpaused
	if err != nil {
		t.Fatalf("run through transient outage: %v", err)
	}
	if !rep.Verify.Ok {
		t.Errorf("verify failed after transient outage: %+v", rep.Verify.Collections)
	}
}

// Acceptance criterion 2 + 6: a CDC stream resumes from its checkpointed token
// after a simulated crash, applying events produced while it was down, with no
// data loss.
func TestCDCResumeFromToken(t *testing.T) {
	srcURI, dstURI := uris(t)
	bg := context.Background()
	src := connect(t, bg, srcURI)
	dst := connect(t, bg, dstURI)
	nss := seedMulti(t, bg, src, dst)

	log := logx.Discard()
	rep := progress.NewLogReporter(log)

	// Snapshot so both sides start equal.
	if _, err := copier.New(src, dst, rep, log).Copy(bg, engine.CopyOptions{Namespaces: nss, Workers: 3, BatchSize: 300, DropTarget: true}); err != nil {
		t.Fatalf("snapshot: %v", err)
	}

	dir := t.TempDir()
	store := checkpoint.NewFileStore(dir)
	const runID = "res-C"

	// Tailer #1: capture a checkpoint token after applying a first wave of writes.
	var lastToken []byte
	tl1 := cdc.New(src, dst, rep, log)
	if err := tl1.Open(bg, engine.TailOptions{Namespaces: nss, ApplyBatchSize: 100,
		OnToken: func(tok engine.ResumeToken) {
			lastToken = []byte(tok)
			_ = store.Save(bg, checkpoint.State{RunID: runID, Mode: "live", ResumeToken: []byte(tok),
				Namespaces: map[string]checkpoint.NSProgress{}})
		}}); err != nil {
		t.Fatalf("open1: %v", err)
	}
	applyDone1 := make(chan error, 1)
	go func() { applyDone1 <- tl1.Apply(bg) }()

	coll := src.Database("res").Collection("aaa")
	writeWave(t, bg, coll, 0, 300)
	// Wait until a token has been checkpointed and the first wave applied.
	waitFor(t, cdcTimeout(20*time.Second), func() bool { return len(lastToken) > 0 && tl1.Stats().Applied >= 300 && tl1.Stats().Buffered == 0 })

	// Simulate a crash: stop applying and abandon the tailer WITHOUT draining.
	tl1.Stop()
	<-applyDone1
	_ = tl1.Close(bg)

	// Second wave produced while "down" — these must be picked up on resume.
	writeWave(t, bg, coll, 300, 300)

	// Resume from the checkpointed token.
	st, _, _ := store.Load(bg, runID)
	tl2 := cdc.New(src, dst, rep, log)
	if err := tl2.Open(bg, engine.TailOptions{Namespaces: nss, ApplyBatchSize: 100, Resume: engine.ResumeToken(st.ResumeToken)}); err != nil {
		t.Fatalf("open2 (resume): %v", err)
	}
	applyDone2 := make(chan error, 1)
	go func() { applyDone2 <- tl2.Apply(bg) }()
	waitFor(t, cdcTimeout(30*time.Second), func() bool { return tl2.Stats().Buffered == 0 && tl2.Stats().Applied >= 300 })
	tl2.Stop()
	<-applyDone2
	_ = tl2.Close(bg)

	// Both waves must be present on the destination.
	vf := verify.New(src, dst, log)
	vr, err := vf.Verify(bg, engine.VerifyOptions{Namespaces: nss, CheckCounts: true, SampleSize: 200, Workers: 3})
	if err != nil {
		t.Fatalf("verify: %v", err)
	}
	if !vr.Ok {
		t.Errorf("post-CDC-resume verify failed: %+v", vr.Collections)
	}
	sc, _ := coll.CountDocuments(bg, bson.D{})
	dc, _ := dst.Database("res").Collection("aaa").CountDocuments(bg, bson.D{})
	if sc != dc {
		t.Errorf("counts differ after resume: src=%d dst=%d", sc, dc)
	}
}

func writeWave(t *testing.T, ctx context.Context, coll *mongo.Collection, start, n int) {
	t.Helper()
	for i := start; i < start+n; i++ {
		if _, err := coll.InsertOne(ctx, bson.D{{Key: "_id", Value: fmt.Sprintf("live-%06d", i)}, {Key: "w", Value: i}}); err != nil {
			t.Fatalf("write wave: %v", err)
		}
	}
}

func waitFor(t *testing.T, d time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("condition not met before deadline")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func countDoneT(s checkpoint.State) int {
	n := 0
	for _, p := range s.Namespaces {
		if p.Done {
			n++
		}
	}
	return n
}
