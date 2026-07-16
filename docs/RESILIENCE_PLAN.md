# mongomigrate — Resilience Tier: Implementation Plan

**Status: PLANNED (not yet implemented). Scheduled to execute at 22:20 Asia/Jakarta.**

Goal: make a migration survive the death of the source, the destination, or the
`mm` process itself, and recover from transient outages — without data loss and
without redoing work already done. Today the tool is *correct* for the happy path
and safe to re-run, but it is not *crash-resilient*: a live-mode crash loses the
in-memory CDC position and forces a fresh snapshot; a transient outage aborts the
run. This tier closes that gap.

This plan is self-contained: an agent (or a person) can execute it end to end by
following the phases below. It mirrors the workflow that built the tool
originally — freeze a contract, fan out parallel worktree implementations via a
Workflow, integrate, test against a real Docker testbed, then run an adversarial
review workflow and fix what it finds.

---

## Design principles

1. **Resume must never lose or duplicate data.** CDC apply is already idempotent
   (ReplaceOne-upsert / DeleteOne keyed by `_id`); snapshot copy must become
   idempotent too, so re-copying an overlapping range is safe.
2. **Checkpoint is the source of truth for progress.** It is written atomically
   and frequently enough that a crash loses at most one batch of *idempotent*
   work.
3. **Prefer re-reading over durable buffering.** Because the CDC resume token lets
   us re-read events from the source oplog, a durable on-disk event buffer is
   redundant *as long as the token is checkpointed frequently and the oplog
   window is adequate*. We checkpoint the token, we do NOT build a disk buffer.
4. **Fail loud, not silent.** `ChangeStreamHistoryLost` (err 286) and any
   unrecoverable state must produce a clear, actionable error — never a silent
   partial success. (The review already fixed the verify-verdict-ignored bug;
   keep that discipline.)
5. **Everything stays unit-testable without a DB** (narrow interfaces + fakes),
   with fault-injection proven against Docker behind the `integration` tag.

---

## Components

### 1. Checkpoint store — `internal/checkpoint` (new)
Persist and load migration state so a run can resume.

```go
type State struct {
    RunID        string
    Mode         string                 // "snapshot" | "live"
    ConfigHash   string                 // guards against resuming with changed options
    Phase        string                 // engine.Phase
    Namespaces   map[string]NSProgress  // key = "db.coll"
    ResumeToken  []byte                 // CDC resume token (raw BSON)
    UpdatedAt    time.Time
}
type NSProgress struct {
    Done       bool
    LastID     []byte  // raw BSON of the last copied _id (snapshot resume point)
    DocsCopied int64
}
type Store interface {
    Save(ctx context.Context, s State) error
    Load(ctx context.Context, runID string) (State, bool, error)
    Delete(ctx context.Context, runID string) error
}
```

Implementations:
- `mongoStore` — an upsert into a `_mongomigrate.checkpoints` collection on the
  **destination** (co-located with the data, survives loss of the `mm` host).
- `fileStore` — atomic JSON via write-temp-then-rename to a local path.
- `composite` — writes both (dest authoritative, file as a fast local mirror);
  default.

Atomicity: `fileStore` uses `os.WriteFile` to a temp file + `os.Rename`;
`mongoStore` uses a single `ReplaceOne(upsert)`. Corrupt/partial file → treated
as "no checkpoint" with a warning, never a crash.

### 2. Resumable snapshot — `internal/copier` (change)
- Add a per-namespace resume point to `CopyOptions` (or a new `ResumeFrom
  map[namespace.Namespace][]byte` of last `_id`).
- When resuming a namespace: query `{_id: {$gt: lastID}}` sorted by `_id`
  (the copier already sorts by `_id`, so this is a filter change).
- Make writes idempotent on overlap: use `ReplaceOne(upsert)` keyed by `_id`
  instead of `InsertMany` **when resuming** (fresh copy may keep the faster
  `InsertMany` + dup-key tolerance). A crash mid-batch re-copies at most the last
  batch, which upsert makes safe.
- Checkpoint after each collection completes, and every N batches with the last
  `_id` copied, via a callback the orchestrator wires to the checkpoint store.

### 3. Resumable CDC — `internal/cdc` (change)
- Persist the resume token to the checkpoint after each applied batch (throttled
  to ≤ once/second) via a callback. On crash, at most one *idempotent* batch is
  replayed.
- On Open, if the checkpoint has a token for this run, resume with it
  (`TailOptions.Resume` already exists — wire it).
- Detect `ChangeStreamHistoryLost` (code 286): stop, mark the run
  non-resumable in the checkpoint, and return a distinct
  `ErrChangeStreamHistoryLost` telling the operator a fresh snapshot is required.

### 4. Retry / backoff — `internal/retry` (new)
```go
type Policy struct { MaxAttempts int; BaseDelay, MaxDelay, MaxElapsed time.Duration; Jitter bool }
func Do(ctx context.Context, p Policy, classify func(error) Class, fn func() error) error
type Class int // Retryable | Fatal
```
- Exponential backoff + jitter; honors `ctx`; caps by attempts and elapsed time.
- `classify` distinguishes retryable (network/socket, pool, server-selection,
  not-primary/not-master: 10107/13435/189/91/9001, write concern timeout) from
  fatal (auth 13/18, `ChangeStreamHistoryLost` 286, invalid namespace, bad option).
- Wrap: copier batch writes + index creation, CDC apply writes, and initial
  connect. Uses an injectable clock so backoff timing is unit-tested
  deterministically.

### 5. Graceful shutdown & pre-flight
- On SIGINT/SIGTERM (already `signal.NotifyContext`), flush a final checkpoint in
  the orchestrator's defer so a re-run resumes.
- Pre-flight health checks before starting: source reachable; destination
  writable; (live) source supports change streams; estimate the source **oplog
  time window** and warn if the projected snapshot duration risks exceeding it
  (the real-world cause of 286).

### 6. CLI — `internal/cli` (change)
- `mm migrate --run-id <id>` (names/repeats a run) and `--resume` (auto-load the
  checkpoint for `--run-id` and continue snapshot + CDC).
- `mm status --run-id <id>` — print checkpoint progress.
- `mm checkpoint clear --run-id <id>` — discard a checkpoint.
- Default `--run-id` derived from a hash of (src, dst, filter) so a bare re-run of
  the same migration finds its checkpoint.

---

## Execution phases

- **Phase 0 (inline):** freeze the contract — add `internal/checkpoint` interfaces
  + `internal/retry` skeleton + the orchestrator/copier/cdc callback hooks
  (compiling stubs). Commit as the foundation for the fan-out.
- **Phase 1 (Workflow, parallel worktrees):** implement in parallel —
  (a) `checkpoint` store (mongo + file + composite) + tests,
  (b) `retry` package + tests,
  (c) copier resume (filter + upsert + checkpoint callbacks) + tests,
  (d) cdc token-persistence + resume + 286 handling + tests.
- **Phase 2 (inline integrate):** wire callbacks into `internal/migrate`
  (checkpoint on phase/collection/token, resume on start, graceful-shutdown
  flush), add the CLI flags/commands and pre-flight. Full `go build ./... && go
  test ./... -race`.
- **Phase 3 (fault-injection integration tests, Docker):** behind `integration`:
  1. kill the **destination** container mid-snapshot → restart it → `--resume` →
     verify PASS.
  2. kill the **`mm` process** mid-CDC → re-run `--resume` → resumes from token →
     verify PASS, zero loss.
  3. **pause/unpause** the destination (transient outage) → retry recovers →
     verify PASS.
  4. advance the source oplog past the token → tool returns
     `ErrChangeStreamHistoryLost` clearly (no silent partial success).
  Reuse and extend `scripts/testbed.sh` (add `pause`/`kill-dst`/`restart-dst`).
- **Phase 4 (adversarial review Workflow):** correctness review focused on
  resume-safety — checkpoint atomicity and ordering (is the token ever
  checkpointed *ahead* of the applied data? that would lose data on resume),
  retry idempotency, snapshot resume boundary (`$gt` off-by-one on `_id`),
  graceful-shutdown races. Verify each finding with independent skeptics; fix the
  confirmed ones with regression tests.

---

## Acceptance criteria (definition of done)

1. Kill destination mid-snapshot → `--resume` → completes → `mm verify` PASS.
2. Kill `mm` mid-CDC → `--resume` → resumes from token → `mm verify` PASS, source
   and destination counts + sampled content identical (zero data loss).
3. Transient destination outage (pause ≥ several seconds) → run auto-recovers via
   retry → PASS.
4. `ChangeStreamHistoryLost` → distinct fatal error instructing a fresh snapshot;
   never reported as success.
5. The entire existing unit suite still passes, `-race` clean, coverage not
   regressed; `go vet` and `gofmt` clean.
6. Checkpoint ordering invariant proven by test: the resume token is only
   persisted **after** the corresponding events are applied to the destination
   (so a crash never skips unapplied events).

---

## Non-goals (explicitly out of scope for this tier)

- Sharded shard-key / chunk preservation (separate work item).
- A durable on-disk CDC event buffer (redundant given token checkpointing — see
  principle 3).
- Live testing against real Atlas / Huawei DDS (needs real managed instances; the
  local matrix uses community MongoDB + Percona + a restricted-user DDS proxy).
- Distributed/multi-worker coordination of a single migration.

---

## Guardrails for the (unattended) executing agent

- Do the work in `mongomigrate/` on a new branch; commit per phase; do NOT push
  anywhere (no remote configured, and none should be added).
- Follow the same quality bar as the original build: narrow interfaces + fakes,
  table-driven tests, `-race`, adversarial review before declaring done.
- Bring up the Docker testbed with `scripts/testbed.sh up` for integration
  phases; tear it down (`scripts/testbed.sh down`) when finished.
- If a decision is genuinely ambiguous, prefer the safest-for-data option and
  record the decision in the final report — do not stop and wait (this is an
  unattended run).
- Produce a final summary: what was implemented, test results (unit + fault
  injection), review findings + fixes, and anything deferred.
