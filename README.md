# mongomigrate (`mm`)

A native Go tool for migrating data between any MongoDB-compatible deployments —
**standalone, replica set, sharded, MongoDB Atlas, Huawei Cloud DDS (4.4 / 5.0),
and Percona Server for MongoDB — in any direction.**

It does the two things a migration actually needs and does them well:

1. **Move the data** — a fast, native, parallel snapshot copier plus an optional
   change-stream tailer (CDC) for a near-zero-downtime cutover.
2. **Prove it moved correctly** — a verifier that compares per-collection counts,
   index definitions, and sampled document *content* (not just counts).

> The design follows the thesis in `../MIGRATION.md`: the bulk copy is 99% of the
> bytes; the hard, valuable part is the delta and the verification. `mm` buffers
> change events from T0, copies the snapshot, then drains the buffer so the
> cutover window is *seconds*, not hours — and it does not scale with data size.

No external `mongodump`/`mongorestore` binaries are required: `mm` is a single
self-contained binary built on the official `mongo-go-driver/v2`.

---

## Install / build

```bash
make build          # -> bin/mm
# or
go install github.com/weefer/mongomigrate/cmd/mm@latest
```

Requires Go 1.26+.

---

## Commands

### `mm discover` — classify an endpoint

```bash
mm discover --uri "mongodb://127.0.0.1:27018/?replicaSet=rs0"
```
```
host:            127.0.0.1
flavor:          mongodb          # mongodb | atlas | huawei-dds | percona | cosmosdb | documentdb
version:         7.0.37
topology:        replicaset       # standalone | replicaset | sharded
replica set:     rs0
change streams:  true             # can this source drive live CDC migration?
```

### `mm migrate` — copy data (snapshot or live)

Snapshot (one-shot; freeze writes for a consistent copy):
```bash
mm migrate \
  --src "mongodb://SRC/?replicaSet=rs0" \
  --dst "mongodb://DST/?replicaSet=rs1" \
  --mode snapshot --drop
```

Live (near-zero-downtime; opens a change stream first, then snapshots, then
drains the delta and cuts over):
```bash
mm migrate --src ... --dst ... --mode live --lag-threshold 1s
# --auto-cutover (default) cuts over automatically when lag is low.
# --auto-cutover=false waits: freeze source writes, then press Enter to cut over.
```

The run prints a report and finishes with a verification pass:
```
phases: init -> tail-open -> snapshot -> apply -> cutover -> verify -> done
copied: 7500 docs across 3 collections in 319ms
=== verification report: PASS ===
  shop.orders   PASS   counts: src=5500 dst=5500 match=true | sample: 100 checked, 0 mismatch | indexes match=true
```

### `mm verify` — prove a destination matches its source

```bash
mm verify --src ... --dst ... --sample-size 200
```
Exits non-zero if any collection fails. Checks counts, index definitions, and a
content-hash sample. Useful after *any* migration path (DRS, dump/restore, `mm`).

### `mm tail` — CDC-only continuous replication

```bash
mm tail --src ... --dst ...      # applies change events until Ctrl-C
```
Keeps a destination in sync after a snapshot. Idempotent, so it is safe to stop
and restart.

---

## Key flags (migrate)

| Flag | Default | Meaning |
|---|---|---|
| `--src`, `--dst` | — | source / destination URIs (required) |
| `--src-flavor`, `--dst-flavor` | auto | override detection (e.g. `huawei-dds`) |
| `--include`, `--exclude` | — | namespace globs, repeatable (e.g. `shop.*`) |
| `--include-system` | false | include `admin`/`local`/`config` + `system.*` |
| `--mode` | `snapshot` | `snapshot` or `live` |
| `--workers` | 4 | collections copied/verified concurrently |
| `--batch-size` | 1000 | docs per destination bulk write |
| `--drop` | false | drop each destination collection first |
| `--copy-indexes` | true | recreate source indexes on the destination |
| `--lag-threshold` | 2s | live: cutover-ready threshold |
| `--auto-cutover` | true | live: cut over automatically when lag is low |
| `--sample-size` | 100 | docs hashed per collection during verify |

---

## Compatibility notes

- **Change streams require a replica set or sharded cluster** on MongoDB 3.6+.
  `mm` checks this and refuses `--mode live` against a standalone (use `snapshot`).
  DDS 4.4 and 5.0 replica sets qualify.
- **Managed sources (Atlas, Huawei DDS)** hide `admin`/`local`/`config`. `mm`
  excludes system databases by default, which is also what you want: **users and
  roles are intentionally NOT migrated** — let the destination (e.g. the Percona
  operator) own its users and recreate application users deliberately.
- **Cosmos DB / AWS DocumentDB** are detected and treated as change-stream-incapable
  (they emulate the API with restricted CDC), so `mm` steers them to snapshot mode.
- **Idempotency**: the tailer applies `ReplaceOne(upsert)` / `DeleteOne` keyed by
  `_id` using `fullDocument: updateLookup`, so replays and partial failures are
  safe. The copier treats duplicate-key as already-present, so a `--mode snapshot`
  re-run *without* `--drop` is safe.

## Resilience — surviving crashes and outages

A migration checkpoints its progress durably, so it survives the death of the
source, the destination, or `mm` itself, and rides out transient outages.

- **Checkpointing is on by default.** Progress (per-collection snapshot position +
  the last *applied* CDC token) is written to a `_mongomigrate.checkpoints`
  collection on the destination and mirrored to a local `.mm-checkpoints/` file.
- **Resume** a crashed or interrupted run:
  ```bash
  mm migrate --src ... --dst ... --mode live --resume
  ```
  Completed collections are skipped, an in-progress one continues from its last
  `_id`, and (live) the change stream resumes from the checkpointed token.
  Snapshot copy on resume uses idempotent upserts, so the boundary batch is safe
  to re-copy — no data is lost or duplicated. `--resume` never drops.
- **Inspect / clear** a run:
  ```bash
  mm status --run-id <id>          # per-collection progress + resumable flag
  mm checkpoint clear --run-id <id>
  ```
  The run id is printed at startup and defaults to a stable hash of
  (source, destination, filter); override with `--run-id`. Disable checkpointing
  with `--no-checkpoint`.
- **Transient outages** (a brief destination pause, a primary step-down) are
  absorbed by bounded exponential-backoff retry on the write paths; the retried
  writes are idempotent, so recovery is safe.
- **The cardinal safety invariant**: the checkpointed CDC token is only ever
  advanced *after* the corresponding events are applied to the destination, so a
  resume can only ever re-apply already-applied events (idempotent) — never skip
  unapplied ones.
- **Lost change-stream history** (source oplog rolled past the resume point, error
  286) is reported as a distinct, fatal error and the run is marked non-resumable:
  a fresh snapshot is required rather than a silent, incomplete resume.

## Limitations (by design / MongoDB constraints)

- **The cutover needs a brief write freeze.** Live mode shrinks downtime to the
  final drain, but the last delta after "cutover-ready" must be captured with
  writes stopped (`--auto-cutover=false` gives you that control). This is inherent
  to logical migration — there is no way to close the very last gap without it.
- **DDL is not replayed during CDC on 4.4.** Create collections/indexes on the
  destination; `mm --copy-indexes` recreates indexes as part of the snapshot.
- **`geoHaystack` indexes** (removed in MongoDB 5.0) will fail to recreate on a
  modern destination — drop them on a 4.4 source first.

---

## Architecture

```
cmd/mm ── internal/cli ─┬─ internal/migrate (orchestrator, interface-only)
                        │        ├─ engine.Copier   ← internal/copier  (parallel snapshot)
                        │        ├─ engine.Tailer   ← internal/cdc     (buffer-then-apply CDC)
                        │        └─ engine.Verifier ← internal/verify  (counts+indexes+hash)
                        └─ internal/{mongox,config,namespace,progress,logx,engine}
```

- `internal/engine` is the frozen contract (interfaces + value types). The
  orchestrator depends only on it, so it is fully unit-testable with fakes.
- `internal/mongox` handles connection + topology/flavor discovery.
- Every driver-touching component hides the driver behind a narrow interface, so
  its orchestration logic is unit-tested without a live server.

---

## Testing

```bash
make test          # unit tests (no database needed)
make test-race     # unit tests with the race detector
make cover         # coverage summary

# Integration tests against two real replica sets:
MM_SRC_URI="mongodb://127.0.0.1:27018/?replicaSet=rs0" \
MM_DST_URI="mongodb://127.0.0.1:27019/?replicaSet=rs1" \
make integration
```

The unit suite covers all pure logic and orchestration (namespace filtering,
copier batching/dup-key/index reconstruction, CDC parse/dedup/idempotency/lag,
verify hashing/index-diff/sampling, and every migrate phase path). The
`integration` suite drives a real snapshot + live-CDC migration and verifies it.
