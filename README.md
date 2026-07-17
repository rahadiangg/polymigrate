# polymigrate (`poly`)

[![CI](https://github.com/rahadiangg/polymigrate/actions/workflows/ci.yml/badge.svg)](https://github.com/rahadiangg/polymigrate/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/rahadiangg/polymigrate?sort=semver)](https://github.com/rahadiangg/polymigrate/releases)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue)](LICENSE)

A single-binary Go tool for migrating data between any MongoDB-compatible
deployment — **standalone, replica set, sharded, MongoDB Atlas, Huawei Cloud DDS
(4.4 / 5.0), and Percona Server for MongoDB — in any direction.**

It does the two things a migration actually needs:

1. **Move the data** — a fast, native, parallel snapshot copier plus an optional
   change-stream tailer (CDC) for a near-zero-downtime cutover.
2. **Prove it moved correctly** — a verifier that compares per-collection counts,
   index definitions, and sampled document *content* (not just counts).

`poly` buffers change events from T0, copies the snapshot, then drains the buffer,
so the cutover window is *seconds*, not hours — and it does not scale with data
size. No external `mongodump`/`mongorestore` is required: it is a self-contained
binary built on the official `mongo-go-driver/v2`.

---

## Install

**Script (Linux / macOS):**

```bash
curl -fsSL https://raw.githubusercontent.com/rahadiangg/polymigrate/master/install.sh | sh
```

Installs the latest release to `/usr/local/bin` (or `$HOME/.local/bin`). Pin a
version or directory with `POLY_VERSION=v0.1.0` / `POLY_INSTALL_DIR=~/bin`.

**Go:**

```bash
go install github.com/rahadiangg/polymigrate/cmd/poly@latest
```

**Binaries:** grab a prebuilt archive from the
[releases page](https://github.com/rahadiangg/polymigrate/releases) (Linux/macOS/Windows,
amd64/arm64).

**From source** (requires Go 1.26+):

```bash
make build      # -> bin/poly
```

---

## Quick start

```bash
# 1. Inspect an endpoint — topology, flavor, change-stream capability.
poly discover --uri "mongodb://SRC/?replicaSet=rs0"

# 2. Near-zero-downtime migration (snapshot + live CDC + verify).
poly migrate --src "mongodb://SRC/?replicaSet=rs0" \
             --dst "mongodb://DST/?replicaSet=rs1" \
             --mode live --drop

# 3. Prove the destination matches (after any migration path).
poly verify  --src "mongodb://SRC/..." --dst "mongodb://DST/..."
```

---

## Commands

### `poly discover` — classify an endpoint

```
host:            127.0.0.1
flavor:          mongodb          # mongodb | atlas | huawei-dds | percona | cosmosdb | documentdb
version:         7.0.37
topology:        replicaset       # standalone | replicaset | sharded
replica set:     rs0
change streams:  true             # can this source drive live CDC migration?
```

### `poly migrate` — copy data (snapshot or live)

Snapshot (one-shot; freeze writes for a consistent copy):

```bash
poly migrate --src "mongodb://SRC/?replicaSet=rs0" \
             --dst "mongodb://DST/?replicaSet=rs1" \
             --mode snapshot --drop
```

Live (opens a change stream first, then snapshots, then drains the delta and cuts
over):

```bash
poly migrate --src ... --dst ... --mode live --lag-threshold 1s
# --auto-cutover (default) cuts over automatically when lag is low.
# --auto-cutover=false waits: freeze source writes, then press Enter to cut over.
```

Each run prints a report and finishes with a verification pass:

```
phases: init -> tail-open -> snapshot -> apply -> cutover -> verify -> done
copied: 7500 docs across 3 collections in 319ms
=== verification report: PASS ===
  shop.orders   PASS   counts: src=5500 dst=5500 match=true | sample: 100 checked, 0 mismatch | indexes match=true
```

### `poly verify` — prove a destination matches its source

```bash
poly verify --src ... --dst ... --sample-size 200
```

Exits non-zero if any collection fails. Checks counts, index definitions, and a
content-hash sample. Useful after *any* migration path (DRS, dump/restore, `poly`).

### `poly tail` — CDC-only continuous replication

```bash
poly tail --src ... --dst ...      # applies change events until Ctrl-C
```

Keeps a destination in sync after a snapshot. Idempotent — safe to stop and restart.

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
| `--resume` | false | resume a crashed/interrupted run from its checkpoint |
| `--sample-size` | 100 | docs hashed per collection during verify |

---

## Resilience

A migration checkpoints its progress durably, so it survives the death of the
source, the destination, or `poly` itself, and rides out transient outages.

- **Checkpointing is on by default** — per-collection snapshot position + the last
  *applied* CDC token, written to a `_polymigrate.checkpoints` collection on the
  destination and mirrored to a local `.poly-checkpoints/` file.
- **Resume** a crashed run with `--resume`: completed collections are skipped, an
  in-progress one continues from its last `_id`, and (live) the change stream
  resumes from the checkpointed token. Snapshot resume uses idempotent upserts, so
  the boundary batch is safe to re-copy. `--resume` never drops.
- **Inspect / clear** with `poly status --run-id <id>` and
  `poly checkpoint clear --run-id <id>`. The run id is printed at startup.
- **Transient outages** are absorbed by bounded exponential-backoff retry on the
  idempotent write paths.
- **Cardinal safety invariant**: the checkpointed CDC token only ever advances
  *after* its events are applied, so a resume can only re-apply already-applied
  events (idempotent) — never skip unapplied ones.

---

## Compatibility

`poly` auto-detects flavor and topology and adapts. Highlights:

- **Change streams require a replica set or sharded cluster** (MongoDB 3.6+).
  `poly` refuses `--mode live` against a standalone — use `snapshot`.
- **Managed sources (Atlas, Huawei DDS)** hide `admin`/`local`/`config`. `poly`
  excludes system databases by default — **users and roles are intentionally NOT
  migrated**; let the destination own its users.
- **Cosmos DB / AWS DocumentDB** are detected and steered to snapshot mode (their
  emulated change streams are unusable for CDC).
- **Live CDC from a *sharded cluster* source** hits a known mongo-driver crash;
  `poly` refuses it up front and steers you to snapshot mode. Replica-set sources
  are unaffected.

**Tested end-to-end** against real community MongoDB (4.4–8.0), MongoDB Atlas,
Huawei Cloud DDS (4.4 & 5.0, replica set + cluster), and Percona Server for MongoDB
(7.0 & 8.0, operator-deployed) — in both directions. See
[**docs/COMPATIBILITY.md**](docs/COMPATIBILITY.md) for the full version-granular
matrix, the coverage model, and the sharded-CDC known issue.

---

## Limitations (by design / MongoDB constraints)

- **The cutover needs a brief write freeze.** Live mode shrinks downtime to the
  final drain, but the last delta must be captured with writes stopped
  (`--auto-cutover=false` gives you that control). This is inherent to logical
  migration.
- **DDL is not replayed during CDC on 4.4.** Create collections/indexes on the
  destination; `--copy-indexes` recreates indexes as part of the snapshot.
- **`geoHaystack` indexes** (removed in MongoDB 5.0) will fail to recreate on a
  modern destination — drop them on a 4.4 source first.

---

## Architecture

```
cmd/poly ── internal/cli ─┬─ internal/migrate (orchestrator, interface-only)
                          │        ├─ engine.Copier   ← internal/copier  (parallel snapshot)
                          │        ├─ engine.Tailer   ← internal/cdc     (buffer-then-apply CDC)
                          │        └─ engine.Verifier ← internal/verify  (counts+indexes+hash)
                          └─ internal/{mongox,config,namespace,progress,logx,engine}
```

- `internal/engine` is the frozen contract (interfaces + value types); the
  orchestrator depends only on it, so it is fully unit-testable with fakes.
- `internal/mongox` handles connection + topology/flavor discovery.
- Every driver-touching component hides the driver behind a narrow interface, so its
  orchestration logic is unit-tested without a live server.

---

## Testing

```bash
make test          # unit tests (no database needed)
make test-race     # unit tests with the race detector
make cover         # coverage summary

# Integration tests against two real replica sets:
POLY_SRC_URI="mongodb://127.0.0.1:27018/?replicaSet=rs0" \
POLY_DST_URI="mongodb://127.0.0.1:27019/?replicaSet=rs1" \
make integration
```

The unit suite covers all pure logic and orchestration (namespace filtering, copier
batching/dup-key/index reconstruction, CDC parse/dedup/idempotency/lag, verify
hashing/index-diff/sampling, and every migrate phase path). The `integration` suite
drives a real snapshot + live-CDC migration and verifies it. `scripts/testbed.sh`
spins up two seeded local replica sets for it.

---

## Contributing

Issues and PRs welcome. Please run `make fmt vet test` before opening a PR; CI runs
gofmt, `go vet`, and the race-enabled unit suite on every push.

## License

[Apache License 2.0](LICENSE).
