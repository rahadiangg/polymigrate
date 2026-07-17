# Compatibility — tested against real deployments

`poly` was exercised end-to-end against real community MongoDB, MongoDB Atlas,
Huawei Cloud DDS, and Percona Server for MongoDB deployments using
`scripts/matrix.sh` (all-local version grid) and `scripts/remote-cell.sh`
(managed endpoints, run from a Linux container). Each cell runs the full
integration suite — snapshot + verify, live CDC, resume-after-interrupt,
CDC-resume-from-token — in **both directions**.

Last full run: **2026-07-17**.

## At a glance — source → destination grid

Rows are the **source**, columns are the **destination** — read a cell as
"migrate *from the row* *into the column*". **Every version is its own
row/column**, even for the same provider (Percona 7.0 vs 8.0, each MongoDB
version, DDS by version + topology).

| **from ↓ / into →** | MongoDB 4.4 | MongoDB 5.0 | MongoDB 6.0 | MongoDB 7.0 | MongoDB 8.0 | Atlas 8.0 | DDS 4.4 RS | DDS 5.0 RS | DDS 4.4 Cluster | DDS 5.0 Cluster | Percona 7.0 | Percona 8.0 |
|---|:--:|:--:|:--:|:--:|:--:|:--:|:--:|:--:|:--:|:--:|:--:|:--:|
| **MongoDB 4.4** | ▪️ | ✅ | ✅ | ✅ | ✅ | ✅ | ✅ | ➖ | ✅ | ➖ | ✅ | ➖ |
| **MongoDB 5.0** | ✅ | ▪️ | ✅ | ✅ | ✅ | ➖ | ➖ | ✅ | ➖ | ➖ | ➖ | ➖ |
| **MongoDB 6.0** | ✅ | ✅ | ▪️ | ✅ | ✅ | ➖ | ➖ | ➖ | ➖ | ➖ | ➖ | ➖ |
| **MongoDB 7.0** | ✅ | ✅ | ✅ | ▪️ | ✅ | ✅ | ➖ | ➖ | ➖ | ➖ | ✅ | ➖ |
| **MongoDB 8.0** | ✅ | ✅ | ✅ | ✅ | ▪️ | ➖ | ➖ | ➖ | ➖ | ➖ | ✅ | ✅ |
| **Atlas 8.0** | ✅ | ➖ | ➖ | ✅ | ➖ | ▪️ | ➖ | ✅ | 🟡 | ➖ | ✅ | ✅ |
| **DDS 4.4 RS** | ✅ | ➖ | ➖ | ➖ | ➖ | ➖ | ▪️ | ➖ | ➖ | ➖ | ✅ | ✅ |
| **DDS 5.0 RS** | ➖ | ✅ | ➖ | ➖ | ➖ | ✅ | ➖ | ▪️ | ➖ | ➖ | ✅ | ✅ |
| **DDS 4.4 Cluster** | 🟡 | ➖ | ➖ | ➖ | ➖ | 🟡 | ➖ | ➖ | ▪️ | ➖ | 🟡 | ➖ |
| **DDS 5.0 Cluster** | ➖ | ➖ | ➖ | ➖ | ➖ | ➖ | ➖ | ➖ | ➖ | ▪️ | 🟡 | ➖ |
| **Percona 7.0** | ✅ | ➖ | ➖ | ✅ | ✅ | ✅ | ✅ | ✅ | 🟡 | 🟡 | ▪️ | ➖ |
| **Percona 8.0** | ➖ | ➖ | ➖ | ➖ | ✅ | ✅ | ✅ | ✅ | ➖ | ➖ | ➖ | ▪️ |

**Legend** — ✅ tested and passed (snapshot + live CDC + resume + verify) · 🟡 snapshot
works, **live CDC limited** (from a sharded cluster the driver crashes — see [Known
issue](#known-issue--live-cdc-from-a-sharded-cluster-source-crashes-the-driver);
into a cluster CDC has latency) · ▪️ **same source & destination** — a database to
itself, not a migration (the diagonal) · ➖ **that exact from→to wasn't tested — not a
failure** (the same behaviour is covered by a neighbouring cell).

> **Percona 7.0** = a Percona Server for MongoDB 7.0 container; **Percona 8.0** =
> Percona Server for MongoDB 8.0 deployed by the **Percona Kubernetes Operator** (the
> operator only deploys; the engine is Percona Server). Both are `flavor: percona`
> to `poly`. 8.0 got the fuller sweep, which is why the 7.0 row/column has more ➖ —
> not failures. The dense green block top-left is the 20-pair community version grid
> (4.4 → 8.0, all directions). The only 🟡 limitation is **live CDC from a sharded
> cluster**, a mongo-driver bug, not `poly`.

## What passed

| Path | Result |
|---|---|
| Community MongoDB **4.4 → 8.0**, all 20 ordered version pairs, both directions (local RS ↔ local RS) | ✅ 20/20 |
| **MongoDB Atlas** (M0, 8.0) ↔ local 4.4 & 7.0, both directions | ✅ 4/4 cells |
| **Huawei DDS 4.4 Replica set** ↔ local 4.4, both directions | ✅ |
| **Huawei DDS 5.0 Replica set** ↔ local 5.0, both directions | ✅ |
| local **→ Huawei DDS 4.4 Cluster** (sharded, into cluster) | ✅ |
| **Huawei DDS 4.4 Cluster → local** (from cluster) | ⚠️ snapshot/resume ✅; **live CDC blocked by a mongo-driver crash** (below) |
| **Huawei DDS 4.4 & 5.0 RS ↔ Percona Server for MongoDB 8.0** (operator-deployed), both directions | ✅ 4/4 each way |
| **Percona Server for MongoDB 8.0 ↔ community MongoDB 8.0** (operator-deployed), both directions | ✅ 4/4 both dirs |
| **MongoDB Atlas ↔ Percona Server for MongoDB 8.0**, both directions | ✅ |
| **Huawei DDS 4.4 Cluster ↔ MongoDB Atlas** (snapshot, both directions) | ✅ (from-cluster may need a retry — mongos verify timing) |
| **Huawei DDS 5.0 Cluster ↔ Percona Server 8.0** (snapshot, both directions) | ✅ |
| _(earlier, on a Percona Server **7.0** container)_ DDS 5.0 RS ↔ Percona 7.0, Atlas ↔ Percona 7.0, Percona 7.0 ↔ community — both directions | ✅ |
| **Standalone MongoDB → Percona** (snapshot) — and `--mode live` correctly refused | ✅ |

## Coverage model

Managed/flavor endpoints are paired with a local peer — **community MongoDB _or_
real Percona Server for MongoDB** — in both directions. The **DDS → Percona** path
is validated against a **real Percona** instance (not a stand-in), as are
Atlas ↔ Percona, Percona ↔ community MongoDB, and standalone → Percona.

- The Percona endpoint is **Percona Server for MongoDB 8.0** (image
  `percona-server-mongodb:8.0.4-1`) deployed by the **Percona Kubernetes Operator
  v1.19.1**. The operator is only the *deployment mechanism*; the endpoint is
  labelled by its engine (Percona Server 8.0), not by the operator.
- `poly discover` reports `flavor: percona` (from the Percona build suffix, e.g. the
  operator image's `8.0.4-1`). Against operator-deployed Percona 8.0 the full suite
  (snapshot, live CDC, resume, CDC-resume-from-token) passes **4/4 in both
  directions**, including **directly against live Huawei DDS 4.4 and 5.0 replica
  sets** (managed DDS source → Percona 8.0 destination and back) and vs community
  MongoDB 8.0. The tool was also exercised against a **Percona Server 7.0 container**
  with identical results. To validate against your own deployment, point `--src` /
  `--dst` at its URI — the CLI and harness accept any URI.
- **DDS 5.0 Cluster** was also validated — snapshot passes **both directions** vs
  operator Percona (into-cluster and from-cluster), mirroring the 4.4 Cluster; its
  live-CDC-from-a-sharded-source path is the same documented driver limitation.
- Combinations **not** run against real deployments: sharded **shard-key
  preservation** (the cluster tests use unsharded collections); and **Cosmos DB /
  AWS DocumentDB** — their flavor is auto-detected and `--mode live` is refused
  (both lack usable change streams), but no end-to-end data migration has been
  exercised against a live instance of either.

## Known issue — live CDC from a sharded cluster source crashes the driver

Migrating **into** a sharded cluster and taking a **snapshot from** one both
work. But opening a **live change stream against a sharded cluster (mongos)** hits
a **mongo-driver bug**: the change-stream reader panics with a nil-pointer
dereference in `ChangeStream.loopNext` (`change_stream.go:734`, `cs.cursor.Err()`)
~10–20 s after events start flowing, around the mongos no-op-writer advancement.
Root cause is confirmed and is **not** in `poly`:

- **Reproduced with the bare Go driver** (`db.Watch` / `coll.Watch` / `client.Watch`,
  with or without `updateLookup`, one mongos or two) — all crash identically after
  correctly delivering the events up to that point.
- **Replica-set sources are unaffected** — DDS 4.4 & 5.0 replica-set live CDC passes
  cleanly, as do Atlas and community replica sets.
- Present in mongo-driver **v2.7.0 and v2.8.0** (v2.8.0 is the latest), so it is not
  a recent regression and there is no fixed release to pin to yet.

**Implication:** with the current driver, **live CDC from a sharded cluster source
is not viable** — use **snapshot mode** (which works) with a brief write freeze, or
run CDC from a **replica set**.

**How `poly` handles it (shipped).** Two layers so the crash can never take down a run:

1. **Pre-flight guard** — `poly migrate --mode live` / `poly tail` refuse to start
   when the source is a sharded DDS cluster, with a clear "use `--mode snapshot`, or
   migrate from a replica set" message (the same way `poly` refuses live for
   standalone / Cosmos / DocumentDB). This fires when the DDS flavor is detected or
   forced with `--src-flavor huawei-dds`.
2. **Panic safety-net** — the CDC read loop wraps the driver call in `recover()`, so
   if the driver *does* panic (e.g. the source was reached via a bare EIP and its
   flavor read as plain `mongodb`, bypassing the pre-flight guard), the crash is
   converted into a clean, actionable error instead of aborting the process.

Both are covered by unit tests, and the pre-flight refusal was verified end-to-end
against a live DDS 5.0 cluster.

**A second, milder cluster wrinkle — snapshot _verify_ timing.** A snapshot **from**
a sharded cluster occasionally fails its post-copy count check on the first attempt
and passes on retry. The copied data is correct — the verifier's sampled documents
**always match**; only the `countDocuments` totals momentarily disagree because a
`mongos` can return a not-yet-settled count right after a bulk seed (a fresh
insert-then-count on the same cluster is consistent, so this is read-timing, not
loss). Re-running verify, or letting the balancer settle before verifying, clears it.

The integration CDC catch-up deadline is env-configurable via **`POLY_CDC_WAIT`**
(e.g. `POLY_CDC_WAIT=90s`) for sharded change-stream latency; the 30 s default suits
replica sets. (This tunes the *tests*; it does not fix the cluster-source driver crash.)

## Connecting to managed endpoints

- **Atlas / DDS replica set**: standard SRV or RS URI. DDS advertises its bound
  EIPs, so `?authSource=admin&replicaSet=replica` connects over public EIPs.
- **DDS cluster**: bind EIPs to the **mongos** nodes and connect to those, with no
  `replicaSet` param.
- **Host-TLS note**: a host-side middlebox can break TLS to Atlas M0 from the host
  while a Linux container connects fine — `scripts/remote-cell.sh` runs the tool in a
  container to sidestep it.
