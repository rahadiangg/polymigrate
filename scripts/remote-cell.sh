#!/usr/bin/env bash
# Containerized remote-endpoint matrix cell: runs the integration suite between a
# LOCAL single-node replica set and a REMOTE managed MongoDB (Atlas / Huawei DDS)
# reachable only over TLS.
#
# WHY containerized (this took a while to diagnose, so it's written down):
#   The macOS host's TLS stack (OpenSSL 3.6.2 / Go 1.26 crypto-tls) fails to
#   complete a TLS handshake with Atlas M0's shared-tier edge — the server sends
#   "tlsv1 alert internal error" (alert 80) regardless of TLS version or curve.
#   A Linux container connects fine (mongosh AND the Go driver both succeed from a
#   container, both fail from the host), so a host-side middlebox/VPN is mangling
#   the handshake. We therefore run the tool from INSIDE a Linux container.
#   Running the test there also dodges the "single-node RS advertises localhost"
#   problem: the local mongod is another container on the same docker network and
#   advertises its docker-DNS name (polylocal:27017), reachable from the runner.
#
# The runner image (mongo:8.0) is used only because it already ships CA certs so
# Go's x509 can verify the managed endpoint's certificate.
#
# The docker-pause transient-outage test is intentionally NOT run here: a managed
# remote node can't be paused. The other four behaviors are the full depth.
#
# Usage:  POLY_REMOTE_URI="mongodb+srv://..." scripts/remote-cell.sh <up|down> <local_ver> [label]
#   up   : local  -> remote        down : remote -> local
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT="${POLY_OUT_DIR:-/tmp/poly-matrix}"; mkdir -p "$OUT"
RESULTS="$OUT/results.tsv"
NET=polynet
RUNNER_IMG="${POLY_RUNNER_IMG:-mongo:8.0}"
TESTS='TestSnapshotAndVerify|TestLiveCDC|TestResumeAfterInterruptedSnapshot|TestCDCResumeFromToken'
EXPECT=(TestSnapshotAndVerify TestLiveCDC TestResumeAfterInterruptedSnapshot TestCDCResumeFromToken)

dir="${1:?up|down}"; lv="${2:?local_ver}"; label="${3:-remote}"
uri="${POLY_REMOTE_URI:?set POLY_REMOTE_URI}"
log(){ echo "[$(date '+%H:%M:%S')] $*"; }
mongo_shell(){ docker exec "$1" sh -c 'command -v mongosh || command -v mongo' 2>/dev/null | head -1; }

# 1. cross-compile the linux/arm64 integration test binary if missing/stale.
BIN="$ROOT/bin/e2e.linux"
if [ ! -x "$BIN" ] || [ -n "$(find "$ROOT/internal" -name '*.go' -newer "$BIN" 2>/dev/null | head -1)" ]; then
  log "cross-compiling linux test binary"
  ( cd "$ROOT" && GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go test -tags=integration -c -o "$BIN" ./internal/e2e/ ) || { log "compile failed"; exit 2; }
fi

# 2. (re)create the docker network + local single-node RS on version $lv.
docker network create "$NET" >/dev/null 2>&1 || true
docker rm -f polylocal >/dev/null 2>&1 || true
limg="${MM_LOCAL_IMAGE:-mongo:$lv}"
log "booting local ($limg) as polylocal on $NET"
docker run -d --name polylocal --network "$NET" "$limg" \
  mongod --replSet rslocal --bind_ip_all --wiredTigerCacheSizeGB 0.5 >/dev/null || { log "local boot failed"; exit 2; }
sh=""
for _ in $(seq 1 60); do
  [ -z "$sh" ] && sh="$(mongo_shell polylocal)"
  [ -n "$sh" ] && docker exec polylocal "$sh" --quiet --eval 'db.runCommand({ping:1}).ok' 2>/dev/null | grep -q 1 && break
  sleep 1
done
docker exec polylocal "$sh" --quiet --eval "rs.initiate({_id:'rslocal',members:[{_id:0,host:'polylocal:27017'}]})" >/dev/null 2>&1
for _ in $(seq 1 60); do
  docker exec polylocal "$sh" --quiet --eval 'db.runCommand({isMaster:1}).ismaster' 2>/dev/null | grep -q true && break; sleep 1
done

# 3. run the suite inside a linux container; src/dst depend on direction.
luri='mongodb://polylocal:27017/?replicaSet=rslocal'
if [ "$dir" = up ]; then src="$luri"; dst="$uri"; else src="$uri"; dst="$luri"; fi
logf="$OUT/remote_${label}_${dir}_local${lv}.log"
tag="$label:$dir:local$lv"
log "CELL $tag : running suite in $RUNNER_IMG (managed node → no transient-outage test)"
t0=$(date +%s)
docker run --rm --network "$NET" -e POLY_SRC_URI="$src" -e POLY_DST_URI="$dst" \
  -e POLY_CDC_WAIT="${POLY_CDC_WAIT:-}" \
  -v "$BIN:/e2e.test:ro" "$RUNNER_IMG" \
  /e2e.test -test.run "$TESTS" -test.v >"$logf" 2>&1
dt=$(( $(date +%s) - t0 ))

ok=1 sm=""
for t in "${EXPECT[@]}"; do
  if   grep -q -- "--- PASS: $t" "$logf"; then sm+="$t=PASS "
  elif grep -q -- "--- FAIL: $t" "$logf"; then sm+="$t=FAIL "; ok=0
  else sm+="$t=MISSING "; ok=0; fi
done
docker rm -f polylocal >/dev/null 2>&1 || true
if [ $ok -eq 1 ]; then log "CELL $tag : PASS ($sm) ${dt}s"; res=PASS
else log "CELL $tag : FAIL ($sm) ${dt}s  [log: $logf]"; res=FAIL; fi
printf '%s\t%s\t%s\t%s\t%ss\n' "$label-$dir" "$lv" "$res" "$sm" "$dt" >>"$RESULTS"
[ $ok -eq 1 ]
