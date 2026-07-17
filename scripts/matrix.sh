#!/usr/bin/env bash
# Compatibility-matrix runner for polymigrate.
#
# For each cell it boots single-node replica set(s) on the requested MongoDB
# image(s) and runs the reviewed integration suite (snapshot+verify, live CDC,
# resume-after-interrupt, transient-outage retry, CDC-resume-from-token) against
# them, recording PASS/FAIL. It supports an all-local grid and mixed local<->Atlas
# cells (managed node can't be docker-paused, so the transient-outage test is
# dropped for Atlas cells).
#
# Guards that make the results trustworthy:
#   * WT cache is capped (--wiredTigerCacheSizeGB) so two mongods coexist on an
#     8GB host; an OOM would otherwise masquerade as a migration failure.
#   * `go test` exits 0 when tests are SKIPPED (the suite t.Skip's when the URIs
#     are unset). We therefore require an explicit "--- PASS: <name>" line for
#     every expected test; SKIP / FAIL / missing => the cell FAILS.
#
# Usage:
#   scripts/matrix.sh grid                 # 20 ordered distinct pairs, both dirs
#   scripts/matrix.sh cell 4.4 8.0         # one local cell src=4.4 dst=8.0
#   scripts/matrix.sh atlas up   7.0 "<uri>"   # local 7.0 -> Atlas
#   scripts/matrix.sh atlas down 7.0 "<uri>"   # Atlas -> local 7.0
#   scripts/matrix.sh down                 # tear down all matrix containers
set -uo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
CACHE_GB="${MM_CACHE_GB:-0.5}"
GO_TIMEOUT="${MM_GO_TIMEOUT:-600s}"
OUT="${POLY_OUT_DIR:-/tmp/poly-matrix}"
RESULTS="$OUT/results.tsv"
mkdir -p "$OUT"

VERSIONS=(4.4 5.0 6.0 7.0 8.0)
ALL_TESTS='TestSnapshotAndVerify|TestLiveCDC|TestResumeAfterInterruptedSnapshot|TestTransientOutageRetry|TestCDCResumeFromToken'
ATLAS_TESTS='TestSnapshotAndVerify|TestLiveCDC|TestResumeAfterInterruptedSnapshot|TestCDCResumeFromToken'

log() { echo "[$(date '+%H:%M:%S')] $*"; }

# Pick the shell binary the image ships (mongo:4.4/5.0 have legacy `mongo`,
# 6.0+ have `mongosh`).
mongo_shell() { docker exec "$1" sh -c 'command -v mongosh || command -v mongo' 2>/dev/null | head -1; }

teardown_all() { docker rm -f poly-src poly-dst poly-local >/dev/null 2>&1; true; }

# boot_rs NAME PORT RS IMG  -> boots a single-node replica set, waits for primary.
boot_rs() {
  local name="$1" port="$2" rs="$3" img="$4"
  docker rm -f "$name" >/dev/null 2>&1
  if ! docker run -d --name "$name" -p "${port}:${port}" "$img" \
        mongod --replSet "$rs" --port "$port" --bind_ip_all \
        --wiredTigerCacheSizeGB "$CACHE_GB" >/dev/null 2>&1; then
    log "  $name: docker run FAILED ($img)"; return 1
  fi
  local sh=""
  for _ in $(seq 1 60); do
    [ -z "$sh" ] && sh="$(mongo_shell "$name")"
    if [ -n "$sh" ] && docker exec "$name" "$sh" --quiet --port "$port" \
         --eval 'db.runCommand({ping:1}).ok' 2>/dev/null | grep -q 1; then break; fi
    sleep 1
  done
  [ -z "$sh" ] && { log "  $name: no mongo shell / never pinged"; docker logs "$name" 2>&1 | tail -5; return 1; }
  docker exec "$name" "$sh" --quiet --port "$port" \
    --eval "rs.initiate({_id:'$rs',members:[{_id:0,host:'localhost:$port'}]})" >/dev/null 2>&1
  for _ in $(seq 1 60); do
    if docker exec "$name" "$sh" --quiet --port "$port" \
         --eval 'db.runCommand({isMaster:1}).ismaster' 2>/dev/null | grep -q true; then
      return 0
    fi
    sleep 1
  done
  log "  $name: never became primary"; docker logs "$name" 2>&1 | tail -5; return 1
}

# run_suite SRC_URI DST_URI LOGF TESTS_REGEX EXPECTED...
# Returns 0 iff every EXPECTED test printed "--- PASS". Echoes a per-test summary.
run_suite() {
  local src="$1" dst="$2" logf="$3" tre="$4"; shift 4
  local expected=("$@")
  POLY_SRC_URI="$src" POLY_DST_URI="$dst" \
    go test -tags=integration -count=1 -timeout "$GO_TIMEOUT" -v \
      -run "$tre" ./internal/e2e/... >"$logf" 2>&1
  local ok=1 summary=""
  for t in "${expected[@]}"; do
    if   grep -q -- "--- PASS: $t" "$logf"; then summary+="$t=PASS "
    elif grep -q -- "--- FAIL: $t" "$logf"; then summary+="$t=FAIL "; ok=0
    elif grep -q -- "--- SKIP: $t" "$logf"; then summary+="$t=SKIP "; ok=0
    else summary+="$t=MISSING "; ok=0
    fi
  done
  echo "$summary"
  return $((1 - ok))
}

# cli_smoke SRC_URI DST_URI LOGF  -> exercises the shipped `mm` binary end to end
# (seed via shell on poly-src is done by caller). Returns 0 on rc==0 & count match.
cli_smoke() {
  local src="$1" dst="$2" logf="$3"
  local sh; sh="$(mongo_shell poly-src)"
  docker exec poly-src "$sh" --quiet --port 27018 --eval '
    const c = db.getSiblingDB("clitest").widgets; c.drop();
    const a=[]; for(let i=0;i<1000;i++) a.push({_id:i,k:"k"+(i%50),n:i});
    c.insertMany(a); c.createIndex({k:1});' >/dev/null 2>&1
  "$ROOT/bin/poly" migrate --src "$src" --dst "$dst" \
    --mode snapshot --drop --include 'clitest.*' --sample-size 200 \
    --no-checkpoint --log-level warn >"$logf" 2>&1
  local rc=$?
  local dsh; dsh="$(mongo_shell poly-dst)"
  local n; n="$(docker exec poly-dst "$dsh" --quiet --port 27019 \
    --eval 'db.getSiblingDB("clitest").widgets.countDocuments({})' 2>/dev/null | tr -dc '0-9')"
  echo "cli rc=$rc dst_count=${n:-?}" >>"$logf"
  [ "$rc" = 0 ] && [ "$n" = 1000 ]
}

record() { printf '%s\t%s\t%s\t%s\t%ss\n' "$1" "$2" "$3" "$4" "$5" >>"$RESULTS"; }

# local_cell SRC_VER DST_VER [smoke]
local_cell() {
  local sv="$1" dv="$2" smoke="${3:-}"
  local tag="local:$sv->$dv" t0 dt
  t0=$(date +%s)
  log "CELL $tag : booting"
  local simg="${MM_SRC_IMAGE:-mongo:$sv}" dimg="${MM_DST_IMAGE:-mongo:$dv}"
  if ! boot_rs poly-src 27018 rs0 "$simg" || ! boot_rs poly-dst 27019 rs1 "$dimg"; then
    dt=$(( $(date +%s) - t0 )); log "CELL $tag : BOOT-FAIL"; record "$sv" "$dv" BOOT-FAIL "boot" "$dt"; teardown_all; return
  fi
  local logf="$OUT/local_${sv}_to_${dv}.log" sm
  log "CELL $tag : running suite"
  sm="$(run_suite 'mongodb://localhost:27018/?replicaSet=rs0' \
                  'mongodb://localhost:27019/?replicaSet=rs1' \
                  "$logf" "$ALL_TESTS" \
                  TestSnapshotAndVerify TestLiveCDC TestResumeAfterInterruptedSnapshot \
                  TestTransientOutageRetry TestCDCResumeFromToken)"
  local res=$?
  if [ -n "$smoke" ]; then
    local clif="$OUT/cli_${sv}_to_${dv}.log"
    if cli_smoke 'mongodb://localhost:27018/?replicaSet=rs0' 'mongodb://localhost:27019/?replicaSet=rs1' "$clif"; then
      sm+="CLI=PASS "
    else
      sm+="CLI=FAIL "; res=1
    fi
  fi
  dt=$(( $(date +%s) - t0 ))
  if [ $res -eq 0 ]; then log "CELL $tag : PASS ($sm) ${dt}s"; record "$sv" "$dv" PASS "$sm" "$dt"
  else log "CELL $tag : FAIL ($sm) ${dt}s  [log: $logf]"; record "$sv" "$dv" FAIL "$sm" "$dt"; fi
  teardown_all
}

# atlas_cell DIR LOCAL_VER ATLAS_URI
atlas_cell() {
  local dir="$1" lv="$2" auri="$3" t0 dt
  local tag="atlas:$dir:local$lv"
  t0=$(date +%s)
  log "CELL $tag : booting local poly-local mongo:$lv"
  if ! boot_rs poly-local 27020 rslocal "mongo:$lv"; then
    dt=$(( $(date +%s) - t0 )); log "CELL $tag : BOOT-FAIL"; record "atlas-$dir" "$lv" BOOT-FAIL "boot" "$dt"; teardown_all; return
  fi
  local luri='mongodb://localhost:27020/?replicaSet=rslocal' src dst
  if [ "$dir" = up ]; then src="$luri"; dst="$auri"; else src="$auri"; dst="$luri"; fi
  local logf="$OUT/atlas_${dir}_local${lv}.log" sm
  log "CELL $tag : running suite (no transient-outage: managed node)"
  sm="$(run_suite "$src" "$dst" "$logf" "$ATLAS_TESTS" \
        TestSnapshotAndVerify TestLiveCDC TestResumeAfterInterruptedSnapshot TestCDCResumeFromToken)"
  local res=$?
  dt=$(( $(date +%s) - t0 ))
  if [ $res -eq 0 ]; then log "CELL $tag : PASS ($sm) ${dt}s"; record "atlas-$dir" "$lv" PASS "$sm" "$dt"
  else log "CELL $tag : FAIL ($sm) ${dt}s  [log: $logf]"; record "atlas-$dir" "$lv" FAIL "$sm" "$dt"; fi
  teardown_all
}

grid() {
  log "=== LOCAL GRID START: 20 ordered distinct pairs of ${VERSIONS[*]} ==="
  : > "$RESULTS"
  local i=0
  for sv in "${VERSIONS[@]}"; do
    for dv in "${VERSIONS[@]}"; do
      [ "$sv" = "$dv" ] && continue
      i=$((i+1))
      local smoke=""
      { [ "$sv" = 4.4 ] && [ "$dv" = 8.0 ]; } && smoke=1
      { [ "$sv" = 8.0 ] && [ "$dv" = 4.4 ]; } && smoke=1
      log "--- cell $i/20 ---"
      local_cell "$sv" "$dv" "$smoke"
    done
  done
  log "=== LOCAL GRID COMPLETE ($i cells) ==="
  log "results: $RESULTS"
  column -t -s $'\t' "$RESULTS" 2>/dev/null || cat "$RESULTS"
  log "MATRIX COMPLETE"
}

case "${1:-grid}" in
  grid)  grid ;;
  cell)  local_cell "$2" "$3" "${4:-}" ;;
  atlas) atlas_cell "$2" "$3" "${MM_ATLAS_URI:-${4:-}}" ;;
  down)  teardown_all; log "torn down" ;;
  *) echo "usage: $0 {grid|cell SV DV|atlas DIR LV URI|down}"; exit 1 ;;
esac
