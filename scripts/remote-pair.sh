#!/usr/bin/env bash
# Run the integration suite between TWO remote/managed endpoints (no local mongo),
# from a Linux container (avoids the host-TLS issue). Both directions.
# The docker-pause transient-outage test is excluded (managed nodes can't be paused).
#
# Usage: MM_A="<uriA>" MM_B="<uriB>" [POLY_CDC_WAIT=60s] scripts/remote-pair.sh <label>
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
OUT="${POLY_OUT_DIR:-/tmp/poly-matrix}"; mkdir -p "$OUT"
RESULTS="$OUT/results.tsv"
RUNNER_IMG="${POLY_RUNNER_IMG:-mongo:8.0}"
BIN="$ROOT/bin/e2e.linux"
TESTS='TestSnapshotAndVerify|TestLiveCDC|TestResumeAfterInterruptedSnapshot|TestCDCResumeFromToken'
EXPECT=(TestSnapshotAndVerify TestLiveCDC TestResumeAfterInterruptedSnapshot TestCDCResumeFromToken)

label="${1:-pair}"; A="${MM_A:?set MM_A}"; B="${MM_B:?set MM_B}"
[ -x "$BIN" ] || { echo "missing $BIN — build with: GOOS=linux GOARCH=arm64 CGO_ENABLED=0 go test -tags=integration -c -o bin/e2e.linux ./internal/e2e/"; exit 2; }
log(){ echo "[$(date '+%H:%M:%S')] $*"; }

run_dir(){ # arrow src dst
  local logf="$OUT/pair_${label}_$1.log" ok=1 sm="" t0 dt
  t0=$(date +%s)
  log "CELL $label $1 : running suite (managed<->managed)"
  docker run --rm -e POLY_SRC_URI="$2" -e POLY_DST_URI="$3" -e POLY_CDC_WAIT="${POLY_CDC_WAIT:-}" \
    -v "$BIN:/e2e.test:ro" "$RUNNER_IMG" /e2e.test -test.run "$TESTS" -test.v >"$logf" 2>&1
  dt=$(( $(date +%s) - t0 ))
  for t in "${EXPECT[@]}"; do
    if grep -q -- "--- PASS: $t" "$logf"; then sm+="$t=PASS "; else sm+="$t=FAIL "; ok=0; fi
  done
  if [ $ok -eq 1 ]; then log "CELL $label $1 : PASS ($sm) ${dt}s"; else log "CELL $label $1 : FAIL ($sm) ${dt}s [log:$logf]"; fi
  printf '%s\t%s\t%s\t%s\t%ss\n' "$label" "$1" "$([ $ok -eq 1 ] && echo PASS || echo FAIL)" "$sm" "$dt" >>"$RESULTS"
}

run_dir "A->B" "$A" "$B"
run_dir "B->A" "$B" "$A"
