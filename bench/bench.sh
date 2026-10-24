#!/usr/bin/env bash
# Throughput/latency of three services at four concurrency levels .
#
#   bench/bench.sh <rt-binary> <label>            # e.g. bench/bench.sh target/release/rt before
#   BENCH_CONFIG=bench/rt-warm.toml bench/bench.sh target/release/rt warm
#   BENCH_C="1 16" BENCH_SECS=4 bench/bench.sh ...   # fewer / shorter passes
#
# Needs: a release build, oha, jq, curl, and `make fixtures` (fixtures/wasm/out/hello.wasm).
# Results: bench/out/<label>.txt (one line per pass, with the status-code distribution).
set -euo pipefail
BIN="$1"; LABEL="$2"
REPO="$(cd "$(dirname "$0")/.." && pwd)"
DIR="$REPO/bench"
CONFIG="${BENCH_CONFIG:-$DIR/rt.toml}"
LEVELS="${BENCH_C:-1 16 64 256}"
SECS="${BENCH_SECS:-6}"
API=http://127.0.0.1:18081; INV=http://127.0.0.1:18080; MET=http://127.0.0.1:19090
export RT_ROOT_TOKEN=bench-root-token RT_MASTER_KEY=$(printf '%064x' 42) RT_LOG=warn
mkdir -p "$DIR/out"
OUT="$DIR/out/$LABEL.txt"; : > "$OUT"
rm -rf "$DIR/data" && mkdir -p "$DIR/data"
cd "$REPO"
"$BIN" serve --config "$CONFIG" > "$DIR/out/node-$LABEL.log" 2>&1 & PID=$!
trap 'kill $PID 2>/dev/null; wait $PID 2>/dev/null || true' EXIT
for _ in $(seq 1 300); do curl -fsS "$MET/readyz" >/dev/null 2>&1 && break; sleep 0.2; done
H=(-H "Authorization: Bearer $RT_ROOT_TOKEN" -H 'Content-Type: application/json')
TEN=$(curl -sS -X POST "$API/v1/tenants" "${H[@]}" -d '{"name":"bench","plan":"bench"}' | jq -r .id)
KEY=$(curl -sS -X POST "$API/v1/tenants/$TEN/keys" "${H[@]}" \
  -d '{"scopes":["services:read","services:write","deploy"]}' | jq -r .secret)
deploy() { # slug runtime content-type file
  local id; id=$(curl -sS -X POST "$API/v1/services" -H "Authorization: Bearer $KEY" -H 'Content-Type: application/json' \
    -d "{\"slug\":\"$1\",\"runtime\":\"$2\"}" | jq -r .id)
  curl -sS -o /dev/null -X POST "$API/v1/services/$id/versions?activate=1" -H "Authorization: Bearer $KEY" \
    -H "Content-Type: $3" --data-binary @"$4"
  for _ in $(seq 1 600); do [ "$(curl -s -o /dev/null -w '%{http_code}' "$INV/s/$1/")" = 200 ] && return; sleep 0.2; done
  echo "deploy $1: not ready" >&2; exit 1
}
deploy hello   deno application/typescript "$REPO/sample/deno/hello/main.ts"
deploy sqltodo deno application/typescript "$REPO/sample/deno/sql-todo/main.ts"
deploy whello  wasm application/wasm       "$REPO/fixtures/wasm/out/hello.wasm"
run() { oha --no-tui --output-format json -c "$3" -z "${SECS}s" "$2" 2>/dev/null | jq -r --arg n "$1" --arg c "$3" \
  '[$n,"c="+$c,"rps="+(.summary.requestsPerSec|floor|tostring),
    "p50="+((.latencyPercentiles.p50*1000*100|floor)/100|tostring)+"ms",
    "p99="+((.latencyPercentiles.p99*1000*100|floor)/100|tostring)+"ms",
    "status="+(.statusCodeDistribution|tostring)]|join(" ")' | tee -a "$OUT"; }
for c in $LEVELS; do
  run deno-hello "$INV/s/hello/?name=x" "$c"; run deno-sql "$INV/s/sqltodo/?done=0" "$c"; run wasm-hello "$INV/s/whello/" "$c"
done
echo "threads=$(ps -M -p $PID 2>/dev/null | tail -n +2 | wc -l | tr -d ' ')" | tee -a "$OUT"
curl -s "$MET/metrics" | grep -E '^rt_deno_isolate_create_seconds_(sum|count)|^rt_admission_rejects_total|^rt_mem_committed_bytes' \
  | tee -a "$OUT" || true
