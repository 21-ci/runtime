#!/usr/bin/env bash
# Resource cost per instance and per request (docs: guides/resources).
#
#   bench/resources.sh <rt-binary> [N]      # N services per runtime (default 20)
#
# 1. Idle node RSS.
# 2. N Deno `hello` services with reuse on and min_instances=1 (warm floor), then N WASM `hello`
#    services with reuse on and min_instances=1: RSS delta / warm instances (target_instances).
# 3. CPU per request: process CPU time over a fixed number of requests (oha, c=8), Deno warm and WASM.
# Output: bench/out/resources.txt
set -uo pipefail
BIN="$1"; N="${2:-20}"
REPO="$(cd "$(dirname "$0")/.." && pwd)"; DIR="$REPO/bench"; OUT="$DIR/out/resources.txt"
API=http://127.0.0.1:18081; INV=http://127.0.0.1:18080; MET=http://127.0.0.1:19090
export RT_ROOT_TOKEN=bench-root-token RT_MASTER_KEY=$(printf '%064x' 42) RT_LOG=warn
mkdir -p "$DIR/out"; : > "$OUT"; rm -rf "$DIR/data" && mkdir -p "$DIR/data"; cd "$REPO"
"$BIN" serve --config "$DIR/rt.toml" > "$DIR/out/node-resources.log" 2>&1 & PID=$!
trap 'kill $PID 2>/dev/null; wait $PID 2>/dev/null || true' EXIT
for _ in $(seq 1 300); do curl -fsS "$MET/readyz" >/dev/null 2>&1 && break; sleep 0.2; done
rss_mb() { ps -o rss= -p "$PID" | awk '{printf "%.1f", $1/1024}'; }
cpu_s() { ps -o time= -p "$PID" | python3 -c 'import sys; p=[float(x) for x in sys.stdin.read().strip().replace("-",":").split(":")]; print(sum(v*60**i for i,v in enumerate(reversed(p))))'; }
creates() { curl -s "$MET/metrics" | awk '/^rt_deno_isolate_create_seconds_count/ {print $2}'; }
log() { echo "$*" | tee -a "$OUT"; }
H=(-H "Authorization: Bearer $RT_ROOT_TOKEN" -H 'Content-Type: application/json')
TEN=$(curl -sS -X POST "$API/v1/tenants" "${H[@]}" -d '{"name":"res","plan":"bench"}' | jq -r .id)
KEY=$(curl -sS -X POST "$API/v1/tenants/$TEN/keys" "${H[@]}" -d '{"scopes":["services:read","services:write","deploy"]}' | jq -r .secret)
K=(-H "Authorization: Bearer $KEY")
deploy() { # slug runtime content-type file extra-config-json
  local id; id=$(curl -sS -X POST "$API/v1/services" "${K[@]}" -H 'Content-Type: application/json' \
    -d "$(jq -nc --arg s "$1" --arg r "$2" --argjson x "$5" '{slug: $s, runtime: $r} + $x')" | jq -r .id)
  curl -sS -o /dev/null -X POST "$API/v1/services/$id/versions?activate=1" "${K[@]}" -H "Content-Type: $3" --data-binary @"$4"
  echo "$id"
}
targets() { # sum of target_instances over the given ids
  local sum=0 t; for id in "$@"; do
    t=$(curl -sS "$API/v1/services/$id" "${K[@]}" | jq -r '.status.admission.target_instances // 0'); sum=$((sum + t)); done; echo "$sum"; }
sleep 2; IDLE=$(rss_mb); log "node idle RSS: ${IDLE} MB"; C0=$(creates)

deploy d1 deno application/typescript "$REPO/sample/deno/hello/main.ts" '{"deno":{"reuse_isolates":true}}' >/dev/null
deploy w1 wasm application/wasm "$REPO/fixtures/wasm/out/hello.wasm" '{"wasm":{"reuse_instances":true}}' >/dev/null
curl -s -o /dev/null "$INV/s/d1/"; curl -s -o /dev/null "$INV/s/w1/"

active() { # label slug url-path count: hold count requests in flight, sample RSS on the plateau
  local r0 r1; r0=$(rss_mb)
  local pids=(); for _ in $(seq 1 "$4"); do curl -s -o /dev/null "$INV/s/$2$3" & pids+=($!); done
  sleep 2; r1=$(rss_mb); wait "${pids[@]}"
  log "$1: $4 in flight, RSS +$(echo "$r1 - $r0" | bc) MB, ~$(echo "scale=2; ($r1 - $r0) / $4" | bc) MB per active instance"; }
SLEEPER="$DIR/out/sleeper.ts"
echo 'export default async () => { await new Promise((r) => setTimeout(r, 4000)); return new Response("ok"); };' > "$SLEEPER"
deploy dslow deno application/typescript "$SLEEPER" '{"scaling":{"max_instances":1000},"limits":{"wall_ms":10000}}' >/dev/null
deploy wslow wasm application/wasm "$REPO/fixtures/wasm/out/stream.wasm" '{"scaling":{"max_instances":1000},"limits":{"wall_ms":10000}}' >/dev/null
curl -s -o /dev/null "$INV/s/dslow/"; curl -s -o /dev/null "$INV/s/wslow/?chunks=1&delay_ms=10"
active "deno active (setTimeout 4 s)" dslow / 100
active "wasm active (stream fixture)" wslow "/?chunks=2&delay_ms=2000" 100

cpu_per_req() { # label url requests
  local c0 c1; c0=$(cpu_s); oha --no-tui --output-format quiet -c 8 -n "$3" "$2" >/dev/null 2>&1; c1=$(cpu_s)
  log "$1: $(echo "scale=1; ($c1 - $c0) * 1000000 / $3" | bc) us CPU per request (node process, $3 requests, c=8)"; }
cpu_per_req "deno hello (warm reuse)" "$INV/s/d1/" 20000
cpu_per_req "wasm hello (reuse)" "$INV/s/w1/" 50000
log "node RSS after load: $(rss_mb) MB"
