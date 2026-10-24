#!/usr/bin/env bash
# WebSockets on a real node .
#
#   bench/ws.sh <rt-binary> [N]      # N held sockets (default 2000)
#
# 1. Event round trip: an echo service (`webSocketMessage` sends the message back), 150 messages
#    one after another on one socket (under the per-socket burst of 200: past it a client is held
#    to 50 messages/s, i.e. 20 ms each), with warm objects (default) and with `ws.warm = false`.
# 2. Memory per idle socket: node RSS before and after holding N sockets of the echo service.
# Needs the spike client: cargo build -p rt-triggers --example ws_spike. Output: bench/out/ws.txt
set -uo pipefail
BIN="$1"; N="${2:-2000}"
REPO="$(cd "$(dirname "$0")/.." && pwd)"; DIR="$REPO/bench"; OUT="$DIR/out/ws.txt"
SPIKE="$REPO/target/debug/examples/ws_spike"
API=http://127.0.0.1:18081; INV=http://127.0.0.1:18080; MET=http://127.0.0.1:19090
export RT_ROOT_TOKEN=bench-root-token RT_MASTER_KEY=$(printf '%064x' 42) RT_LOG=warn
ulimit -n 65536 2>/dev/null || true
mkdir -p "$DIR/out"; : > "$OUT"; rm -rf "$DIR/data" && mkdir -p "$DIR/data"; cd "$REPO"
"$BIN" serve --config "$DIR/rt.toml" > "$DIR/out/node-ws.log" 2>&1 & PID=$!
trap 'kill $PID 2>/dev/null; kill $(jobs -p) 2>/dev/null; wait 2>/dev/null || true' EXIT
for _ in $(seq 1 300); do curl -fsS "$MET/readyz" >/dev/null 2>&1 && break; sleep 0.2; done
log() { echo "$*" | tee -a "$OUT"; }
rss_kb() { ps -o rss= -p "$PID" | tr -d ' '; }
H=(-H "Authorization: Bearer $RT_ROOT_TOKEN" -H 'Content-Type: application/json')
TEN=$(curl -sS -X POST "$API/v1/tenants" "${H[@]}" -d '{"name":"ws","plan":"bench"}' | jq -r .id)
KEY=$(curl -sS -X POST "$API/v1/tenants/$TEN/keys" "${H[@]}" -d '{"scopes":["services:read","services:write","deploy"]}' | jq -r .secret)
K=(-H "Authorization: Bearer $KEY")
SRC="$DIR/out/ws-echo.ts"
cat > "$SRC" <<'TS'
import { sockets } from "std:sockets";
export default {
  fetch(req: Request) { return sockets.accept(req); },
  webSocketMessage(ws: any, message: string) { ws.send(message); },
};
TS
deploy() { # slug extra-config-json
  local id; id=$(curl -sS -X POST "$API/v1/services" "${K[@]}" -H 'Content-Type: application/json' \
    -d "$(jq -nc --arg s "$1" --argjson x "$2" '{slug: $s, runtime: "deno"} + $x')" | jq -r .id)
  curl -sS -o /dev/null -X POST "$API/v1/services/$id/versions?activate=1" "${K[@]}" -H "Content-Type: application/typescript" --data-binary @"$SRC"
}
deploy wswarm '{}'
deploy wscold '{"ws":{"warm":false}}'
for s in wswarm wscold; do "$SPIKE" latency "ws://127.0.0.1:18080/s/$s/" 20 >/dev/null; done
log "event round trip, warm objects (default): $("$SPIKE" latency ws://127.0.0.1:18080/s/wswarm/ 150)"
log "event round trip, ws.warm = false:        $("$SPIKE" latency ws://127.0.0.1:18080/s/wscold/ 150)"
log "past the burst (rate limit 50/s):         $("$SPIKE" latency ws://127.0.0.1:18080/s/wswarm/ 400)"
sleep 1; R0=$(rss_kb)
"$SPIKE" hold ws://127.0.0.1:18080/s/wswarm/ "$N" > "$DIR/out/ws-hold.log" 2>&1 & HOLDER=$!
for _ in $(seq 1 120); do grep -q holding "$DIR/out/ws-hold.log" && break; sleep 1; done
sleep 3; R1=$(rss_kb)
log "$(cat "$DIR/out/ws-hold.log" | head -1): node RSS +$(( (R1 - R0) / 1024 )) MiB, $(( (R1 - R0) / N )) KiB per idle socket"
kill $HOLDER 2>/dev/null
