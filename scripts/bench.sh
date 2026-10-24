#!/usr/bin/env bash
# Load-test a running node against the performance criteria (PRF-*).
#
#   scripts/bench.sh [SCENARIO] [--quick] [--url URL] [-h]
#
# Uses `oha` when installed (brew install oha / cargo install oha); otherwise falls back to a plain
# curl loop that is only a smoke test (no connection reuse, ~16 clients, results are NOT comparable
# with the targets). Start the node first (`make docker-run`, ideally with RT_CPUS=4 RT_MEMORY=2g to
# match the reference box) and deploy the target service(s); see sample/scripts/deploy.sh.
#
# Scenarios (what this script automates):
#   prf-01   WASM hello, warm            GET $RT_INVOKE/s/$BENCH_WASM_SLUG/   >= 50,000 RPS, p99 <= 10 ms
#   prf-02   Deno hello, warm            GET $RT_INVOKE/s/$BENCH_DENO_SLUG/   >=  4,000 RPS, p99 <= 25 ms
#   prf-08   Mixed 50/50 WASM + Deno at half of their single-kind targets (both at once, oha only);
#            compare each p99 with the single-kind runs: must stay <= 2x
#   url      any URL (--url), no target check
#   list     print every PRF scenario and how to run it   (default)
#
# Not automated here:
#   PRF-03 / PRF-07  1k-service corpus, 80/20 hot set / round-robin 500 RPS for 10 min: deploy the
#                    corpus, then drive it with `oha --urls-from-file` (bench/ scenarios, to be added)
#   PRF-04           overhead at 50 % load: run prf-01/02 with BENCH_QPS = half of the measured RPS
#   PRF-05           first request after T2 / T3 eviction: restart the node on a warm / cold /data and time one request
#   PRF-06           idle RSS with 1k services registered: `docker stats --no-stream rt-dev`  (<= 64 MB)
#   PRF-09           scheduler micro (SCH-23, >= 1M tasks/s): `make test-perf`
#   PRF-10           ingress variant comparison (S-07): run prf-01 on both builds
#
# Protocol (defaults): 30 s warm-up, 120 s measurement, 3 runs, median reported, 256 keep-alive
# connections. --quick: 2 s / 10 s / 1 run / 32 connections, for checking the setup.
#
# Env (all optional):
#   RT_INVOKE         public listener base (default http://127.0.0.1:8080)
#   RT_INVOKE_KEY     `rtk_...` tenant key, only for access=key services
#   BENCH_WASM_SLUG   default hello-wasm        BENCH_DENO_SLUG   default hello
#   BENCH_CONNS (256)  BENCH_WARMUP (30)  BENCH_DURATION (120)  BENCH_RUNS (3)   seconds / counts
#   BENCH_QPS         rate limit in requests/s (oha only); unset = closed loop at full speed
#   BENCH_OUT         results dir (default bench/results); file <date>-<git sha>.json, one entry per scenario
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

die() { echo "error: $*" >&2; exit 1; }
usage() { sed -n '2,/^set -euo/{/^set -euo/d;s/^# \{0,1\}//;p;}' "${BASH_SOURCE[0]}"; }

SCENARIO="list" QUICK=0 URL_ARG=""
while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help) usage; exit 0 ;;
    --quick)   QUICK=1 ;;
    --url)     [[ $# -ge 2 ]] || die "--url needs a value"; URL_ARG="$2"; shift ;;
    prf-01|prf-02|prf-08|url|list) SCENARIO="$1" ;;
    *)         die "unknown argument '$1' (see --help)" ;;
  esac
  shift
done

if [[ "$SCENARIO" == list ]]; then
  usage | sed -n '/^Scenarios/,/^Protocol/p' | sed '$d'
  exit 0
fi

command -v curl >/dev/null 2>&1 || die "curl is required"
command -v jq >/dev/null 2>&1 || die "jq is required"

RT_INVOKE="${RT_INVOKE:-http://127.0.0.1:8080}"; RT_INVOKE="${RT_INVOKE%/}"
WASM_SLUG="${BENCH_WASM_SLUG:-hello-wasm}"
DENO_SLUG="${BENCH_DENO_SLUG:-hello}"
if [[ "$QUICK" == 1 ]]; then
  CONNS="${BENCH_CONNS:-32}" WARMUP="${BENCH_WARMUP:-2}" DURATION="${BENCH_DURATION:-10}" RUNS="${BENCH_RUNS:-1}"
else
  CONNS="${BENCH_CONNS:-256}" WARMUP="${BENCH_WARMUP:-30}" DURATION="${BENCH_DURATION:-120}" RUNS="${BENCH_RUNS:-3}"
fi
QPS="${BENCH_QPS:-}"
OUT_DIR="${BENCH_OUT:-bench/results}"

HAVE_OHA=0
command -v oha >/dev/null 2>&1 && HAVE_OHA=1
if [[ "$HAVE_OHA" == 0 ]]; then
  echo "note: oha not found, using the curl fallback (smoke test only; install oha for real numbers)" >&2
  [[ "$SCENARIO" != prf-08 ]] || die "prf-08 needs oha (brew install oha / cargo install oha)"
fi

# oha >= 1.x: --output-format json; older: -j.
OHA_JSON=(--output-format json)
if [[ "$HAVE_OHA" == 1 ]] && ! oha --help 2>&1 | grep -q -- '--output-format'; then OHA_JSON=(-j); fi

AUTH=()
[[ -n "${RT_INVOKE_KEY:-}" ]] && AUTH=(-H "Authorization: Bearer $RT_INVOKE_KEY")

WORK="$(mktemp -d "${TMPDIR:-/tmp}/rt-bench.XXXXXX")"
trap 'rm -rf "$WORK"' EXIT

# preflight URL: the service must exist and answer 2xx before we hammer it.
preflight() {
  local url="$1" code
  code="$(curl -sS -o /dev/null -w '%{http_code}' --max-time 10 ${AUTH[@]+"${AUTH[@]}"} "$url" 2>&1)" \
    || die "cannot reach $url (is the node up? RT_INVOKE=$RT_INVOKE)"
  [[ "$code" =~ ^2 ]] || die "GET $url answered HTTP $code: deploy the service first, e.g.
       sample/scripts/deploy.sh sample/deno/hello hello   (slugs: BENCH_WASM_SLUG / BENCH_DENO_SLUG)"
}

# oha_run URL SECONDS CONNS QPS OUTFILE  (QPS may be empty)
oha_run() {
  local url="$1" secs="$2" conns="$3" qps="$4" out="$5" q=()
  [[ -n "$qps" ]] && q=(-q "$qps")
  oha -z "${secs}s" -c "$conns" --no-tui "${OHA_JSON[@]}" ${q[@]+"${q[@]}"} ${AUTH[@]+"${AUTH[@]}"} "$url" > "$out" 2>/dev/null \
    || die "oha failed against $url"
}

now() {
  local t
  t="$(date +%s.%N 2>/dev/null || true)"
  if [[ "$t" =~ ^[0-9]+\.[0-9]+$ ]]; then echo "$t"; return; fi
  perl -MTime::HiRes=time -e 'printf "%.3f\n", time' 2>/dev/null || date +%s
}

# curl_run URL OUTFILE: N requests with 16 parallel clients; writes the same JSON shape as the oha summary.
curl_run() {
  local url="$1" out="$2" par=16 total t0 t1
  total=$(( DURATION * 100 )); [[ $total -gt 5000 ]] && total=5000   # ~ smoke-test sized
  t0="$(now)"
  seq "$total" | xargs -P "$par" -I{} curl -s -o /dev/null --max-time 10 ${AUTH[@]+"${AUTH[@]}"} \
      -w '%{time_total} %{http_code}\n' "$url" > "$WORK/curl.raw" || true
  t1="$(now)"
  sort -n -k1,1 "$WORK/curl.raw" | awk -v t0="$t0" -v t1="$t1" '
    { if ($2 ~ /^2/) ok++; t[NR] = $1 }
    END {
      n = NR
      if (n == 0) { print "{}"; exit }
      p50 = t[int((n - 1) * 0.50) + 1]; p99 = t[int((n - 1) * 0.99) + 1]
      el = t1 - t0; if (el <= 0) el = 1
      printf "{\"summary\":{\"requestsPerSec\":%.1f,\"successRate\":%.4f},\"latencyPercentiles\":{\"p50\":%.6f,\"p99\":%.6f}}\n", n / el, ok / n, p50, p99
    }' > "$out"
}

run_once() { # URL OUTFILE [QPS]
  if [[ "$HAVE_OHA" == 1 ]]; then oha_run "$1" "$DURATION" "$CONNS" "${3:-$QPS}" "$2"; else curl_run "$1" "$2"; fi
}

median() { jq -s -r "map($1) | sort | .[(length / 2 | floor)]" "${@:2}"; }

# bench_url ID LABEL URL MIN_RPS MAX_P99_MS  -> prints a report, appends to the results file, returns 1 on a missed target
bench_url() {
  local id="$1" label="$2" url="$3" min_rps="$4" max_p99="$5" i files=()
  echo "== $id: $label"
  echo "   GET $url  conns=$CONNS warmup=${WARMUP}s measure=${DURATION}s runs=$RUNS ${QPS:+qps=$QPS }tool=$([[ $HAVE_OHA == 1 ]] && echo oha || echo curl)"
  preflight "$url"
  if [[ "$HAVE_OHA" == 1 && "$WARMUP" -gt 0 ]]; then
    echo "   warm-up ${WARMUP}s"
    oha_run "$url" "$WARMUP" "$CONNS" "$QPS" "$WORK/warmup.json"
  fi
  for (( i = 1; i <= RUNS; i++ )); do
    files+=("$WORK/$id-$i.json")
    run_once "$url" "${files[$((i-1))]}"
    printf '   run %d: %s RPS, p99 %s ms, ok %s%%\n' "$i" \
      "$(jq -r '.summary.requestsPerSec | floor' "${files[$((i-1))]}")" \
      "$(jq -r '.latencyPercentiles.p99 * 1000 | . * 100 | floor / 100' "${files[$((i-1))]}")" \
      "$(jq -r '.summary.successRate * 100 | . * 100 | floor / 100' "${files[$((i-1))]}")"
  done
  local rps p50 p99 ok verdict="n/a"
  rps="$(median '.summary.requestsPerSec' "${files[@]}")"
  p50="$(median '.latencyPercentiles.p50 * 1000' "${files[@]}")"
  p99="$(median '.latencyPercentiles.p99 * 1000' "${files[@]}")"
  ok="$(median '.summary.successRate' "${files[@]}")"
  local rc=0 target="none"
  [[ -n "$min_rps" ]] && target=">= $min_rps RPS, p99 <= $max_p99 ms"
  if [[ "$HAVE_OHA" == 1 && -n "$min_rps" && -z "$QPS" ]]; then
    if jq -n -e --argjson r "$rps" --argjson p "$p99" --argjson ok "$ok" --argjson mr "$min_rps" --argjson mp "$max_p99" \
         '$r >= $mr and $p <= $mp and $ok >= 0.999' >/dev/null; then verdict="PASS"; else verdict="FAIL"; rc=1; fi
  fi
  local note=""
  [[ "$QUICK" == 1 && "$verdict" != n/a ]] && note=" (quick run, not a valid measurement)"
  printf '   median: %.0f RPS, p50 %.2f ms, p99 %.2f ms, success %.2f%%   target: %s   => %s%s\n' \
    "$rps" "$p50" "$p99" "$(jq -n "$ok * 100")" "$target" "$verdict" "$note"

  mkdir -p "$OUT_DIR"
  local sha res
  sha="$(git -C "$ROOT" rev-parse --short=12 HEAD 2>/dev/null || echo nogit)"
  res="$OUT_DIR/$(date +%Y-%m-%d)-$sha.json"
  [[ -s "$res" ]] || echo '{}' > "$res"
  jq --arg id "$id" --arg url "$url" --arg tool "$([[ $HAVE_OHA == 1 ]] && echo oha || echo curl)" \
     --argjson rps "$rps" --argjson p50 "$p50" --argjson p99 "$p99" --argjson ok "$ok" \
     --argjson conns "$CONNS" --argjson dur "$DURATION" --argjson runs "$RUNS" --arg verdict "$verdict" \
     --arg date "$(date -u +%Y-%m-%dT%H:%M:%SZ)" --arg sha "$sha" --arg host "$(uname -sm)" \
     '.date = $date | .sha = $sha | .host = $host
      | .scenarios[$id] = {url: $url, tool: $tool, connections: $conns, duration_s: $dur, runs: $runs,
                           median: {rps: $rps, p50_ms: $p50, p99_ms: $p99, success_rate: $ok}, verdict: $verdict}' \
     "$res" > "$res.tmp" && mv "$res.tmp" "$res"
  echo "   recorded in $res"
  return "$rc"
}

case "$SCENARIO" in
  prf-01) bench_url prf-01 "WASM hello, warm" "$RT_INVOKE/s/$WASM_SLUG/" 50000 10 ;;
  prf-02) bench_url prf-02 "Deno hello, warm" "$RT_INVOKE/s/$DENO_SLUG/" 4000 25 ;;
  url)    [[ -n "$URL_ARG" ]] || die "scenario 'url' needs --url URL"
          bench_url url "custom URL" "$URL_ARG" "" "" ;;
  prf-08)
    # Both kinds at once, each at half of its single-kind RPS target (rate-limited closed loop).
    echo "== prf-08: mixed 50/50 WASM + Deno at half of the single-kind targets (concurrent)"
    preflight "$RT_INVOKE/s/$WASM_SLUG/"; preflight "$RT_INVOKE/s/$DENO_SLUG/"
    oha_run "$RT_INVOKE/s/$WASM_SLUG/" "$DURATION" "$CONNS" 25000 "$WORK/mix-wasm.json" &
    wpid=$!
    oha_run "$RT_INVOKE/s/$DENO_SLUG/" "$DURATION" "$CONNS" 2000 "$WORK/mix-deno.json" &
    dpid=$!
    wait "$wpid" "$dpid" || die "a mixed-load oha run failed"
    for k in wasm deno; do
      printf '   %-5s %s RPS, p50 %s ms, p99 %s ms, ok %s%%\n' "$k" \
        "$(jq -r '.summary.requestsPerSec | floor' "$WORK/mix-$k.json")" \
        "$(jq -r '.latencyPercentiles.p50 * 1000 | . * 100 | floor / 100' "$WORK/mix-$k.json")" \
        "$(jq -r '.latencyPercentiles.p99 * 1000 | . * 100 | floor / 100' "$WORK/mix-$k.json")" \
        "$(jq -r '.summary.successRate * 100 | . * 100 | floor / 100' "$WORK/mix-$k.json")"
    done
    echo "   check: each p99 <= 2x its single-kind p99 (run prf-01 / prf-02), and re-kinding converged (SCH-19: GET /v1/node/scheduler)"
    mkdir -p "$OUT_DIR"
    sha="$(git -C "$ROOT" rev-parse --short=12 HEAD 2>/dev/null || echo nogit)"
    res="$OUT_DIR/$(date +%Y-%m-%d)-$sha.json"
    [[ -s "$res" ]] || echo '{}' > "$res"
    jq --slurpfile w "$WORK/mix-wasm.json" --slurpfile d "$WORK/mix-deno.json" --arg sha "$sha" \
       '.sha = $sha | .scenarios["prf-08"] = {wasm: {rps: $w[0].summary.requestsPerSec, p99_ms: ($w[0].latencyPercentiles.p99 * 1000)},
                                              deno: {rps: $d[0].summary.requestsPerSec, p99_ms: ($d[0].latencyPercentiles.p99 * 1000)}}' \
       "$res" > "$res.tmp" && mv "$res.tmp" "$res"
    echo "   recorded in $res"
    ;;
esac
