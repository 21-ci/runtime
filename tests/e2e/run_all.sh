#!/usr/bin/env bash
# Run every black-box E2E script against a running container and summarize.
#
#   RT_API=http://127.0.0.1:8081 RT_INVOKE=http://127.0.0.1:8080 RT_ROOT_TOKEN=… tests/e2e/run_all.sh [name-filter]
#
# The filter is a substring of the script name (e.g. `wasm` runs 30_ and 31_). Each script prints
# PASS / SKIP / FAIL and exits non-zero on failure; this runner exits non-zero if any failed.
# Environment: RT_API, RT_INVOKE, RT_METRICS, RT_ROOT_TOKEN (required), RT_E2E_PLAN, RT_E2E_BUILD_WASM,
# RT_E2E_SCHEDULE_SECONDS. KEEP_GOING=0 stops at the first failure (default: run everything).
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
: "${RT_API:=http://127.0.0.1:8081}"
: "${RT_INVOKE:=http://127.0.0.1:8080}"
: "${RT_METRICS:=http://127.0.0.1:9090}"
export RT_API RT_INVOKE RT_METRICS
if [[ -z "${RT_ROOT_TOKEN:-}" ]]; then
  echo "FAIL run_all: RT_ROOT_TOKEN must be set"
  exit 1
fi
export RT_ROOT_TOKEN
FILTER="${1:-}"

# Wait for the node (up to 60 s) so the first script does not race the container start.
deadline=$(( SECONDS + 60 ))
until curl -sS -o /dev/null --max-time 3 -f "${RT_METRICS%/}/readyz" 2>/dev/null; do
  if (( SECONDS >= deadline )); then
    echo "FAIL run_all: ${RT_METRICS%/}/readyz did not become ready within 60 s"
    exit 1
  fi
  sleep 1
done

passed=0; failed=0; skipped=0
failed_names=()
shopt -s nullglob
scripts=("$DIR"/[0-9][0-9]_*.sh)
if [[ ${#scripts[@]} -eq 0 ]]; then
  echo "FAIL run_all: no scripts found in $DIR"
  exit 1
fi

for script in "${scripts[@]}"; do
  name="$(basename "$script" .sh)"
  if [[ -n "$FILTER" && "$name" != *"$FILTER"* ]]; then
    continue
  fi
  echo "=== $name"
  start=$SECONDS
  out="$(mktemp)"
  if bash "$script" >"$out" 2>&1; then rc=0; else rc=$?; fi
  cat "$out"
  result="$(grep -E '^(PASS|SKIP|FAIL) ' "$out" | tail -n 1 || true)"
  rm -f "$out"
  case "$rc:$result" in
    0:PASS*)  passed=$((passed + 1)) ;;
    0:SKIP*)  skipped=$((skipped + 1)) ;;
    *)        failed=$((failed + 1)); failed_names+=("$name")
              [[ $rc -ne 0 ]] || echo "FAIL $name: exited 0 without a PASS/SKIP line" ;;
  esac
  echo "    ($((SECONDS - start)) s)"
  if (( failed > 0 )) && [[ "${KEEP_GOING:-1}" == "0" ]]; then
    break
  fi
done

echo
echo "E2E summary: $passed passed, $skipped skipped, $failed failed"
if (( failed > 0 )); then
  printf 'FAILED: %s\n' "${failed_names[*]}"
  exit 1
fi
echo "PASS e2e"
