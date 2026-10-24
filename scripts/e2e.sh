#!/usr/bin/env bash
# Black-box E2E: docker build -> docker compose up -> wait for /healthz -> tests/e2e/run_all.sh -> compose down.
#
#   scripts/e2e.sh [--no-build] [--keep] [-- <args for run_all.sh>]
#
#   --no-build   reuse the existing image ($IMAGE) instead of running scripts/docker-build.sh
#   --keep       leave the stack running afterwards (pass or fail) for debugging; stop it with
#                `docker compose -p rt-e2e down -v`
#
# Env (all optional):
#   IMAGE            image tag to build and run (default: rt:e2e, so the dev image rt:dev is untouched)
#   E2E_PROJECT      compose project name (default: rt-e2e; own network and data volume)
#   E2E_TIMEOUT      seconds to wait for /healthz and /readyz (default: 120)
#   E2E_LOG_DIR      where logs are kept on failure (default: target/e2e)
#   RT_ROOT_TOKEN, RT_MASTER_KEY   fixed secrets; random ones are generated for each run by default
#   RT_PUBLIC_PORT, RT_CONTROL_PORT, RT_METRICS_PORT   host ports (defaults 8080, 8081, 9090)
#
# tests/e2e/run_all.sh gets: RT_API (control), RT_INVOKE (public), RT_METRICS, RT_ROOT_TOKEN; other
# RT_E2E_* variables pass through. Needs docker (compose v2), curl, jq.
# On failure the container logs are saved to $E2E_LOG_DIR/<project>-<timestamp>.log before teardown.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

die() { echo "error: $*" >&2; exit 1; }
usage() { sed -n '2,/^set -euo/{/^set -euo/d;s/^# \{0,1\}//;p;}' "${BASH_SOURCE[0]}"; }

BUILD=1 KEEP=0
TEST_ARGS=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)  usage; exit 0 ;;
    --no-build) BUILD=0 ;;
    --keep)     KEEP=1 ;;
    --)         shift; TEST_ARGS=("$@"); break ;;
    *)          die "unknown option '$1' (see --help)" ;;
  esac
  shift
done

RUNNER="tests/e2e/run_all.sh"
for bin in docker curl jq; do
  command -v "$bin" >/dev/null 2>&1 || die "'$bin' is required but not installed"
done
docker compose version >/dev/null 2>&1 || die "docker compose v2 is required (docker compose version failed)"
docker info >/dev/null 2>&1 || die "cannot reach the docker daemon (is Docker running?)"
[[ -f "$RUNNER" ]] || die "$RUNNER not found: the E2E suite lives in tests/e2e/"

export IMAGE="${IMAGE:-rt:e2e}"
export RT_IMAGE="$IMAGE"
PROJECT="${E2E_PROJECT:-rt-e2e}"
TIMEOUT="${E2E_TIMEOUT:-120}"
LOG_DIR="${E2E_LOG_DIR:-target/e2e}"
PUBLIC_PORT="${RT_PUBLIC_PORT:-8080}" CONTROL_PORT="${RT_CONTROL_PORT:-8081}" METRICS_PORT="${RT_METRICS_PORT:-9090}"
export RT_PUBLIC_PORT="$PUBLIC_PORT" RT_CONTROL_PORT="$CONTROL_PORT" RT_METRICS_PORT="$METRICS_PORT"

rand_hex32() {
  if command -v openssl >/dev/null 2>&1; then openssl rand -hex 32
  else head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n'; fi
}
export RT_ROOT_TOKEN="${RT_ROOT_TOKEN:-$(rand_hex32)}"
export RT_MASTER_KEY="${RT_MASTER_KEY:-$(rand_hex32)}"
[[ "$RT_ROOT_TOKEN" != change-me* && "$RT_MASTER_KEY" != change-me* ]] \
  || die "RT_ROOT_TOKEN / RT_MASTER_KEY still hold the .env.example placeholders"

# tests/e2e/docker-compose.e2e.yml mounts tests/e2e/rt.e2e.toml (the `pro` plan of the E2E tenants).
dc() { docker compose -p "$PROJECT" -f "$ROOT/docker-compose.yml" -f "$ROOT/tests/e2e/docker-compose.e2e.yml" "$@"; }

# Never leave this run's stack behind unless asked to, and never lose the logs of a failed run.
rc=0
cleanup() {
  local code=$?
  [[ $code -ne 0 ]] && rc=$code
  trap - EXIT INT TERM
  if [[ $rc -ne 0 ]]; then
    mkdir -p "$LOG_DIR"
    local log
    log="$LOG_DIR/$PROJECT-$(date +%Y%m%d-%H%M%S).log"
    dc logs --no-color --timestamps > "$log" 2>&1 || true
    echo >&2
    echo "e2e FAILED (exit $rc). Container logs kept in $log; last lines:" >&2
    tail -n 30 "$log" >&2 || true
  fi
  if [[ "$KEEP" == 1 ]]; then
    echo "e2e: --keep given, stack left running (project $PROJECT). Stop: docker compose -p $PROJECT down -v" >&2
  else
    dc down --volumes --remove-orphans >/dev/null 2>&1 || true
  fi
  exit "$rc"
}
trap cleanup EXIT
trap 'rc=130; exit 130' INT
trap 'rc=143; exit 143' TERM

if [[ "$BUILD" == 1 ]]; then
  echo "==> docker build ($IMAGE)"
  scripts/docker-build.sh
else
  docker image inspect "$IMAGE" >/dev/null 2>&1 || die "image $IMAGE not found (drop --no-build or run scripts/docker-build.sh)"
fi

# A stale stack from an interrupted run would hold the ports and the old data volume.
dc down --volumes --remove-orphans >/dev/null 2>&1 || true

echo "==> docker compose up ($PROJECT)"
dc up -d --no-build

echo "==> waiting for http://127.0.0.1:$METRICS_PORT/healthz and /readyz (max ${TIMEOUT}s)"
deadline=$(( $(date +%s) + TIMEOUT ))
until curl -fsS -o /dev/null --max-time 2 "http://127.0.0.1:$METRICS_PORT/healthz" 2>/dev/null \
   && curl -fsS -o /dev/null --max-time 2 "http://127.0.0.1:$METRICS_PORT/readyz" 2>/dev/null; do
  cid="$(dc ps -aq rt 2>/dev/null | head -n1 || true)"
  state="$([[ -n "$cid" ]] && docker inspect -f '{{.State.Status}}' "$cid" 2>/dev/null || echo unknown)"
  case "$state" in exited|dead) rc=1; die "container $state before becoming healthy" ;; esac
  if [[ $(date +%s) -ge $deadline ]]; then rc=1; die "node not healthy after ${TIMEOUT}s"; fi
  sleep 1
done
echo "    node is up"

echo "==> $RUNNER"
export RT_API="http://127.0.0.1:$CONTROL_PORT"
export RT_INVOKE="http://127.0.0.1:$PUBLIC_PORT"
export RT_METRICS="http://127.0.0.1:$METRICS_PORT"
rc=0
bash "$RUNNER" ${TEST_ARGS[@]+"${TEST_ARGS[@]}"} || rc=$?
if [[ $rc -eq 0 ]]; then echo "e2e passed"; fi
exit "$rc"
