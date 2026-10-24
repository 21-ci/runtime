#!/usr/bin/env bash
# Run the runtime image as a detached container on loopback ports, then wait until it is healthy.
#
#   scripts/docker-run.sh [--no-wait]
#
# Idempotent: an existing container with the same name is replaced (its data volume is kept).
# Stop it with scripts/docker-stop.sh, follow logs with `make docker-logs`.
#
# Secrets: RT_ROOT_TOKEN and RT_MASTER_KEY are taken from the environment, else from ./.env, else
# generated (openssl rand -hex 32) and appended to ./.env (git-ignored, mode 600). Keep the master
# key: the data volume holds secrets sealed with it.
# Config: ./rt.toml (or $RT_CONFIG_FILE) is mounted read-only at /etc/rt/rt.toml when present;
# otherwise the image uses the built-in defaults (profile `balanced`).
#
# Env (all optional):
#   IMAGE            image tag (default: rt:dev; build it with scripts/docker-build.sh)
#   CONTAINER        container name (default: rt-dev)
#   VOLUME           data volume (default: rt-dev-data, mounted at /data)
#   RT_PUBLIC_PORT, RT_CONTROL_PORT, RT_METRICS_PORT   host ports, always bound to 127.0.0.1 (8080, 8081, 9090)
#   RT_LOG           log filter inside the container (default: info; RUST_LOG is the fallback)
#   RT_CPUS, RT_MEMORY   optional limits, e.g. RT_CPUS=4 RT_MEMORY=2g = the reference box of the PRF-* criteria
#   RT_WAIT          seconds to wait for /healthz (default: 90)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

die() { echo "error: $*" >&2; exit 1; }
usage() { sed -n '2,/^set -euo/{/^set -euo/d;s/^# \{0,1\}//;p;}' "${BASH_SOURCE[0]}"; }

WAIT=1
while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help) usage; exit 0 ;;
    --no-wait) WAIT=0 ;;
    *)         die "unknown option '$1' (see --help)" ;;
  esac
  shift
done

for bin in docker curl; do
  command -v "$bin" >/dev/null 2>&1 || die "'$bin' is required but not installed"
done
docker info >/dev/null 2>&1 || die "cannot reach the docker daemon (is Docker running?)"

IMAGE="${IMAGE:-rt:dev}"
CONTAINER="${CONTAINER:-rt-dev}"
VOLUME="${VOLUME:-rt-dev-data}"
PUBLIC_PORT="${RT_PUBLIC_PORT:-8080}" CONTROL_PORT="${RT_CONTROL_PORT:-8081}" METRICS_PORT="${RT_METRICS_PORT:-9090}"
WAIT_SECS="${RT_WAIT:-90}"

docker image inspect "$IMAGE" >/dev/null 2>&1 || die "image $IMAGE not found: run 'make docker-build' first"

rand_hex32() {
  if command -v openssl >/dev/null 2>&1; then openssl rand -hex 32
  else head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n'; fi
}

# dotenv_get FILE KEY: value of KEY= in a .env file (first match, optional surrounding quotes stripped).
dotenv_get() {
  [[ -f "$1" ]] || return 0
  local line
  line="$(grep -E "^[[:space:]]*(export[[:space:]]+)?$2=" "$1" | head -n1 || true)"
  line="${line#*=}"
  line="${line%\"}"; line="${line#\"}"; line="${line%\'}"; line="${line#\'}"
  printf '%s' "$line"
}

# resolve_secret NAME: env > .env > freshly generated (appended to .env).
resolve_secret() {
  local name="$1" val="${!1:-}"
  [[ -n "$val" ]] || val="$(dotenv_get .env "$name")"
  if [[ -z "$val" ]]; then
    val="$(rand_hex32)"
    ( umask 077; touch .env; printf '%s=%s\n' "$name" "$val" >> .env )
    echo "generated $name into .env (git-ignored)" >&2
  fi
  [[ "$val" != change-me* ]] || die "$name still holds the .env.example placeholder; set a real value (openssl rand -hex 32)"
  printf -v "$name" '%s' "$val"
  export "${name?}"
}
resolve_secret RT_ROOT_TOKEN
resolve_secret RT_MASTER_KEY
[[ "$RT_MASTER_KEY" =~ ^[0-9a-fA-F]{64}$ ]] || die "RT_MASTER_KEY must be 64 hex characters (openssl rand -hex 32)"

MOUNTS=(-v "$VOLUME:/data")
LIMITS=()
[[ -z "${RT_CPUS:-}" ]] || LIMITS+=(--cpus "$RT_CPUS")
[[ -z "${RT_MEMORY:-}" ]] || LIMITS+=(--memory "$RT_MEMORY")
CONFIG_FILE="${RT_CONFIG_FILE:-$ROOT/rt.toml}"
if [[ -f "$CONFIG_FILE" ]]; then
  MOUNTS+=(-v "$(cd "$(dirname "$CONFIG_FILE")" && pwd)/$(basename "$CONFIG_FILE"):/etc/rt/rt.toml:ro")
  echo "config: mounting $CONFIG_FILE at /etc/rt/rt.toml" >&2
else
  echo "config: no rt.toml, using the image's built-in defaults (profile balanced)" >&2
fi

if docker container inspect "$CONTAINER" >/dev/null 2>&1; then
  echo "replacing existing container $CONTAINER (volume $VOLUME is kept)" >&2
  docker rm -f "$CONTAINER" >/dev/null
fi

# `-e NAME` without a value forwards it from this shell: secrets never show up in `ps` or the script output.
docker run -d --name "$CONTAINER" --init --restart unless-stopped \
  -p "127.0.0.1:$PUBLIC_PORT:8080" \
  -p "127.0.0.1:$CONTROL_PORT:8081" \
  -p "127.0.0.1:$METRICS_PORT:9090" \
  -e RT_ROOT_TOKEN -e RT_MASTER_KEY -e "RT_LOG=${RT_LOG:-${RUST_LOG:-info}}" \
  --cap-drop ALL --security-opt no-new-privileges:true \
  --ulimit nofile=65536:65536 \
  --stop-timeout 30 \
  ${LIMITS[@]+"${LIMITS[@]}"} \
  "${MOUNTS[@]}" \
  "$IMAGE" >/dev/null

if [[ "$WAIT" == 1 ]]; then
  echo "waiting for http://127.0.0.1:$METRICS_PORT/healthz (max ${WAIT_SECS}s)" >&2
  deadline=$(( $(date +%s) + WAIT_SECS ))
  until curl -fsS -o /dev/null --max-time 2 "http://127.0.0.1:$METRICS_PORT/healthz" 2>/dev/null; do
    state="$(docker inspect -f '{{.State.Status}}' "$CONTAINER" 2>/dev/null || echo gone)"
    if [[ "$state" != running && "$state" != restarting ]] || [[ $(date +%s) -ge $deadline ]]; then
      echo "error: $CONTAINER is not healthy (state: $state). Last log lines:" >&2
      docker logs --tail 30 "$CONTAINER" >&2 || true
      exit 1
    fi
    sleep 1
  done
fi

cat <<EOF
$CONTAINER is up (image $IMAGE, data volume $VOLUME)
  invoke   http://127.0.0.1:$PUBLIC_PORT/s/<slug>/...
  control  http://127.0.0.1:$CONTROL_PORT/v1/...   docs: http://127.0.0.1:$CONTROL_PORT/docs
  metrics  http://127.0.0.1:$METRICS_PORT/metrics /healthz /readyz
  token    RT_ROOT_TOKEN in .env (Authorization: Bearer <token>)
  logs     make docker-logs     stop: make docker-stop
EOF
