#!/usr/bin/env bash
# Stop and remove the container started by scripts/docker-run.sh. Safe to run when nothing is running.
#
#   scripts/docker-stop.sh [--purge]
#
#   --purge   also delete the data volume (all tenants, services, artifacts, sealed secrets)
#
# Env: CONTAINER (default rt-dev), VOLUME (default rt-dev-data)
set -euo pipefail

die() { echo "error: $*" >&2; exit 1; }
usage() { sed -n '2,/^set -euo/{/^set -euo/d;s/^# \{0,1\}//;p;}' "${BASH_SOURCE[0]}"; }

PURGE=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help) usage; exit 0 ;;
    --purge)   PURGE=1 ;;
    *)         die "unknown option '$1' (see --help)" ;;
  esac
  shift
done

command -v docker >/dev/null 2>&1 || die "docker is not installed"
docker info >/dev/null 2>&1 || die "cannot reach the docker daemon (is Docker running?)"

CONTAINER="${CONTAINER:-rt-dev}"
VOLUME="${VOLUME:-rt-dev-data}"

if docker container inspect "$CONTAINER" >/dev/null 2>&1; then
  # SIGTERM first: the node drains in-flight requests (up to the stop timeout).
  docker stop --time 30 "$CONTAINER" >/dev/null
  docker rm "$CONTAINER" >/dev/null
  echo "stopped and removed $CONTAINER"
else
  echo "$CONTAINER is not running"
fi

if [[ "$PURGE" == 1 ]]; then
  if docker volume inspect "$VOLUME" >/dev/null 2>&1; then
    docker volume rm "$VOLUME" >/dev/null
    echo "deleted volume $VOLUME"
  else
    echo "volume $VOLUME does not exist"
  fi
fi
