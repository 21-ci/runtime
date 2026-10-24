#!/usr/bin/env bash
# Build the runtime image from ./Dockerfile (multi-stage; needs BuildKit for the cargo cache mounts).
#
#   scripts/docker-build.sh [extra `docker build` args, e.g. --no-cache --platform linux/amd64]
#
# Env:
#   IMAGE   image tag (default: rt:dev)
#
# The first build downloads the V8 static library and compiles the whole workspace (20+ min on a
# laptop). Later builds reuse the cargo registry and target caches, which live in BuildKit's cache,
# not in the image: `docker builder prune` drops them.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

die() { echo "error: $*" >&2; exit 1; }
usage() { sed -n '2,/^set -euo/{/^set -euo/d;s/^# \{0,1\}//;p;}' "${BASH_SOURCE[0]}"; }

case "${1:-}" in -h|--help) usage; exit 0 ;; esac

command -v docker >/dev/null 2>&1 || die "docker is not installed"
docker info >/dev/null 2>&1 || die "cannot reach the docker daemon (is Docker running?)"
[[ -f Dockerfile ]] || die "no Dockerfile in $ROOT"

IMAGE="${IMAGE:-rt:dev}"
export DOCKER_BUILDKIT=1

LABELS=()
if command -v git >/dev/null 2>&1 && REV="$(git -C "$ROOT" rev-parse --short=12 HEAD 2>/dev/null)"; then
  LABELS+=(--label "org.opencontainers.image.revision=$REV")
fi

echo "==> docker build -t $IMAGE ."
docker build -t "$IMAGE" ${LABELS[@]+"${LABELS[@]}"} "$@" .
echo "built $IMAGE ($(docker image inspect -f '{{.Size}}' "$IMAGE" | awk '{printf "%.0f MB", $1/1048576}'))"
