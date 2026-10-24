#!/usr/bin/env bash
# Print the id (svc_...) of the service with the given slug.
#
#   svc-id.sh <slug>      exit 0 + id on stdout, exit 1 if there is no such service
#
# One call: `GET /v1/services?slug=<slug>` (exact filter, 0 or 1 item; contracts C-9,
# docs/api.md), no paging.
#
# Env: RT_API (default http://127.0.0.1:8081), RT_KEY (Bearer token: root token
#      or a tenant key with services:read; may be empty against a debug build).
set -euo pipefail

die() { echo "error: $*" >&2; exit 2; }

SLUG="${1:-}"
[[ -n "$SLUG" ]] || { echo "usage: ${0##*/} <slug>" >&2; exit 2; }

for bin in curl jq; do
  command -v "$bin" >/dev/null 2>&1 || die "'$bin' is required but not installed"
done

RT_API="${RT_API:-http://127.0.0.1:8081}"
RT_API="${RT_API%/}"
RT_KEY="${RT_KEY:-${RT_ROOT_TOKEN:-}}"

# Keep the token out of `ps`: hand it to curl through a config file descriptor.
auth_cfg() { [[ -n "$RT_KEY" ]] && printf 'header = "Authorization: Bearer %s"\n' "$RT_KEY" || true; }

TMP="$(mktemp)"
trap 'rm -f "$TMP"' EXIT

CODE="$(curl -sS -G -o "$TMP" -w '%{http_code}' --data-urlencode "slug=$SLUG" -K <(auth_cfg) "$RT_API/v1/services")" \
  || die "cannot reach the control API at $RT_API"
if [[ "$CODE" != 2* ]]; then
  MSG="$(jq -r '.error | "\(.code): \(.message)"' "$TMP" 2>/dev/null || true)"
  die "GET /v1/services?slug=$SLUG -> HTTP $CODE ${MSG:+($MSG)}"
fi

# The filter is exact, but keep the slug check so a node that ignores `slug` cannot return a wrong id.
ID="$(jq -r --arg s "$SLUG" '(.items // [])[] | select(.slug == $s) | .id' "$TMP" | head -n 1)"
if [[ -n "$ID" ]]; then
  echo "$ID"
  exit 0
fi
exit 1
