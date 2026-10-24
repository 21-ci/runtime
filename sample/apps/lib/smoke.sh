#!/usr/bin/env bash
# Shared helpers for sample/apps/*/smoke.sh. Sourced, not run. Works with bash 3.2 (macOS).
#
# Env (the same variables as sample/scripts/*.sh and 21ci):
#   RT_API         control API base      (default http://127.0.0.1:8081)
#   RT_INVOKE      invoke listener base  (default http://127.0.0.1:8080)
#   RT_KEY         root token or tenant key rtk_… with services:read, services:write, deploy,
#                  env:write, secrets:write (+ queue:write for some smokes)
#   RT_COMPILE     lazy | eager (default eager here, so build errors fail the deploy step)
#   SMOKE_CLEANUP  1 = delete the services this smoke deployed when it ends
#
# Every request leaves the status in $CODE, the body in $BODY (a file) and the response
# headers in $HDRS (a file). Tokens and secrets are handed to curl through a config file on a
# pipe (-K <(...)) or a temp file, never on its command line, so they do not show up in `ps`.
set -euo pipefail

SMOKE_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SCRIPTS_DIR="$(cd "$SMOKE_LIB_DIR/../../scripts" && pwd)"

RT_API="${RT_API:-http://127.0.0.1:8081}"; RT_API="${RT_API%/}"
RT_INVOKE="${RT_INVOKE:-http://127.0.0.1:8080}"; RT_INVOKE="${RT_INVOKE%/}"
RT_KEY="${RT_KEY:-${RT_ROOT_TOKEN:-}}"
export RT_API RT_INVOKE RT_KEY

for _bin in curl jq openssl; do
  command -v "$_bin" >/dev/null 2>&1 || { echo "error: '$_bin' is required but not installed" >&2; exit 1; }
done

SMOKE_TMP="$(mktemp -d)"
chmod 700 "$SMOKE_TMP"
BODY="$SMOKE_TMP/body"
HDRS="$SMOKE_TMP/headers"
CODE=""
PASSED=0
FAILED=0
DEPLOYED_SLUGS=""

_smoke_exit() {
  local rc=$? s
  if [[ "${SMOKE_CLEANUP:-0}" == "1" && -n "$DEPLOYED_SLUGS" ]]; then
    for s in $DEPLOYED_SLUGS; do delete_service "$s" || true; done
  fi
  rm -rf "$SMOKE_TMP"
  exit "$rc"
}
trap _smoke_exit EXIT

# ------------------------------------------------------------------------------------ output
step() { printf '\n== %s\n' "$*"; }
ok()   { PASSED=$((PASSED + 1)); printf '  ok    %s\n' "$*"; }
fail() {
  FAILED=$((FAILED + 1))
  printf '  FAIL  %s\n' "$*" >&2
  if [[ -s "$BODY" ]]; then
    printf '        HTTP %s: %s\n' "$CODE" "$(head -c 400 "$BODY" | tr '\n' ' ')" >&2
  fi
}
die() { echo "error: $*" >&2; exit 1; }

# Print the summary; returns 1 when anything failed (call it last).
finish() {
  printf '\n%d passed, %d failed\n' "$PASSED" "$FAILED"
  [[ "$FAILED" -eq 0 ]]
}

rand_hex() { openssl rand -hex "${1:-16}"; }

# ------------------------------------------------------------------------------------ HTTP
_curl_cfg() {
  local h
  for h in "$@"; do
    h="${h//\\/\\\\}"
    h="${h//\"/\\\"}"
    printf 'header = "%s"\n' "$h"
  done
}

# _request METHOD URL DATA [HEADER...]
_request() {
  local method="$1" url="$2" data="$3"
  shift 3
  local args=(-sS -X "$method" -o "$BODY" -D "$HDRS" -w '%{http_code}' --max-time "${SMOKE_TIMEOUT:-45}")
  if [[ -n "$data" ]]; then
    printf '%s' "$data" > "$SMOKE_TMP/data"
    args+=(--data-binary "@$SMOKE_TMP/data")
  fi
  : > "$BODY"
  : > "$HDRS"
  CODE="$(curl "${args[@]}" -K <(_curl_cfg "$@") "$url" 2>"$SMOKE_TMP/curl.err")" || CODE="000"
}

# api METHOD PATH [JSON]: control API with RT_KEY.
api() {
  local method="$1" path="$2" data="${3-}"
  set --
  if [[ -n "$RT_KEY" ]]; then set -- "$@" "Authorization: Bearer $RT_KEY"; fi
  if [[ -n "$data" ]]; then set -- "$@" "Content-Type: application/json"; fi
  _request "$method" "$RT_API$path" "$data" "$@"
}

# root_api METHOD PATH [JSON]: control API with the operator token RT_ROOT_TOKEN (operator SDKs).
root_api() {
  local method="$1" path="$2" data="${3-}"
  [[ -n "${RT_ROOT_TOKEN:-}" ]] || die "RT_ROOT_TOKEN is required for $method $path (operator SDKs)"
  set -- "Authorization: Bearer $RT_ROOT_TOKEN"
  if [[ -n "$data" ]]; then set -- "$@" "Content-Type: application/json"; fi
  _request "$method" "$RT_API$path" "$data" "$@"
}

# publish_sdk NAME VERSION FILE [CHANNEL]: POST /v1/sdks (operator) with a TypeScript source.
publish_sdk() {
  local body
  body="$(jq -nc --arg n "$1" --arg v "$2" --arg c "${4-}" --rawfile src "$3" \
    '{name: $n, version: $v, content_type: "application/typescript", source: ($src | @base64)}
     + (if $c == "" then {} else {channel: $c} end)')"
  root_api POST /v1/sdks "$body"
}

# inv SLUG METHOD PATH [BODY [HEADER...]]: public invoke listener, /s/SLUG/PATH.
# A non-empty BODY is sent as application/json unless a Content-Type header is given.
inv() {
  local slug="$1" method="$2" path="$3" data="${4-}" h has_ct=0
  if [[ $# -ge 4 ]]; then shift 4; else shift "$#"; fi
  for h in "$@"; do
    case "$(printf '%s' "$h" | tr 'A-Z' 'a-z')" in content-type:*) has_ct=1 ;; esac
  done
  if [[ -n "$data" && "$has_ct" -eq 0 ]]; then set -- "$@" "Content-Type: application/json"; fi
  _request "$method" "$RT_INVOKE/s/$slug$path" "$data" "$@"
}

# ------------------------------------------------------------------------------------ asserts
# expect_code WANT LABEL   (WANT may list alternatives: "200|201")
expect_code() {
  if [[ "|$1|" == *"|$CODE|"* ]]; then ok "$2 -> $CODE"; else fail "$2: expected HTTP $1, got $CODE"; fi
}

# expect_jq FILTER LABEL   (jq -e on the last body)
expect_jq() {
  if jq -e "$1" "$BODY" >/dev/null 2>&1; then ok "$2"; else fail "$2 (jq: $1)"; fi
}

jget() { jq -r "$1" "$BODY"; }

# resp_header NAME: value of the last response header NAME (case-insensitive).
resp_header() {
  local name
  name="$(printf '%s' "$1" | tr 'A-Z' 'a-z')"
  tr -d '\r' < "$HDRS" | awk -v n="$name" 'index($0, ":") { k = tolower(substr($0, 1, index($0, ":") - 1)); if (k == n) { v = substr($0, index($0, ":") + 1); sub(/^ +/, "", v) } } END { print v }'
}

# retry_until SECONDS COMMAND...: re-run COMMAND every second until it succeeds or time is up.
retry_until() {
  local deadline=$(( $(date +%s) + $1 ))
  shift
  while :; do
    if "$@"; then return 0; fi
    if [[ "$(date +%s)" -ge "$deadline" ]]; then return 1; fi
    sleep 1
  done
}

# ------------------------------------------------------------------------------------ services
svc_id() { bash "$SCRIPTS_DIR/svc-id.sh" "$1"; }

# deploy SOURCE SLUG: create if missing, upload, activate (sample/scripts/deploy.sh).
deploy() {
  if RT_COMPILE="${RT_COMPILE:-eager}" bash "$SCRIPTS_DIR/deploy.sh" "$1" "$2" > "$SMOKE_TMP/deploy.log" 2>&1; then
    ok "deployed $2 ($(grep -m1 '^Version' "$SMOKE_TMP/deploy.log" | sed 's/^Version *: *//'))"
    DEPLOYED_SLUGS="$DEPLOYED_SLUGS $2"
  else
    cat "$SMOKE_TMP/deploy.log" >&2
    die "deploy of $2 failed"
  fi
}

_require_id() { svc_id "$1" || die "service $1 not found"; }

# configure SLUG JSON: PATCH /v1/services/{id} (each block given replaces that block).
configure() {
  local id
  id="$(_require_id "$1")"
  api PATCH "/v1/services/$id" "$2"
  if [[ "$CODE" == 2* ]]; then ok "configured $1: $2"; else fail "PATCH $1"; die "cannot configure $1"; fi
}

# set_env SLUG JSON_MAP: PUT /v1/services/{id}/env (replaces the whole map).
set_env() {
  local id
  id="$(_require_id "$1")"
  api PUT "/v1/services/$id/env" "$2"
  if [[ "$CODE" == 2* ]]; then ok "env of $1 set"; else fail "PUT env $1"; die "cannot set env of $1"; fi
}

# set_secret SLUG NAME VALUE: PUT /v1/services/{id}/secrets/{NAME} (value via stdin to jq).
set_secret() {
  local id body
  id="$(_require_id "$1")"
  body="$(printf '%s' "$3" | jq -Rsc '{value: .}')"
  api PUT "/v1/services/$id/secrets/$2" "$body"
  if [[ "$CODE" == 2* ]]; then ok "secret $2 set on $1"; else fail "PUT secret $2 on $1"; die "cannot set secret $2"; fi
}

delete_service() {
  local id
  id="$(svc_id "$1")" || return 0
  api DELETE "/v1/services/$id"
  printf '  cleanup: deleted %s (HTTP %s)\n' "$1" "$CODE"
}
