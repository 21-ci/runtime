#!/usr/bin/env bash
# Shared helpers for the black-box E2E scripts (bash + curl + jq). Source it; do not run it.
#
#   source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
#
# Environment (all scripts):
#   RT_API          control listener            (default http://127.0.0.1:8081)
#   RT_INVOKE       public invoke listener      (default http://127.0.0.1:8080)
#   RT_METRICS      metrics/health listener     (default http://127.0.0.1:9090)
#   RT_ROOT_TOKEN   operator token              (required)
#   RT_E2E_PLAN     plan for the test tenants   (default pro; falls back to free)
#   HTTP_TIMEOUT    per-request curl timeout    (default 60 s)
#
# Every script ends with exactly one result line on stdout and an exit status:
#   PASS <name>            exit 0
#   SKIP <name>: <reason>  exit 0
#   FAIL <name>: <reason>  exit 1   (also for any unexpected command failure)
set -euo pipefail

: "${RT_API:=http://127.0.0.1:8081}"
: "${RT_INVOKE:=http://127.0.0.1:8080}"
: "${RT_METRICS:=http://127.0.0.1:9090}"
RT_API="${RT_API%/}"; RT_INVOKE="${RT_INVOKE%/}"; RT_METRICS="${RT_METRICS%/}"
export RT_API RT_INVOKE RT_METRICS

E2E_LIB_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_DIR="$(cd "$E2E_LIB_DIR/../.." && pwd)"
SAMPLE_DIR="$REPO_DIR/sample"
E2E_NAME="${E2E_NAME:-$(basename "${BASH_SOURCE[1]:-$0}" .sh)}"

WORK="$(mktemp -d)"
BODY_FILE="$WORK/body"
HEADERS_FILE="$WORK/headers"
HTTP_CODE=""
HTTP_TOKEN=""
HTTP_ARGS=()
CONTENT_TYPE=""
TENANT_ID=""
TENANT_KEY=""
E2E_DONE=""

for _bin in curl jq; do
  command -v "$_bin" >/dev/null 2>&1 || { echo "FAIL $E2E_NAME: '$_bin' is required but not installed" >&2; exit 1; }
done
if [[ -z "${RT_ROOT_TOKEN:-}" ]]; then
  echo "FAIL $E2E_NAME: RT_ROOT_TOKEN must be set" >&2
  exit 1
fi

# ------------------------------------------------------------------------------ results / cleanup

e2e_cleanup() {
  # Best effort: remove the tenant (and its services, data) created by setup_tenant.
  if [[ -n "$TENANT_ID" ]]; then
    curl -sS -o /dev/null --max-time 20 -X DELETE -K <(printf 'header = "Authorization: Bearer %s"\n' "$RT_ROOT_TOKEN") \
      "$RT_API/v1/tenants/$TENANT_ID?purge=1" >/dev/null 2>&1 || true
  fi
  rm -rf "$WORK"
}

e2e_on_exit() {
  local rc=$?
  if [[ -z "$E2E_DONE" ]]; then
    # An unexpected failure (set -e), or a fail() inside a command substitution (its subshell
    # leaves the reason in $WORK/fail_reason).
    local reason="aborted unexpectedly (exit status $rc)"
    [[ -s "$WORK/fail_reason" ]] && reason="$(cat "$WORK/fail_reason")"
    echo "FAIL $E2E_NAME: $reason"
    [[ $rc -eq 0 ]] && rc=1
  fi
  e2e_cleanup
  exit "$rc"
}
trap e2e_on_exit EXIT

pass() { E2E_DONE=1; echo "PASS $E2E_NAME"; exit 0; }
skip() { E2E_DONE=1; echo "SKIP $E2E_NAME: $*"; exit 0; }
fail() {
  # Prints the result line at top level; inside $(...) the reason is handed to the parent via a file.
  printf '%s' "$*" > "$WORK/fail_reason"
  echo "FAIL $E2E_NAME: $*" >&2
  if [[ -s "$BODY_FILE" ]]; then
    echo "  last response: HTTP ${HTTP_CODE:-?}" >&2
    head -c 1500 "$BODY_FILE" | sed 's/^/    /' >&2 || true
    echo >&2
  fi
  exit 1
}

step() { echo "  - $*"; }

# ------------------------------------------------------------------------------ HTTP

# http METHOD URL [BODY]      body: a string, or @file (sent as-is). Sets HTTP_CODE, $BODY_FILE, $HEADERS_FILE.
#   HTTP_TOKEN       bearer token (empty = none)
#   CONTENT_TYPE     request content type (default application/json when a body is given)
#   HTTP_ARGS        extra curl arguments (array)
http() {
  local method="$1" url="$2" body="${3-}"
  local args=(-sS -X "$method" -o "$BODY_FILE" -D "$HEADERS_FILE" -w '%{http_code}' --max-time "${HTTP_TIMEOUT:-60}")
  if [[ -n "$body" ]]; then
    args+=(-H "Content-Type: ${CONTENT_TYPE:-application/json}" --data-binary "$body")
  fi
  if [[ ${#HTTP_ARGS[@]} -gt 0 ]]; then
    args+=("${HTTP_ARGS[@]}")
  fi
  : > "$BODY_FILE"
  # The token goes through a config file descriptor so it never shows up in `ps`.
  HTTP_CODE="$(curl "${args[@]}" -K <(if [[ -n "$HTTP_TOKEN" ]]; then printf 'header = "Authorization: Bearer %s"\n' "$HTTP_TOKEN"; fi) "$url")" \
    || fail "cannot reach $url ($method)"
}

# control-plane call with the operator token / with a tenant key
op()   { HTTP_TOKEN="$RT_ROOT_TOKEN" http "$@"; }
tkey() { HTTP_TOKEN="$TENANT_KEY" http "$@"; }
# unauthenticated
anon() { HTTP_TOKEN="" http "$@"; }

body() { cat "$BODY_FILE"; }
jqr()  { jq -r "$1" "$BODY_FILE"; }
jqe()  { jq -e "$1" "$BODY_FILE" >/dev/null 2>&1; }

# response header (case-insensitive), last occurrence, trimmed
header() {
  awk -v want="$(printf '%s' "$1" | tr 'A-Z' 'a-z')" '
    BEGIN { FS=":" }
    { line=$0; sub(/\r$/, "", line); n=index(line, ":"); if (n>0) { k=tolower(substr(line,1,n-1)); v=substr(line,n+1); sub(/^[ \t]+/, "", v); if (k==want) val=v } }
    END { print val }' "$HEADERS_FILE"
}

# ------------------------------------------------------------------------------ assertions

expect_code() {  # expect_code CODE [what]
  [[ "$HTTP_CODE" == "$1" ]] || fail "${2:-request}: expected HTTP $1, got $HTTP_CODE"
}

expect_eq() {  # expect_eq ACTUAL EXPECTED what
  [[ "$1" == "$2" ]] || fail "$3: expected '$2', got '$1'"
}

expect_jq() {  # expect_jq FILTER what     (the filter must be truthy on the last response body)
  jqe "$1" || fail "${2:-$1}: jq filter '$1' is not true for the response"
}

expect_contains() {  # expect_contains HAYSTACK NEEDLE what
  [[ "$1" == *"$2"* ]] || fail "$3: '$2' not found in '${1:0:300}'"
}

expect_error_code() {  # expect_error_code HTTP CODE what
  expect_code "$1" "$3"
  local got
  got="$(jqr '.error.code // empty')"
  [[ "$got" == "$2" ]] || fail "$3: expected error code '$2', got '${got}'"
}

# retry CMD... until it succeeds (exit 0) or TIMEOUT seconds pass (env WAIT, default 30)
wait_until() {  # wait_until what CMD...
  local what="$1"; shift
  local deadline=$(( SECONDS + ${WAIT:-30} ))
  until "$@"; do
    (( SECONDS < deadline )) || fail "timed out waiting for: $what"
    sleep 0.5
  done
}

# ------------------------------------------------------------------------------ fixtures

uniq_slug() {  # uniq_slug prefix -> prefix-<random>; matches ^[a-z][a-z0-9-]{1,62}$
  printf '%s-%s%s' "$1" "$RANDOM" "$RANDOM"
}

# Create a tenant on $RT_E2E_PLAN (fallback: free) and a key with the given scopes (default: all).
# Sets TENANT_ID and TENANT_KEY; the tenant is deleted again when the script exits.
setup_tenant() {  # setup_tenant [scopes-json]
  local scopes="${1:-[\"services:read\",\"services:write\",\"deploy\",\"invoke\",\"logs:read\",\"env:write\",\"secrets:write\",\"queue:write\",\"data:admin\"]}"
  local plan="${RT_E2E_PLAN:-pro}"
  op POST "$RT_API/v1/tenants" "$(jq -nc --arg n "e2e-$E2E_NAME-$RANDOM" --arg p "$plan" '{name:$n, plan:$p}')"
  if [[ "$HTTP_CODE" == "422" && "$plan" != "free" ]]; then
    plan="free"
    op POST "$RT_API/v1/tenants" "$(jq -nc --arg n "e2e-$E2E_NAME-$RANDOM" --arg p "$plan" '{name:$n, plan:$p}')"
  fi
  expect_code 201 "create tenant"
  TENANT_ID="$(jqr .id)"
  [[ "$TENANT_ID" == ten_* ]] || fail "tenant id '$TENANT_ID' does not look like ten_…"
  op POST "$RT_API/v1/tenants/$TENANT_ID/keys" "$(jq -nc --argjson s "$scopes" '{scopes:$s}')"
  expect_code 201 "create key"
  TENANT_KEY="$(jqr .secret)"
  [[ "$TENANT_KEY" == rtk_* ]] || fail "key secret '${TENANT_KEY:0:8}…' does not look like rtk_…"
}

# deploy SOURCE SLUG [compile]   via sample/scripts/deploy.sh with the tenant key; prints nothing on success.
deploy() {
  local source="$1" slug="$2" compile="${3:-}"
  local out="$WORK/deploy.out"
  if ! RT_KEY="$TENANT_KEY" RT_COMPILE="$compile" bash "$SAMPLE_DIR/scripts/deploy.sh" "$source" "$slug" >"$out" 2>&1; then
    cat "$out" >&2
    fail "deploy.sh $source $slug failed"
  fi
}

svc_id() {  # svc_id SLUG -> service id (via GET /v1/services?slug=)
  tkey GET "$RT_API/v1/services?slug=$1"
  expect_code 200 "look up service $1"
  jq -er '.items[0].id' "$BODY_FILE" || fail "service $1 not found"
}

# create_service SLUG RUNTIME [extra-json-object]  -> prints the service id
create_service() {
  local body extra="${3:-}"
  # (not "${3:-{}}": bash 3.2 on macOS keeps the backslash of "${3:-{\}}" and jq gets "{\}")
  [[ -n "$extra" ]] || extra='{}'
  body="$(jq -nc --arg s "$1" --arg r "$2" --argjson x "$extra" '{slug:$s, runtime:$r} + $x')"
  tkey POST "$RT_API/v1/services" "$body"
  expect_code 201 "create service $1"
  jqr .id
}

# upload SVC_ID CONTENT_TYPE SOURCE|@file [query]   -> HTTP_CODE/body of the upload
upload() {
  local ct="$2" src="$3" query="${4:-activate=1}"
  CONTENT_TYPE="$ct" tkey POST "$RT_API/v1/services/$1/versions?$query" "$src"
}

# invoke SLUG [PATH] [curl args…]  -> GET on the public listener, same variables as http()
invoke() {
  local slug="$1" path="${2:-/}"
  shift $(( $# > 2 ? 2 : $# ))
  HTTP_ARGS=("$@")
  HTTP_TOKEN="" http GET "$RT_INVOKE/s/$slug$path"
  HTTP_ARGS=()
}

# invoke_json METHOD SLUG PATH [BODY]  -> request with a JSON body on the public listener
invoke_json() {
  HTTP_TOKEN="" http "$1" "$RT_INVOKE/s/$2$3" "${4-}"
}

# true when the machine has internet access (esm.sh reachable)
have_internet() {
  curl -sS -o /dev/null --max-time 5 -I https://esm.sh/ >/dev/null 2>&1
}
