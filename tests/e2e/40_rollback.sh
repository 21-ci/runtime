#!/usr/bin/env bash
# E2E 40: versions, rollback and auto-rollback (E2E-03/04/05 in miniature): activate new
# versions, flip `live` back with If-Match, reject failed versions, auto-revert a failing build.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
setup_tenant

ts_version() { printf 'export default () => new Response("v%s");\n' "$1"; }
upload_version() {  # upload_version N [query]
  ts_version "$1" > "$WORK/v$1.ts"
  upload "$id" application/typescript "@$WORK/v$1.ts" "${2:-activate=1}"
}
live_version() { tkey GET "$RT_API/v1/services/$id"; jqr .live_version; }

slug="$(uniq_slug e2e-roll)"
id="$(create_service "$slug" deno)"

step "v1, v2, v3 each activated on upload"
for n in 1 2 3; do
  upload_version "$n"
  expect_code 201 "upload v$n"
  expect_eq "$(jqr .version)" "$n" "version number"
  invoke "$slug" "/"
  expect_code 200 "request after v$n"
  expect_eq "$(body)" "v$n" "body"
  expect_eq "$(header x-rt-version)" "$n" "x-rt-version"
done
tkey GET "$RT_API/v1/services/$id/versions"
expect_code 200 "list versions"
expect_jq '(if type == "array" then . else .items end) | length == 3' "three retained versions"

step "rollback: PUT /live {version:1} with the current ETag"
tkey GET "$RT_API/v1/services/$id"
etag="$(header etag)"
[[ -n "$etag" ]] || fail "services carry an ETag header"
HTTP_ARGS=(-H "If-Match: $etag")
tkey PUT "$RT_API/v1/services/$id/live" '{"version":1}'
HTTP_ARGS=()
expect_code 200 "PUT /live"
expect_eq "$(jqr .live_version)" 1 "live_version after rollback"
invoke "$slug" "/"
expect_eq "$(body)" v1 "the next request is served by v1"
expect_eq "$(header x-rt-version)" 1 "x-rt-version after rollback"

step "a stale If-Match is rejected with 412 and changes nothing"
HTTP_ARGS=(-H "If-Match: $etag")
tkey PUT "$RT_API/v1/services/$id/live" '{"version":2}'
HTTP_ARGS=()
expect_error_code 412 precondition_failed "stale ETag"
expect_eq "$(live_version)" 1 "live stays on v1"

step "unknown versions are 404"
tkey PUT "$RT_API/v1/services/$id/live" '{"version":99}'
expect_code 404 "version 99"

step "roll forward to v3 again (no If-Match)"
tkey PUT "$RT_API/v1/services/$id/live" '{"version":3}'
expect_code 200 "PUT /live v3"
invoke "$slug" "/"
expect_eq "$(body)" v3 "served by v3"

step "a broken version (missing import) is accepted lazily and fails on its first request"
cat > "$WORK/v4.ts" <<'TS'
import { nothing } from "./missing.ts";
export default () => new Response(String(nothing));
TS
upload "$id" application/typescript "@$WORK/v4.ts" "activate=1"
expect_code 201 "upload v4"
expect_eq "$(jqr .version)" 4 "version number"
invoke "$slug" "/"
expect_code 502 "first request to the broken version"
expect_eq "$(header x-rt-reason)" build_failed "x-rt-reason"
case "$(header content-type)" in text/plain*) ;; *) fail "build failure must be text/plain" ;; esac

step "auto-rollback: live reverts to the newest healthy version and a rollback.auto event is recorded"
revert_done() { [[ "$(live_version)" == "3" ]]; }
WAIT=15 wait_until "live to revert to v3" revert_done
invoke "$slug" "/"
expect_code 200 "request after the auto-rollback"
expect_eq "$(body)" v3 "served by v3 again"
tkey GET "$RT_API/v1/services/$id/versions/4"
expect_eq "$(jqr .build_state)" failed "v4 build_state"
expect_jq '.failure != null' "failure details are recorded on the version"
tkey GET "$RT_API/v1/services/$id/events"
expect_code 200 "events"
expect_jq '.items | map(.kind) | index("rollback.auto") != null' "rollback.auto event"

step "a failed version cannot be made live (409 version_not_ready)"
tkey PUT "$RT_API/v1/services/$id/live" '{"version":4}'
expect_error_code 409 version_not_ready "PUT /live to a failed version"

step "a pending version can: upload without activating, then flip"
upload_version 5 "activate=0"
expect_code 201 "upload v5"
expect_eq "$(jqr .build_state)" pending "v5 is pending"
tkey PUT "$RT_API/v1/services/$id/live" '{"version":5}'
expect_code 200 "PUT /live to a pending version"
invoke "$slug" "/"
expect_eq "$(body)" v5 "v5 serves after its lazy build"

pass
