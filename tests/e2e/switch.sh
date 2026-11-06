#!/usr/bin/env bash
# E2E switch: deploy v1, create a branch, push v2 to the branch, switch with a long request in
# flight: it finishes on v1, new requests run v2 and the drain closes. Rollback does the same the
# other way (docs/api.md, docs/deploy.md).
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
setup_tenant

SLEEP_MS=5000

source_of() {  # source_of LABEL: answers LABEL, after ?sleep=<ms> if asked
  cat <<TS
export default async (req: Request) => {
  const ms = Number(new URL(req.url).searchParams.get("sleep") ?? 0);
  if (ms > 0) await new Promise((r) => setTimeout(r, ms));
  return new Response("$1");
};
TS
}

# The long request runs in the background with its own files: http() keeps one body per script.
start_long() {  # start_long NAME: sets LONG_PID
  curl -sS --max-time 30 -o "$WORK/$1.body" -D "$WORK/$1.headers" -w '%{http_code}' \
    "$RT_INVOKE/s/$slug/?sleep=$SLEEP_MS" > "$WORK/$1.code" &
  LONG_PID=$!
}
long_header() {  # long_header NAME HEADER
  awk -v want="$2" 'tolower($1) == want ":" { sub(/\r$/, "", $2); print $2 }' "$WORK/$1.headers"
}
expect_long_finished() {  # expect_long_finished NAME BODY VERSION
  wait "$LONG_PID" || fail "$1: the in-flight request failed"
  expect_eq "$(cat "$WORK/$1.code")" 200 "$1: status of the in-flight request"
  expect_eq "$(cat "$WORK/$1.body")" "$2" "$1: body of the in-flight request"
  expect_eq "$(long_header "$1" x-rt-version)" "$3" "$1: x-rt-version of the in-flight request"
}
expect_still_running() {  # expect_still_running what
  kill -0 "$LONG_PID" 2>/dev/null || fail "$1: the long request ended too early, the test proves nothing"
}
expect_serves() {  # expect_serves BODY VERSION what
  invoke "$slug" "/"
  expect_code 200 "$3"
  expect_eq "$(body)" "$1" "$3: body"
  expect_eq "$(header x-rt-version)" "$2" "$3: x-rt-version"
}
# Until the drain closes, a switch counts the old request.
switch_state() {  # switch_state ID -> sets the response
  tkey GET "$RT_API/v1/services/$id/switches/$1"
  expect_code 200 "get switch"
}
expect_draining_one() {  # expect_draining_one SWITCH_ID what
  has_inflight() { switch_state "$1"; jqe '.state == "draining" and .inflight_old == 1'; }
  WAIT=3 wait_until "$2: state draining with inflight_old 1" has_inflight "$1"
}
expect_drain_closed() {  # expect_drain_closed SWITCH_ID what
  is_completed() { switch_state "$1"; jqe '.state == "completed" and .inflight_old == 0'; }
  WAIT=10 wait_until "$2: state completed" is_completed "$1"
}

slug="$(uniq_slug e2e-sw)"
id="$(create_service "$slug" deno)"

step "v1 is live on the parent"
source_of v1 > "$WORK/v1.ts"
upload "$id" application/typescript "@$WORK/v1.ts"
expect_code 201 "upload v1"
expect_serves v1 1 "parent before the switch"

step "create a branch and push v2 to it"
tkey POST "$RT_API/v1/services/$id/branches" '{"name":"next"}'
expect_code 201 "create branch"
branch_slug="$(jqr .slug)"
branch_id="$(jqr .id)"
expect_eq "$branch_slug" "$slug-br-next" "branch slug"
source_of v2 > "$WORK/v2.ts"
upload "$branch_id" application/typescript "@$WORK/v2.ts"
expect_code 201 "upload to the branch"
expect_eq "$(jqr .version)" 1 "the branch has its own version numbers"

step "the branch serves v2 on its own URL, the parent stays on v1"
invoke "$branch_slug" "/"
expect_code 200 "branch request"
expect_eq "$(body)" v2 "branch body"
expect_eq "$(header x-robots-tag)" noindex "a branch is not indexed"
expect_serves v1 1 "parent while the branch is up"

step "a dry run changes nothing"
tkey POST "$RT_API/v1/services/$id/switch" '{"from":{"branch":"next"},"dry_run":true}'
expect_code 200 "dry run"
expect_serves v1 1 "parent after the dry run"

step "switch with a long request in flight on v1"
start_long old
sleep 1
expect_still_running "before the switch"
tkey POST "$RT_API/v1/services/$id/switch" '{"from":{"branch":"next"}}'
expect_code 202 "switch"
switch_id="$(jqr .id)"
expect_eq "$(jqr .from_version)" 1 "from_version"
expect_eq "$(jqr .to_version)" 2 "to_version is the adopted number on the parent"
expect_draining_one "$switch_id" "switch"

step "new requests run v2 while the old one is still running on v1"
expect_serves v2 2 "parent after the switch"
expect_still_running "right after the switch"

step "the old request finishes on v1 and the drain closes"
expect_long_finished old v1 1
expect_drain_closed "$switch_id" "switch"
expect_serves v2 2 "parent after the drain"

step "the adopted version has the digest of the branch version"
tkey GET "$RT_API/v1/services/$branch_id/versions/1"
expect_code 200 "branch version"
branch_digest="$(jqr .source_digest)"
[[ -n "$branch_digest" && "$branch_digest" != null ]] || fail "branch version has no source_digest"
tkey GET "$RT_API/v1/services/$id/versions/2"
expect_code 200 "adopted version"
expect_eq "$(jqr .source_digest)" "$branch_digest" "adopted digest"

step "rollback with a long request in flight on v2"
start_long new
sleep 1
expect_still_running "before the rollback"
tkey POST "$RT_API/v1/services/$id/switches/$switch_id/rollback"
expect_code 202 "rollback"
rollback_id="$(jqr .id)"
expect_eq "$(jqr .kind)" rollback "kind"
expect_eq "$(jqr .from_version)" 2 "rollback from_version"
expect_eq "$(jqr .to_version)" 1 "rollback to_version"
expect_draining_one "$rollback_id" "rollback"
expect_serves v1 1 "parent after the rollback"
expect_still_running "right after the rollback"
expect_long_finished new v2 2
expect_drain_closed "$rollback_id" "rollback"
expect_serves v1 1 "parent after the rollback drain"

step "the events tell the story"
tkey GET "$RT_API/v1/services/$id/events"
expect_code 200 "events"
for kind in service.switch.started service.switch.flipped service.switch.drained; do
  expect_jq "[.items[].kind] | index(\"$kind\") != null" "event $kind"
done

pass
