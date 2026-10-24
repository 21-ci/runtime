#!/usr/bin/env bash
# E2E 20: deploy Deno services (E2E-01): the hello sample through sample/scripts/deploy.sh and a
# one-liner through the raw API; version header, runtime headers, no warm state.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
setup_tenant

step "sample/deno/hello through deploy.sh"
slug="$(uniq_slug e2e-hello)"
deploy "$SAMPLE_DIR/deno/hello" "$slug"
id="$(svc_id "$slug")"
tkey GET "$RT_API/v1/services/$id"
expect_code 200 "get service"
expect_eq "$(jqr .runtime)" deno "runtime"
expect_eq "$(jqr .live_version)" 1 "live_version after the first deploy"
expect_eq "$(jqr .tenant_id)" "$TENANT_ID" "tenant_id"

step "first request: path stripping, base path, runtime headers"
invoke "$slug" "/x?name=Ada"
expect_code 200 "GET /s/$slug/x?name=Ada"
expect_eq "$(jqr .message)" "Hello, Ada!" "message"
expect_eq "$(jqr .path)" "/x" "the handler sees the stripped path"
expect_eq "$(jqr .basePath)" "/s/$slug" "x-rt-base-path"
expect_eq "$(jqr .method)" GET "method"
expect_eq "$(jqr .service.slug)" "$slug" "ctx.service.slug"
expect_eq "$(jqr .trigger)" http "ctx.trigger"
expect_eq "$(header x-rt-version)" 1 "x-rt-version"
task1="$(header x-rt-task-id)"
[[ "$task1" == task_* ]] || fail "x-rt-task-id '$task1' is not task_…"
expect_eq "$(jqr .taskId)" "$task1" "ctx.taskId equals x-rt-task-id"
[[ -n "$(header server-timing)" ]] || fail "missing server-timing header"

step "no warm state between requests: every task starts fresh"
invoke "$slug" "/"
expect_code 200 "second request"
expect_eq "$(jqr .hitsInThisTask)" 1 "hitsInThisTask on the second request"
[[ "$(header x-rt-task-id)" != "$task1" ]] || fail "two requests share a task id"

step "E2E-01: a one-line service through the raw API, activate=1, 201 pending"
slug2="$(uniq_slug e2e-hi)"
id2="$(create_service "$slug2" deno)"
printf 'export default () => new Response("hi")\n' > "$WORK/hi.ts"
upload "$id2" application/typescript "@$WORK/hi.ts"
expect_code 201 "upload"
expect_eq "$(jqr .version)" 1 "version"
expect_eq "$(jqr .build_state)" pending "build_state"
[[ "$(jqr .source_digest)" == sha256:* ]] || fail "source_digest is not sha256:…"
invoke "$slug2" "/"
expect_code 200 "first call after deploy"
expect_eq "$(body)" hi "response body"
expect_eq "$(header x-rt-version)" 1 "x-rt-version"

step "a service that does not exist, and one with a bad slug"
invoke "$slug-nope" "/"
expect_code 404 "unknown slug"
tkey POST "$RT_API/v1/services" '{"slug":"Not A Slug","runtime":"deno"}'
expect_error_code 422 validation_failed "invalid slug"
tkey POST "$RT_API/v1/services" "$(jq -nc --arg s "$slug" '{slug:$s, runtime:"deno"}')"
expect_error_code 409 slug_taken "duplicate slug"

pass
