#!/usr/bin/env bash
# E2E 22: an npm: dependency resolved at build time (sample/deno/npm-zod). Needs internet access
# to the configured npm CDN (esm.sh); skipped without it.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

if ! have_internet; then
  skip "no internet access (esm.sh is not reachable)"
fi
setup_tenant

step "deploy sample/deno/npm-zod (rt.json import map: zod -> npm:zod@3); the build happens on the first request"
slug="$(uniq_slug e2e-zod)"
deploy "$SAMPLE_DIR/deno/npm-zod" "$slug"
id="$(svc_id "$slug")"
tkey GET "$RT_API/v1/services/$id/versions/1"
expect_code 200 "version before the first request"
expect_eq "$(jqr .build_state)" pending "lazy build: pending until the first task"

step "first request compiles (fetches zod through the CDN) and validates input"
HTTP_TIMEOUT=120
invoke_json POST "$slug" "/" '{"title":"write tests","priority":"high"}'
expect_code 201 "valid todo"
expect_eq "$(jqr .message)" valid "message"
expect_eq "$(jqr .todo.priority)" high "priority"
invoke_json POST "$slug" "/" '{"title":"x"}'
expect_code 201 "default priority"
expect_eq "$(jqr .todo.priority)" medium "zod default applied"
invoke_json POST "$slug" "/" '{"title":""}'
expect_code 400 "invalid todo"
expect_jq '.issues | length >= 1' "zod issues are reported"
invoke "$slug" "/"
expect_code 405 "GET is not allowed"
expect_eq "$(header allow)" POST "allow header"

step "the build is recorded: ready, with a lock digest"
tkey GET "$RT_API/v1/services/$id/versions/1"
expect_eq "$(jqr .build_state)" ready "build_state after the first request"
expect_jq '.lock_digest | type == "string" and startswith("sha256:")' "rt.lock digest"

step "no network at run time: later requests are fast"
start=$SECONDS
for _ in 1 2 3 4 5; do
  invoke_json POST "$slug" "/" '{"title":"again"}'
  expect_code 201 "warm request"
done
(( SECONDS - start < 20 )) || fail "5 warm requests took $((SECONDS - start)) s"

pass
