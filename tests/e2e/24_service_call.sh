#!/usr/bin/env bash
# E2E 24: service-to-service calls (sample/deno/service-call): same-tenant call, background
# call under waitUntil, depth limit (508), cross-tenant refusal.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
setup_tenant

hello="$(uniq_slug e2e-callee)"
caller="$(uniq_slug e2e-caller)"
deploy "$SAMPLE_DIR/deno/hello" "$hello"
deploy "$SAMPLE_DIR/deno/service-call" "$caller"

step "A calls B in-process and returns B's response"
invoke "$caller" "/call?target=$hello&name=from-service"
expect_code 200 "GET /call"
expect_eq "$(jqr .caller)" "$caller" "caller"
expect_eq "$(jqr .status)" 200 "status of the callee"
expect_eq "$(jqr .response.message)" "Hello, from-service!" "the callee's response body"
expect_eq "$(jqr .response.basePath)" "/s/$hello" "the callee sees its own base path"
expect_eq "$(jqr .response.trigger)" call "the callee's trigger is call"

step "an unknown target is reported as a response, not a crash"
invoke "$caller" "/call?target=e2e-no-such-service"
expect_code 200 "caller survives"
expect_jq '.status == 404 or .status == 500' "unknown target status"

step "ctx.waitUntil: the response is immediate, the call finishes in the background"
invoke "$caller" "/background?target=$hello"
expect_code 202 "background call"
expect_eq "$(jqr .target)" "$hello" "target"

step "recursion is cut at depth 15 with 508 call_depth; every task finishes"
invoke "$caller" "/recurse"
expect_code 200 "recursive self-call"
jq -e '[.. | objects | select(.reason? == "call_depth")] | length == 1' "$BODY_FILE" >/dev/null || fail "no call_depth at the bottom of the chain"
jq -e '[.. | objects | select(.status? == 508)] | length == 1' "$BODY_FILE" >/dev/null || fail "no 508 at the bottom of the chain"
depth="$(jq '[.. | objects | select(has("depth")) | .depth] | max' "$BODY_FILE")"
[[ "$depth" -ge 14 && "$depth" -le 15 ]] || fail "deepest level answering is $depth, expected 14..15"
wait_sched_idle() {
  op GET "$RT_API/v1/node/scheduler"
  [[ "$(jqr .inflight)" == "0" ]]
}
WAIT=20 wait_until "no task left in flight after the chain" wait_sched_idle
invoke "$caller" "/call?target=$hello"
expect_code 200 "the node serves normally afterwards"

step "cross-tenant: a service of another tenant cannot be called"
# Second tenant with its own hello (the first tenant's key and cleanup stay in TENANT_*).
FIRST_TENANT_ID="$TENANT_ID"; FIRST_TENANT_KEY="$TENANT_KEY"
setup_tenant
OTHER_TENANT_ID="$TENANT_ID"
other="$(uniq_slug e2e-other)"
deploy "$SAMPLE_DIR/deno/hello" "$other"
invoke "$other" "/"
expect_code 200 "the other tenant's service works for its owner"
invoke "$caller" "/call?target=$other"
expect_code 200 "caller survives a refused call"
jq -e '.status == 403' "$BODY_FILE" >/dev/null || fail "calling another tenant's service must answer 403 (got: $(body))"
reason="$(jqr '.reason // empty')"
[[ -z "$reason" || "$reason" == "cross_tenant_call" ]] || fail "unexpected x-rt-reason '$reason' for a cross-tenant call"
# Clean the second tenant up explicitly; the EXIT trap deletes the last TENANT_ID only.
op DELETE "$RT_API/v1/tenants/$OTHER_TENANT_ID?purge=1"
TENANT_ID="$FIRST_TENANT_ID"; TENANT_KEY="$FIRST_TENANT_KEY"

pass
