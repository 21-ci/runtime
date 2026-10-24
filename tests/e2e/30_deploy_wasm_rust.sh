#!/usr/bin/env bash
# E2E 30: a Rust wasi:http component (sample/wasm/rust-hello) (E2E-02): deploy, serve, no warm
# state, stdout in the service logs, rejection of artifacts that are not valid components.
#   RT_E2E_BUILD_WASM=1   build the component with sample/wasm/rust-hello/build.sh when missing
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

wasm="$SAMPLE_DIR/wasm/rust-hello/target/wasm32-wasip2/release/rust_hello_wasm.wasm"
if [[ ! -f "$wasm" && "${RT_E2E_BUILD_WASM:-0}" == "1" ]]; then
  step "building the component (build.sh)"
  bash "$SAMPLE_DIR/wasm/rust-hello/build.sh" >"$WORK/build.out" 2>&1 || { cat "$WORK/build.out" >&2; fail "building rust-hello failed"; }
fi
[[ -f "$wasm" ]] || skip "component not built ($wasm); run sample/wasm/rust-hello/build.sh or set RT_E2E_BUILD_WASM=1"
setup_tenant

step "deploy the component (deploy.sh creates a wasm service)"
slug="$(uniq_slug e2e-rust)"
deploy "$wasm" "$slug"
id="$(svc_id "$slug")"
tkey GET "$RT_API/v1/services/$id"
expect_eq "$(jqr .runtime)" wasm "runtime"

step "first request"
HTTP_TIMEOUT=120 invoke "$slug" "/?name=Ferris"
expect_code 200 "GET /s/$slug/?name=Ferris"
expect_eq "$(jqr .message)" "Hello from Rust WASM component, Ferris!" "message"
expect_eq "$(jqr .path)" "/" "path"
expect_eq "$(jqr .method)" GET "method"
expect_eq "$(header x-rt-version)" 1 "x-rt-version"
task="$(header x-rt-task-id)"
[[ "$task" == task_* ]] || fail "x-rt-task-id '$task'"

step "a fresh instance per request: no state between requests (WSM-04)"
for _ in 1 2 3; do
  invoke "$slug" "/"
  expect_code 200 "request"
  expect_eq "$(jqr .hitsInThisInstance)" 1 "hitsInThisInstance"
done

step "stdout of the component lands in the service logs with the task id (WSM-07)"
logs_have_line() {
  tkey GET "$RT_API/v1/services/$id/logs?task_id=$task"
  [[ "$HTTP_CODE" == "200" ]] && jq -e '[.items[] | select(.msg | startswith("hello: GET"))] | length >= 1' "$BODY_FILE" >/dev/null
}
WAIT=15 wait_until "the component's stdout line in the logs" logs_have_line

step "artifacts that are not valid components are rejected (422 invalid_artifact)"
printf '\000asm\001\000\000\000' > "$WORK/core.wasm"
bad="$(create_service "$(uniq_slug e2e-badwasm)" wasm)"
upload "$bad" application/wasm "@$WORK/core.wasm"
expect_error_code 422 invalid_artifact "a core module without incoming-handler"
tkey GET "$RT_API/v1/services/$bad/versions"
expect_jq '(if type == "array" then . else .items end) | length == 0' "no version was stored for the invalid artifact"

pass
