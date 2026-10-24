#!/usr/bin/env bash
# E2E 31: a JS component compiled with `jco componentize` (sample/wasm/js-componentize).
#   RT_E2E_BUILD_WASM=1   build it with sample/wasm/js-componentize/build.sh when missing (needs node + npm)
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

wasm="$SAMPLE_DIR/wasm/js-componentize/dist/app.wasm"
if [[ ! -f "$wasm" && "${RT_E2E_BUILD_WASM:-0}" == "1" ]]; then
  step "building the component (build.sh)"
  bash "$SAMPLE_DIR/wasm/js-componentize/build.sh" >"$WORK/build.out" 2>&1 || { cat "$WORK/build.out" >&2; fail "building js-componentize failed"; }
fi
[[ -f "$wasm" ]] || skip "component not built ($wasm); run sample/wasm/js-componentize/build.sh or set RT_E2E_BUILD_WASM=1"
setup_tenant

# The Cranelift compile of the ~14 MB engine takes longer than `sched.queue_timeout` (a task that
# waits for a lazy build is shed after it), so the deploy asks for `compile=eager` and
# deploy.sh waits until the version is ready.
step "deploy the ~14 MB component (compile=eager: deploy.sh waits for the build)"
slug="$(uniq_slug e2e-jco)"
deploy "$wasm" "$slug" eager
id="$(svc_id "$slug")"
tkey GET "$RT_API/v1/services/$id"
expect_eq "$(jqr .runtime)" wasm "runtime"

step "serve it"
HTTP_TIMEOUT=180 invoke "$slug" "/?name=Tashkent"
expect_code 200 "GET /s/$slug/?name=Tashkent"
expect_eq "$(jqr .message)" "Hello from WASM component, Tashkent!" "message"
expect_eq "$(jqr .method)" GET "method"
expect_eq "$(jqr .basePath)" "/s/$slug" "x-rt-base-path reaches the component"
expect_eq "$(jqr .hitsInThisInstance)" 1 "hitsInThisInstance"
expect_eq "$(header x-rt-version)" 1 "x-rt-version"

step "warm requests are served from the compiled artifact"
for _ in 1 2 3; do
  invoke "$slug" "/"
  expect_code 200 "warm request"
  expect_eq "$(jqr .hitsInThisInstance)" 1 "fresh instance every time"
done
tkey GET "$RT_API/v1/services/$id/versions/1"
expect_eq "$(jqr .build_state)" ready "build_state"

pass
