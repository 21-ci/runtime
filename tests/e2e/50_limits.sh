#!/usr/bin/env bash
# E2E 50: guest failures and limits map to the documented statuses (docs/api.md):
# guest_error 500, cpu_limit 502, oom 502, timeout 504, plus plan ceilings (422 limit_exceeds_plan).
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
setup_tenant

limits='{"limits":{"cpu_ms":200,"wall_ms":2000,"memory_mb":64}}'

mk() {  # mk NAME SOURCE-FILE [LIMITS-JSON] -> slug of an activated service with tight limits
  local slug id
  slug="$(uniq_slug "e2e-$1")"
  id="$(create_service "$slug" deno "${3:-$limits}")"
  upload "$id" application/typescript "@$2"
  expect_code 201 "upload $1"
  printf '%s' "$slug"
}

cat > "$WORK/throw.ts" <<'TS'
export default function (): Response { throw new Error("boom-secret-e2e"); }
TS
cat > "$WORK/cpu.ts" <<'TS'
export default function (): Response { while (true) {} }
TS
cat > "$WORK/oom.ts" <<'TS'
export default function (): Response { const a: number[][] = []; while (true) a.push(new Array(1_000_000).fill(1)); }
TS
cat > "$WORK/sleep.ts" <<'TS'
export default async function (): Promise<Response> { await new Promise((r) => setTimeout(r, 60000)); return new Response("late"); }
TS
cat > "$WORK/ok.ts" <<'TS'
export default () => new Response("still-fine");
TS

throw_slug="$(mk throw "$WORK/throw.ts")"
cpu_slug="$(mk cpu "$WORK/cpu.ts")"
# Filling a 64 MB heap takes a few hundred ms of CPU (V8 collects hard before it reports the
# limit), so the oom service gets a CPU budget well above that: the memory limit must be the one that fires.
oom_slug="$(mk oom "$WORK/oom.ts" '{"limits":{"cpu_ms":1000,"wall_ms":5000,"memory_mb":64}}')"
sleep_slug="$(mk sleep "$WORK/sleep.ts")"
ok_slug="$(mk ok "$WORK/ok.ts")"

step "a throwing handler is 500 guest_error with a generic body (the message stays in the logs)"
invoke "$throw_slug" "/"
expect_code 500 "throwing handler"
expect_eq "$(header x-rt-reason)" guest_error "x-rt-reason"
if grep -q "boom-secret-e2e" "$BODY_FILE"; then fail "the guest's error message leaked to the caller"; fi
case "$(header content-type)" in text/plain*) ;; *) fail "guest errors are text/plain, got '$(header content-type)'" ;; esac

step "a CPU hog is killed at cpu_ms: 502 cpu_limit"
invoke "$cpu_slug" "/"
expect_code 502 "cpu hog"
expect_eq "$(header x-rt-reason)" cpu_limit "x-rt-reason"

step "allocating past memory_mb is 502 oom"
invoke "$oom_slug" "/"
expect_code 502 "oom"
expect_eq "$(header x-rt-reason)" oom "x-rt-reason"

step "sleeping past wall_ms is 504 timeout (killed near 2 s, not after 60 s)"
start=$SECONDS
invoke "$sleep_slug" "/"
expect_code 504 "timeout"
expect_eq "$(header x-rt-reason)" timeout "x-rt-reason"
(( SECONDS - start < 15 )) || fail "the wall limit was not enforced: took $((SECONDS - start)) s"

step "the node keeps serving after every kind of failure"
invoke "$ok_slug" "/"
expect_code 200 "healthy service"
expect_eq "$(body)" still-fine "body"
invoke "$throw_slug" "/"
expect_code 500 "the failing service still fails the same way"
HTTP_TOKEN="" http GET "$RT_METRICS/readyz"
expect_code 200 "/readyz"

step "failures are visible as system log records for the developer"
tkey GET "$RT_API/v1/services/$(svc_id "$cpu_slug")/logs?kind=system"
expect_code 200 "system logs"

step "values above the plan ceiling are rejected, never clamped (422 limit_exceeds_plan)"
tkey POST "$RT_API/v1/services" "$(jq -nc --arg s "$(uniq_slug e2e-big)" '{slug:$s, runtime:"deno", limits:{memory_mb:1000000}}')"
expect_error_code 422 limit_exceeds_plan "memory_mb far above the plan"
tkey POST "$RT_API/v1/services" "$(jq -nc --arg s "$(uniq_slug e2e-bigcpu)" '{slug:$s, runtime:"deno", limits:{cpu_ms:100000000}}')"
expect_error_code 422 limit_exceeds_plan "cpu_ms far above the plan"

pass
