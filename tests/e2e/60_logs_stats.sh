#!/usr/bin/env bash
# E2E 60: logs (query + SSE follow, E2E-06), stats, events and Prometheus metrics for a service.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
setup_tenant

slug="$(uniq_slug e2e-logs)"
id="$(create_service "$slug" deno)"
cat > "$WORK/logger.ts" <<'TS'
export default function (req: Request): Response {
  console.log("e2e-log-line");
  console.error("e2e-error-line");
  return new Response("ok");
}
TS
upload "$id" application/typescript "@$WORK/logger.ts"
expect_code 201 "upload"
invoke "$slug" "/"
expect_code 200 "warm-up request (compiles the service)"

step "E2E-06: GET /logs?follow=1 streams a record per request with its task_id"
sse="$WORK/sse.txt"
curl -sN --max-time 15 -H 'Accept: text/event-stream' -K <(printf 'header = "Authorization: Bearer %s"\n' "$TENANT_KEY") \
  "$RT_API/v1/services/$id/logs?follow=1" >"$sse" 2>/dev/null &
sse_pid=$!
sleep 1
tasks=()
for _ in 1 2 3; do
  invoke "$slug" "/"
  expect_code 200 "request"
  tasks+=("$(header x-rt-task-id)")
done
for t in "${tasks[@]}"; do
  WAIT=5 wait_until "the SSE record of $t" grep -q "$t" "$sse"
done
kill "$sse_pid" 2>/dev/null || true
wait "$sse_pid" 2>/dev/null || true
grep -q '^data: ' "$sse" || fail "the stream carried no data: events"
grep '^data: ' "$sse" | sed 's/^data: //' | jq -e -s '[.[] | select(.msg == "e2e-log-line")] | length >= 3' >/dev/null \
  || fail "fewer than 3 e2e-log-line records were streamed"

step "GET /logs returns the same records with all fields"
last_task="${tasks[2]}"
logs_ready() {
  tkey GET "$RT_API/v1/services/$id/logs?task_id=$last_task"
  [[ "$HTTP_CODE" == "200" ]] && jq -e '.items | length >= 2' "$BODY_FILE" >/dev/null
}
WAIT=10 wait_until "the records of the last request" logs_ready
expect_jq '.items | all(has("ts") and has("level") and has("service_id") and has("version") and has("task_id") and has("trigger") and has("kind") and has("msg"))' "record fields"
expect_jq '.items | all(.task_id == "'"$last_task"'")' "task_id filter"
expect_jq '[.items[] | select(.msg == "e2e-error-line") | .level] == ["error"]' "console.error is level error"
expect_jq '[.items[] | select(.msg == "e2e-log-line") | .level] == ["info"]' "console.log is level info"
expect_jq '.items | all(.kind == "runtime" and .trigger == "http" and .service_id == "'"$id"'")' "kind, trigger, service"
tkey GET "$RT_API/v1/services/$id/logs?level=error&limit=100"
expect_jq '.items | length >= 3 and all(.level == "error")' "level filter"
tkey GET "$RT_API/v1/services/$id/logs?kind=build"
expect_code 200 "kind filter"

step "stats: counts and latency percentiles"
tkey GET "$RT_API/v1/services/$id/stats?window=5m"
expect_code 200 "GET /stats"
expect_jq 'type == "object" and (tostring | test("p99|p95|p50"))' "percentiles are reported"
tkey GET "$RT_API/v1/services/$id/stats?window=bogus"
expect_error_code 422 validation_failed "unknown stats window"

step "events: the deploy is recorded"
tkey GET "$RT_API/v1/services/$id/events"
expect_code 200 "GET /events"
expect_jq '.items | length >= 1' "at least the deploy event"
expect_jq '.items | all(has("id") and has("ts") and has("kind"))' "event shape"

step "another tenant cannot read these logs (404)"
FIRST_KEY="$TENANT_KEY"; FIRST_TENANT="$TENANT_ID"
setup_tenant
tkey GET "$RT_API/v1/services/$id/logs"
expect_error_code 404 not_found "cross-tenant logs"
op DELETE "$RT_API/v1/tenants/$TENANT_ID?purge=1"
TENANT_ID="$FIRST_TENANT"; TENANT_KEY="$FIRST_KEY"

step "node metrics reflect the traffic"
HTTP_TOKEN="" http GET "$RT_METRICS/metrics"
expect_code 200 "/metrics"
grep -Eq '^rt_logs_records_total' "$BODY_FILE" || fail "rt_logs_records_total is missing"
grep -Eq '^rt_http_requests_total\{[^}]*listener="public"' "$BODY_FILE" || fail "no public-listener request counter"
grep -Eq '^rt_tasks_total\{[^}]*trigger="http"' "$BODY_FILE" || fail "no http task counter"

pass
