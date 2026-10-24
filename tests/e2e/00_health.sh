#!/usr/bin/env bash
# E2E 00: the node is up, healthy, exposes metrics and guards the control API.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

step "liveness and readiness probes on the metrics listener"
HTTP_TOKEN="" http GET "$RT_METRICS/healthz"
expect_code 200 "/healthz"
HTTP_TOKEN="" http GET "$RT_METRICS/readyz"
expect_code 200 "/readyz"

step "Prometheus metrics"
HTTP_TOKEN="" http GET "$RT_METRICS/metrics"
expect_code 200 "/metrics"
grep -q '^# TYPE rt_' "$BODY_FILE" || fail "/metrics lists no rt_* metric family"
for m in rt_tasks_total rt_sched_processors rt_mem_budget_bytes rt_http_requests_total rt_schedule_fires_total rt_logs_records_total; do
  grep -q "$m" "$BODY_FILE" || fail "/metrics is missing $m"
done

step "the control API rejects missing and wrong credentials with the error envelope"
anon GET "$RT_API/v1/node/status"
expect_error_code 401 unauthorized "no credentials"
HTTP_TOKEN="e2e-wrong-token" http GET "$RT_API/v1/node/status"
expect_error_code 401 unauthorized "wrong token"

step "node status with the operator token"
op GET "$RT_API/v1/node/status"
expect_code 200 "GET /v1/node/status"
expect_jq '.mode == "normal"' "node mode"
expect_jq '.processors > 0' "processor count"
expect_jq '.v8_sandbox | type == "boolean"' "v8_sandbox flag"

step "the invoke listener answers unknown services with a plain 404 and x-rt-reason"
invoke "e2e-no-such-service" "/"
expect_code 404 "unknown service"
expect_eq "$(header x-rt-reason)" "not_found" "x-rt-reason"
case "$(header content-type)" in text/plain*) ;; *) fail "invoke errors must be text/plain, got '$(header content-type)'" ;; esac

pass
