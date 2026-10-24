#!/usr/bin/env bash
# E2E 23: schedules and the queue (sample/deno/scheduled-queue): interval schedule fires, queue
# delivery, retry with backoff, dedup, API enqueue, DLQ and redrive.
#   RT_E2E_SCHEDULE_SECONDS  interval of the test schedule (default 10 = the default plan minimum)
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"
setup_tenant

slug="$(uniq_slug e2e-sq)"
deploy "$SAMPLE_DIR/deno/scheduled-queue" "$slug"
id="$(svc_id "$slug")"
every="${RT_E2E_SCHEDULE_SECONDS:-10}"

count_kind() {  # count_kind KIND
  invoke "$slug" "/"
  jq --arg k "$1" '[.events[] | select(.kind == $k)] | length' "$BODY_FILE"
}
has_done_text() {  # has_done_text TEXT
  invoke "$slug" "/"
  jq -e --arg t "$1" '[.events[] | select(.kind == "queue-done") | (.detail | fromjson | .text)] | index($t) != null' "$BODY_FILE" >/dev/null
}
dlq_count() {
  tkey GET "$RT_API/v1/services/$id/dlq"
  expect_code 200 "GET /dlq"
  jq '(if type == "array" then . else (.items // .entries // []) end) | length' "$BODY_FILE"
}
scheduled_at_least() { [[ "$(count_kind scheduled)" -ge "$1" ]]; }

step "interval schedule every ${every}s with a payload"
tkey POST "$RT_API/v1/services/$id/schedules" "$(jq -nc --argjson e "$every" '{kind:"interval", every_seconds:$e, payload:{step:"e2e"}}')"
if [[ "$HTTP_CODE" == "422" ]]; then
  fail "schedule rejected ($(jqr '.error.code')): the plan minimum is above ${every}s; set RT_E2E_SCHEDULE_SECONDS"
fi
expect_code 201 "create schedule"
sid="$(jqr .id)"
[[ "$sid" == sch_* ]] || fail "schedule id '$sid' is not sch_…"
tkey POST "$RT_API/v1/services/$id/schedules" '{"kind":"interval","every_seconds":1}'
if [[ "$HTTP_CODE" == "201" ]]; then
  : # a plan with a 1 s minimum accepted it; remove the extra schedule again
  extra="$(jqr .id)"
  tkey DELETE "$RT_API/v1/services/$id/schedules/$extra"
  expect_code 204 "delete the extra schedule"
else
  expect_error_code 422 schedule_too_frequent "an interval below the plan minimum"
fi

step "the handler is called with the schedule's payload, at most once per period (never overlapping)"
WAIT=$(( every * 3 + 30 )) wait_until "two scheduled runs" scheduled_at_least 2
invoke "$slug" "/"
jq -e '[.events[] | select(.kind == "scheduled") | (.detail | fromjson | .payload)] | map(select(. != null)) | length >= 1 and all(.step == "e2e")' "$BODY_FILE" >/dev/null \
  || fail "event.payload is not the schedule's payload"

step "disable the schedule: no more fires"
tkey DELETE "$RT_API/v1/services/$id/schedules/$sid"
expect_code 204 "delete schedule"
sleep 2
fires_after_delete="$(count_kind scheduled)"
sleep $(( every + 3 ))
[[ "$(count_kind scheduled)" -le $(( fires_after_delete + 1 )) ]] || fail "a deleted schedule keeps firing"

step "queue through the guest SDK: delivered once and acked"
invoke_json POST "$slug" "/enqueue" '{"text":"e2e-plain"}'
expect_code 202 "enqueue via std:queue"
WAIT=30 wait_until "e2e-plain delivered" has_done_text "e2e-plain"

step "queue through the control API"
tkey POST "$RT_API/v1/services/$id/queue" '{"body":{"text":"from-api"}}'
expect_code 202 "POST /queue"
[[ "$(jqr .id)" == msg_* ]] || fail "message id is not msg_…"
WAIT=30 wait_until "from-api delivered" has_done_text "from-api"

step "a handler that fails twice is retried and succeeds on attempt 3"
invoke_json POST "$slug" "/enqueue" '{"text":"e2e-flaky","failUntilAttempt":3}'
expect_code 202 "enqueue flaky"
WAIT=60 wait_until "e2e-flaky delivered" has_done_text "e2e-flaky"
invoke "$slug" "/"
expect_jq '[.events[] | select(.kind == "queue-done") | select((.detail | fromjson | .text) == "e2e-flaky") | .attempt] | .[0] == 3' "acked on attempt 3"
expect_jq '[.events[] | select(.kind == "queue-retry") | select((.detail | fromjson | .text) == "e2e-flaky")] | length == 2' "two failed attempts before"

step "dedupId: two enqueues, one delivery"
dd="dd-$RANDOM$RANDOM"
invoke_json POST "$slug" "/enqueue" "$(jq -nc --arg d "$dd" '{text:"e2e-dedup", delaySeconds:2, dedupId:$d}')"
expect_code 202 "first enqueue"
invoke_json POST "$slug" "/enqueue" "$(jq -nc --arg d "$dd" '{text:"e2e-dedup", delaySeconds:2, dedupId:$d}')"
expect_code 202 "second enqueue"
WAIT=30 wait_until "e2e-dedup delivered" has_done_text "e2e-dedup"
sleep 3
invoke "$slug" "/"
expect_jq '[.events[] | select(.kind == "queue-done") | select((.detail | fromjson | .text) == "e2e-dedup")] | length == 1' "exactly one delivery for the dedup id"

step "a message that always fails ends in the DLQ after 5 attempts; redrive; purge"
invoke_json POST "$slug" "/enqueue" '{"text":"e2e-poison","failUntilAttempt":99}'
expect_code 202 "enqueue poison"
deadline=$(( SECONDS + 120 ))
until [[ "$(dlq_count)" -ge 1 ]]; do (( SECONDS < deadline )) || fail "the poison message never reached the DLQ"; sleep 1; done
invoke "$slug" "/"
expect_jq '[.events[] | select(.kind == "queue-retry") | select((.detail | fromjson | .text) == "e2e-poison")] | length == 5' "five attempts before dead-lettering"
tkey GET "$RT_API/v1/services/$id/queue"
expect_code 200 "queue stats"
expect_jq '.depth == 0' "nothing waiting"
tkey POST "$RT_API/v1/services/$id/dlq/redrive"
expect_code 200 "redrive"
expect_eq "$(jqr .redriven)" 1 "redriven count"
deadline=$(( SECONDS + 120 ))
until [[ "$(dlq_count)" -ge 1 ]]; do (( SECONDS < deadline )) || fail "the redriven poison message never came back to the DLQ"; sleep 1; done
tkey DELETE "$RT_API/v1/services/$id/dlq"
expect_code 204 "purge DLQ"
expect_eq "$(dlq_count)" 0 "DLQ after purge"

pass
