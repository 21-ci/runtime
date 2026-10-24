#!/usr/bin/env bash
# Smoke test for the cbu-rates sample against a running node. Deterministic part: admin ingest of
# synthetic CBU-format payloads (change detection, idempotency, notify guard, history, convert).
# Live part: one real poll of cbu.uz (200, or 502 with an error code during a CBU outage).
#
#   RT_API=… RT_INVOKE=… RT_KEY=… ./smoke.sh
#
# Env: SLUG (default cbu-rates-smoke)
#      NOTIFY_SLUG  a queue consumer to notify (e.g. telegram-bot-smoke); default: a missing slug,
#                   which checks that a failed enqueue does not break ingestion
#      SMOKE_SLOW=1 also adds a 60 s interval schedule and waits for one scheduled poll
#      SMOKE_CLEANUP=1 deletes the service at the end
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/smoke.sh
source "$HERE/../lib/smoke.sh"

SLUG="${SLUG:-cbu-rates-smoke}"
ADMIN="adm_$(rand_hex 16)"
NOTIFY="${NOTIFY_SLUG:-no-such-service-$(rand_hex 4)}"
# A date 20 days ahead keeps the synthetic rows newer than anything a live poll stores.
DATE="$(jq -nr 'now + 20 * 86400 | strftime("%d.%m.%Y")')"
ISO="$(jq -nr 'now + 20 * 86400 | strftime("%Y-%m-%d")')"
# Cents never end in 0: the service stores canonical decimals ("12908.40" is kept as "12908.4").
cents() { echo "$(( RANDOM % 9 + 1 ))$(( RANDOM % 9 + 1 ))"; }
R1="$(( 11000 + RANDOM % 1000 )).$(cents)"
R2="$(( 12000 + RANDOM % 1000 )).$(cents)"

payload() { # USD_RATE
  jq -nc --arg d "$DATE" --arg r "$1" '[
    {Code: "840", Ccy: "USD", CcyNm_EN: "US Dollar", Nominal: "1", Rate: $r, Diff: "1.5", Date: $d},
    {Code: "978", Ccy: "EUR", CcyNm_EN: "Euro", Nominal: "1", Rate: "13198.93", Diff: "-25.62", Date: $d},
    {Code: "360", Ccy: "IDR", CcyNm_EN: "Indonesian Rupiah", Nominal: "10", Rate: "7.12", Diff: "0", Date: $d},
    {Ccy: "BAD", Rate: "not a number", Nominal: "1", Date: $d}
  ]'
}
AUTH="Authorization: Bearer $ADMIN"

step "deploy $SLUG"
deploy "$HERE/src" "$SLUG"
set_secret "$SLUG" ADMIN_TOKEN "$ADMIN"
set_env "$SLUG" "$(jq -nc --arg n "$NOTIFY" '{NOTIFY_SERVICE: $n, NOTIFY_CCYS: "USD,EUR"}')"

step "public API basics"
inv "$SLUG" GET /
expect_code 200 "GET /"
inv "$SLUG" GET "/history?ccy=US"
expect_code 400 "history rejects a bad currency code"
inv "$SLUG" GET "/history?ccy=USD&days=0"
expect_code 400 "history rejects days=0"
inv "$SLUG" POST /latest '{}'
expect_code 405 "read-only API"

step "admin auth"
inv "$SLUG" POST /admin/ingest "$(payload "$R1")"
expect_code 401 "ingest without the admin token"
inv "$SLUG" POST /admin/ingest '{"not": "an array"}' "$AUTH"
expect_code 422 "ingest of a non-array"

step "ingest and change detection"
inv "$SLUG" POST /admin/ingest "$(payload "$R1")" "$AUTH"
expect_code 200 "ingest #1"
expect_jq '.accepted == 3 and (.skipped | length) == 1' "bad item skipped, 3 accepted"
expect_jq "[.changed[] | select(.ccy == \"USD\" and .rate == \"$R1\")] | length == 1" "USD recorded"

inv "$SLUG" POST /admin/ingest "$(payload "$R1")" "$AUTH"
expect_code 200 "ingest #2 (same data)"
expect_jq '.changed == [] and .olderAdded == 0' "no change, nothing stored twice"

inv "$SLUG" POST /admin/ingest "$(payload "$R2")" "$AUTH"
expect_code 200 "ingest #3 (USD moved)"
expect_jq "[.changed[] | select(.ccy == \"USD\")][0] | .previous == \"$R1\" and .rate == \"$R2\"" "USD change with the previous rate"
if [[ -n "${NOTIFY_SLUG:-}" ]]; then
  expect_jq '.notify.enqueued == true and (.notify.key | startswith("cbu:"))' "notification enqueued to $NOTIFY"
else
  expect_jq '.notify.enqueued == false and .notify.reason != null' "missing consumer: enqueue fails softly ($(jget .notify.reason))"
fi

step "read API"
inv "$SLUG" GET "/latest?ccy=USD,IDR"
expect_code 200 "latest"
expect_jq "(.rates | map(.ccy)) == [\"IDR\", \"USD\"] and (.rates[] | select(.ccy == \"USD\") | .rate == \"$R2\")" "latest USD is the new rate"
expect_jq '.rates[] | select(.ccy == "IDR") | .nominal == 10 and .perUnit == 0.712' "nominal 10 handled"
[[ "$(resp_header access-control-allow-origin)" == "*" ]] && ok "CORS header" || fail "no CORS header"

inv "$SLUG" GET "/history?ccy=USD&days=5"
expect_code 200 "history"
expect_jq "[.points[] | select(.date == \"$ISO\") | .rate] | (index(\"$R1\") != null and index(\"$R2\") != null)" "both USD rates in the history"

inv "$SLUG" GET "/convert?amount=2&from=USD&to=UZS"
expect_code 200 "convert"
expect_jq ".result == ((\"$R2\" | tonumber) * 2 * 10000 | round / 10000)" "2 USD in UZS"
inv "$SLUG" GET "/convert?amount=1&from=XYZ"
expect_code 404 "convert with an unknown currency"

step "live poll of cbu.uz"
inv "$SLUG" POST /admin/refresh "" "$AUTH"
if [[ "$CODE" == 200 ]]; then
  expect_jq '.ok == true and .fetched > 50' "fetched $(jget .fetched) rates"
elif [[ "$CODE" == 502 ]]; then
  expect_jq '.ok == false and (.error | startswith("upstream_"))' "CBU unreachable, reported as $(jget .error)"
else
  fail "refresh: unexpected HTTP $CODE"
fi
inv "$SLUG" GET /status
expect_code 200 "status"
expect_jq 'has("consecutiveFailures") and has("lastAttemptAt")' "status fields"

if [[ "${SMOKE_SLOW:-0}" == "1" ]]; then
  step "interval schedule (up to ~80 s)"
  SVC="$(svc_id "$SLUG")"
  BEFORE="$(jget .lastAttemptAt)"
  api POST "/v1/services/$SVC/schedules" '{"kind": "interval", "every_seconds": 60}'
  expect_code 201 "schedule created"
  SCH="$(jget .id)"
  polled() { inv "$SLUG" GET /status; [[ "$(jget .lastAttemptAt)" != "$BEFORE" ]]; }
  if retry_until 80 polled; then ok "scheduled poll ran"; else fail "no scheduled poll within 80 s"; fi
  api DELETE "/v1/services/$SVC/schedules/$SCH"
  expect_code "200|204" "schedule removed"
fi

finish
