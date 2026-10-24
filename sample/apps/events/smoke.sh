#!/usr/bin/env bash
# Smoke test for the events sample against a running node: admin CRUD, registration rules
# (capacity, duplicates, closed events), idempotent retries, cancellation by token, JSON + CSV export,
# and the per-address registration throttle (THROTTLE_PER_MINUTE).
#
#   RT_API=… RT_INVOKE=… RT_KEY=… ./smoke.sh
#
# Env: SLUG (default events-smoke), SMOKE_CLEANUP=1 deletes the service at the end.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/smoke.sh
source "$HERE/../lib/smoke.sh"

SLUG="${SLUG:-events-smoke}"
ADMIN="adm_$(rand_hex 16)"
AUTH="Authorization: Bearer $ADMIN"
STARTS="$(jq -nr 'now + 2 * 86400 | strftime("%Y-%m-%dT%H:%M:%SZ")')"
RUN="$(rand_hex 4)"   # unique contacts per run
THROTTLE=15           # registrations per minute per address; the run sends 10 before the throttle step

NOTIFY="$SLUG-notify"

step "deploy $SLUG and its notifier $NOTIFY"
deploy "$HERE/notify" "$NOTIFY"
deploy "$HERE/src" "$SLUG"
set_secret "$SLUG" ADMIN_TOKEN "$ADMIN"
set_env "$SLUG" "{\"THROTTLE_PER_MINUTE\": \"$THROTTLE\", \"NOTIFY_SERVICE\": \"$NOTIFY\"}"

step "admin: create"
EVENT_JSON="$(jq -nc --arg s "$STARTS" '{title: "Smoke meetup", description: "Talks\nand tea", location: "Tashkent", starts_at: $s, capacity: 2}')"
inv "$SLUG" POST /events "$EVENT_JSON"
expect_code 401 "create without the admin token"
inv "$SLUG" POST /events '{"title": "x", "starts_at": "2026-11-20T18:00:00", "capacity": 2}' "$AUTH"
expect_code 400 "starts_at without an offset is rejected"
inv "$SLUG" POST /events "$EVENT_JSON" "$AUTH"
expect_code 201 "create"
EV="$(jget .id)"
expect_jq '.spotsLeft == 2 and .registrationOpen == true' "2 spots, open"

step "public listing"
inv "$SLUG" GET /events
expect_code 200 "list"
expect_jq "[.events[].id] | index($EV) != null" "event listed"
inv "$SLUG" GET "/events/$EV"
expect_jq '.title == "Smoke meetup" and .registered == 0' "event detail"
inv "$SLUG" GET /events/999999999
expect_code 404 "unknown event"

reg() { # NAME CONTACT_JSON_FIELDS [HEADER...]
  local name="$1" fields="$2"
  shift 2
  inv "$SLUG" POST "/events/$EV/registrations" "$(jq -nc --arg n "$name" --argjson f "$fields" '{name: $n} + $f')" "$@"
}

step "registration"
KEY="smoke-$RUN-ada"
reg "Ada" "{\"email\": \"ada.$RUN@example.com\"}" "Idempotency-Key: $KEY"
expect_code 201 "Ada registers"
T_ADA="$(jget .token)"
expect_jq '.registration.status == "confirmed" and (.token | startswith("evr_"))' "token returned once"
expect_jq '.receipt | startswith("RC-")' "the notifier confirmed it over RPC"
inv "$NOTIFY" GET /last
expect_jq ".caller == \"$SLUG\" and .eventId == $EV" "the notifier saw this service as ctx.caller"
reg "Ada" "{\"email\": \"ada.$RUN@example.com\"}" "Idempotency-Key: $KEY"
expect_code 201 "retry with the same Idempotency-Key"
[[ "$(resp_header idempotent-replayed)" == "true" && "$(jget .token)" == "$T_ADA" ]] && ok "replayed the first answer" || fail "not replayed"
reg "Ada Lovelace" "{\"email\": \"ada.$RUN@example.com\"}" "Idempotency-Key: $KEY"
expect_code 422 "same key, different body"
reg "Ada again" "{\"email\": \"ADA.$RUN@example.com\"}"
expect_code 409 "duplicate email (case-insensitive)"
expect_jq '.error.code == "already_registered"' "already_registered"
reg "Bob" "{\"telegram\": \"@bob_$RUN\"}"
expect_code 201 "Bob registers by Telegram"
reg "Carol" "{\"email\": \"carol.$RUN@example.com\"}"
expect_code 409 "event full"
expect_jq '.error.code == "event_full"' "event_full"
reg "Eve" '{"email": "not-an-email"}'
expect_code 400 "bad email"

step "own registration and cancellation"
inv "$SLUG" GET /registration "" "X-Registration-Token: $T_ADA"
expect_code 200 "look up by token"
expect_jq ".registration.name == \"Ada\" and .event.id == $EV" "own registration + event"
inv "$SLUG" GET /registration "" "X-Registration-Token: evr_nope"
expect_code 401 "malformed token"
inv "$SLUG" DELETE /registration "" "X-Registration-Token: $T_ADA"
expect_code 200 "cancel"
expect_jq '.registration.status == "cancelled"' "cancelled"
inv "$SLUG" DELETE /registration "" "X-Registration-Token: $T_ADA"
expect_code 200 "cancel again is idempotent"
reg "Carol" "{\"email\": \"carol.$RUN@example.com\"}"
expect_code 201 "the freed spot is taken"

step "admin: update, close, reopen"
inv "$SLUG" PATCH "/events/$EV" '{"capacity": 1}' "$AUTH"
expect_code 409 "capacity below registrations"
inv "$SLUG" PATCH "/events/$EV" '{"capacity": 3, "title": "Smoke meetup (updated)"}' "$AUTH"
expect_code 200 "update"
expect_jq '.capacity == 3 and .spotsLeft == 1' "one spot left"
inv "$SLUG" POST "/events/$EV/close" "" "$AUTH"
expect_jq '.status == "closed" and .registrationOpen == false' "closed"
reg "Dave" "{\"email\": \"dave.$RUN@example.com\"}"
expect_code 409 "no registration while closed"
expect_jq '.error.code == "event_closed"' "event_closed"
inv "$SLUG" POST "/events/$EV/reopen" "" "$AUTH"
expect_code 200 "reopen"
reg "=HYPERLINK(\"http://evil\")" "{\"email\": \"dave.$RUN@example.com\"}"
expect_code 201 "a name that looks like a formula is accepted"

step "admin: export"
inv "$SLUG" GET "/events/$EV/registrations" "" "$AUTH"
expect_code 200 "JSON list"
expect_jq '(.registrations | length) == 4 and ([.registrations[] | select(.status == "confirmed")] | length) == 3' "4 rows, 3 confirmed"
inv "$SLUG" GET "/events/$EV/registrations?status=cancelled" "" "$AUTH"
expect_jq '(.registrations | length) == 1 and .registrations[0].name == "Ada"' "status filter"
inv "$SLUG" GET "/events/$EV/registrations.csv"
expect_code 401 "CSV needs the admin token"
inv "$SLUG" GET "/events/$EV/registrations.csv" "" "$AUTH"
expect_code 200 "CSV export"
[[ "$(resp_header content-type)" == text/csv* ]] && ok "text/csv" || fail "content-type is $(resp_header content-type)"
[[ "$(tr -d '\r' < "$BODY" | grep -c .)" == "5" ]] && ok "header + 4 records" || fail "unexpected CSV line count"
grep -q "\"'=HYPERLINK" "$BODY" && ok "formula neutralised in CSV" || fail "formula not guarded"
grep -q ",bob_$RUN," "$BODY" && ok "telegram column" || fail "telegram column missing"

step "per-address throttle ($THROTTLE registrations per minute)"
# Invalid bodies (400) count too; enough attempts even if a new minute starts during the loop.
for _ in $(seq 1 $((2 * THROTTLE + 2))); do
  reg "Flood" '{"email": "not-an-email"}'
  [[ "$CODE" == 400 ]] || break
done
expect_code 429 "registrations past the budget"
expect_jq '.error.code == "rate_limited"' "rate_limited"
[[ "$(resp_header retry-after)" =~ ^[0-9]+$ ]] && ok "Retry-After $(resp_header retry-after)s" || fail "no Retry-After"
inv "$SLUG" GET "/events/$EV"
expect_code 200 "reads are not throttled"

finish
