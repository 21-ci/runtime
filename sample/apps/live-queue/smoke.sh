#!/usr/bin/env bash
# Smoke test for the live-queue sample against a running node: pages and CSP, concurrent ticket
# numbering, ticket tokens, call-next / skip / done / leave, pause, long-polling (a waiting
# request returns as soon as the queue changes, or after LONGPOLL_SECONDS without a change), and
# the per-address throttle on taking tickets (THROTTLE_PER_MINUTE).
#
#   RT_API=… RT_INVOKE=… RT_KEY=… ./smoke.sh
#
# Env: SLUG (default live-queue-smoke), SMOKE_CLEANUP=1 deletes the service at the end.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/smoke.sh
source "$HERE/../lib/smoke.sh"

SLUG="${SLUG:-live-queue-smoke}"
ADMIN="adm_$(rand_hex 16)"
AUTH="Authorization: Bearer $ADMIN"
LONGPOLL=6
THROTTLE=10   # tickets per minute per address; the run takes 7 before the throttle step

step "deploy $SLUG"
deploy "$HERE/src" "$SLUG"
set_secret "$SLUG" ADMIN_TOKEN "$ADMIN"
set_env "$SLUG" "{\"LONGPOLL_SECONDS\": \"$LONGPOLL\", \"THROTTLE_PER_MINUTE\": \"$THROTTLE\"}"

step "admin: open a queue"
inv "$SLUG" POST /api/queues '{"name": "Passport office, window 3"}'
expect_code 401 "create without the admin token"
inv "$SLUG" POST /api/queues '{"name": ""}' "$AUTH"
expect_code 400 "empty name"
inv "$SLUG" POST /api/queues '{"name": "Passport office, window 3", "minutesPerPerson": 2}' "$AUTH"
expect_code 201 "create"
QID="$(jget .queue.id)"
expect_jq ".links.take == \"/s/$SLUG/q/$QID\"" "links use the /s/{slug} base path"
inv "$SLUG" GET /api/queues "" "$AUTH"
expect_jq "[.queues[].id] | index(\"$QID\") != null" "listed for the admin"

step "pages"
for p in / /admin "/admin/q/$QID" "/q/$QID" "/q/$QID/ticket?n=1" "/q/$QID/screen"; do
  inv "$SLUG" GET "$p"
  if [[ "$CODE" == 200 && "$(resp_header content-type)" == text/html* && "$(resp_header content-security-policy)" == *"script-src 'nonce-"* ]]; then
    ok "GET $p: HTML with a nonce CSP"
  else
    fail "GET $p: HTTP $CODE, $(resp_header content-type)"
  fi
done
inv "$SLUG" GET "/q/$QID"
grep -q "\"qid\":\"$QID\"" "$BODY" && ok "page config carries the queue id" || fail "no config in the page"
inv "$SLUG" GET /q/not-an-id
expect_code 404 "bad queue id"

step "concurrent ticket numbering"
for i in 1 2 3 4 5; do
  curl -sS -X POST -H 'Content-Type: application/json' -d "{\"name\": \"Person $i\"}" \
    -o "$SMOKE_TMP/take.$i" "$RT_INVOKE/s/$SLUG/api/queues/$QID/tickets" &
done
wait
NUMS="$(for i in 1 2 3 4 5; do jq -r '.ticket.number' "$SMOKE_TMP/take.$i"; done | sort -n | tr '\n' ' ')"
[[ "$NUMS" == "1 2 3 4 5 " ]] && ok "5 parallel takes got 1..5" || fail "numbers were: $NUMS"
token_of() { for i in 1 2 3 4 5; do jq -r --argjson n "$1" 'select(.ticket.number == $n) | .token' "$SMOKE_TMP/take.$i"; done; }
T1="$(token_of 1)"; T3="$(token_of 3)"; T4="$(token_of 4)"; T5="$(token_of 5)"

step "ticket status"
inv "$SLUG" GET "/api/queues/$QID/tickets/3" "" "X-Ticket-Token: $T3"
expect_code 200 "own ticket"
expect_jq '.ticket.status == "waiting" and .ahead == 2 and .etaSeconds > 0' "2 ahead, ETA given"
V="$(jget .queue.version)"
inv "$SLUG" GET "/api/queues/$QID/tickets/3" "" "X-Ticket-Token: $T1"
expect_code 404 "someone else's token"
inv "$SLUG" GET "/api/queues/$QID/tickets/3"
expect_code 401 "no token"

step "long-poll wakes up on a change"
START=$(date +%s)
curl -sS -H "X-Ticket-Token: $T3" -o "$SMOKE_TMP/poll" "$RT_INVOKE/s/$SLUG/api/queues/$QID/tickets/3?since=$V" &
POLL_PID=$!
sleep 1
inv "$SLUG" POST "/api/queues/$QID/call-next" '{"count": 2, "counter": "window 3"}' "$AUTH"
expect_code 200 "call next 2"
expect_jq '.calledNow == [1, 2]' "called #1 and #2"
wait "$POLL_PID"
ELAPSED=$(( $(date +%s) - START ))
if [[ "$ELAPSED" -lt "$LONGPOLL" ]] && jq -e ".queue.version > $V and .ahead == 0" "$SMOKE_TMP/poll" >/dev/null; then
  ok "waiting request returned after ${ELAPSED}s with the new state"
else
  fail "long-poll: ${ELAPSED}s, $(head -c 200 "$SMOKE_TMP/poll")"
fi

step "call, skip, done, leave"
inv "$SLUG" GET "/api/queues/$QID/tickets/1" "" "X-Ticket-Token: $T1"
expect_jq '.ticket.status == "called" and .ticket.counter == "window 3"' "#1 is called to window 3"
inv "$SLUG" POST "/api/queues/$QID/call-next" '{"counter": "window 3"}' "$AUTH"
expect_jq '.calledNow == [3] and ([.called[].number] | index(1) == null)' "next call at the same counter finishes #1, #2 and calls #3"
inv "$SLUG" POST "/api/queues/$QID/tickets/5/done" "" "$AUTH"
expect_code 409 "done needs a called ticket"
inv "$SLUG" POST "/api/queues/$QID/tickets/4/skip" "" "$AUTH"
expect_code 200 "skip #4 (no-show)"
inv "$SLUG" GET "/api/queues/$QID/tickets/4" "" "X-Ticket-Token: $T4"
expect_jq '.ticket.status == "skipped"' "#4 sees skipped"
inv "$SLUG" DELETE "/api/queues/$QID/tickets/5" "" "X-Ticket-Token: $T5"
expect_code 200 "#5 leaves"
expect_jq '.ticket.status == "cancelled"' "#5 cancelled"

step "pause and resume"
inv "$SLUG" POST "/api/queues/$QID/pause" "" "$AUTH"
expect_jq '.queue.status == "paused"' "paused"
inv "$SLUG" POST "/api/queues/$QID/tickets" '{}'
expect_code 409 "no tickets while paused"
inv "$SLUG" POST "/api/queues/$QID/resume" "" "$AUTH"
expect_jq '.queue.status == "open"' "resumed"
inv "$SLUG" POST "/api/queues/$QID/tickets"
expect_code 201 "ticket after resume"
expect_jq '.ticket.number == 6' "numbering continues at 6"

step "board and an idle long-poll"
inv "$SLUG" GET "/api/queues/$QID/board" "" "$AUTH"
expect_code 200 "board"
expect_jq '.stats.served == 2 and .stats.skipped == 1 and (.waitingList | map(.number)) == [6] and .queue.secondsPerPerson > 0' "stats, waiting list"
V="$(jget .queue.version)"
START=$(date +%s)
inv "$SLUG" GET "/api/queues/$QID?since=$V"
ELAPSED=$(( $(date +%s) - START ))
if [[ "$CODE" == 200 && "$ELAPSED" -ge $((LONGPOLL - 2)) && "$(jget .version)" == "$V" ]]; then
  ok "no change: answered after ${ELAPSED}s (LONGPOLL_SECONDS=$LONGPOLL)"
else
  fail "idle long-poll: HTTP $CODE after ${ELAPSED}s"
fi

step "per-address throttle ($THROTTLE tickets per minute)"
# Enough attempts to use up a minute's budget even if a new minute starts during the loop.
for _ in $(seq 1 $((2 * THROTTLE + 2))); do
  inv "$SLUG" POST "/api/queues/$QID/tickets" '{"name": "Flood"}'
  [[ "$CODE" == 201 ]] || break
done
expect_code 429 "takes past the budget"
expect_jq '.error.code == "rate_limited"' "rate_limited"
[[ "$(resp_header retry-after)" =~ ^[0-9]+$ ]] && ok "Retry-After $(resp_header retry-after)s" || fail "no Retry-After"
inv "$SLUG" GET "/api/queues/$QID"
expect_code 200 "reads are not throttled"

finish
