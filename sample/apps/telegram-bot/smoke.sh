#!/usr/bin/env bash
# Smoke test for the telegram-bot sample against a running node. Uses dry-run mode
# (TELEGRAM_DRY_RUN=1): the bot records Bot API calls in its outbox instead of calling Telegram,
# so no real bot token is needed. Deploys to its own slug, so a real bot is not touched.
#
#   RT_API=… RT_INVOKE=… RT_KEY=… ./smoke.sh
#
# Env: SLUG (default telegram-bot-smoke), SMOKE_SLOW=1 also waits ~70 s for a reminder to fire,
#      SMOKE_CLEANUP=1 deletes the service at the end. RT_KEY needs queue:write for the rates step.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/smoke.sh
source "$HERE/../lib/smoke.sh"

SLUG="${SLUG:-telegram-bot-smoke}"
ADMIN="adm_$(rand_hex 16)"
SECRET="whs_$(rand_hex 20)"
CHAT=$(( 100000000 + RANDOM * 1000 + RANDOM % 1000 ))
UPDATE_ID=$(( $(date +%s) * 1000 ))

step "deploy $SLUG"
deploy "$HERE/src" "$SLUG"
set_env "$SLUG" "$(jq -nc --arg u "https://bot.example.test/s/$SLUG" \
  '{TELEGRAM_DRY_RUN: "1", PUBLIC_URL: $u, BOT_TZ_OFFSET_MINUTES: "300", RATES_SERVICE: "cbu-rates-smoke"}')"
set_secret "$SLUG" ADMIN_TOKEN "$ADMIN"
set_secret "$SLUG" TELEGRAM_WEBHOOK_SECRET "$SECRET"

LAST_UPDATE=""
tg() { # TEXT: deliver a private-chat text message from $CHAT as a new update
  UPDATE_ID=$((UPDATE_ID + 1))
  LAST_UPDATE="$(jq -nc --argjson u "$UPDATE_ID" --argjson c "$CHAT" --arg t "$1" \
    '{update_id: $u, message: {message_id: $u, date: 0, chat: {id: $c, type: "private"}, from: {id: $c, is_bot: false, first_name: "Smoke"}, text: $t}}')"
  inv "$SLUG" POST /webhook "$LAST_UPDATE" "X-Telegram-Bot-Api-Secret-Token: $SECRET"
}
last_reply() { # newest sendMessage text to $CHAT
  inv "$SLUG" GET "/admin/outbox?chat_id=$CHAT" "" "Authorization: Bearer $ADMIN"
  jget '[.entries[] | select(.method == "sendMessage")][0].params.text // ""'
}
expect_reply() { # PATTERN LABEL
  local text
  text="$(last_reply)"
  if printf '%s' "$text" | grep -Eq -- "$1"; then ok "$2"; else fail "$2: reply was: $(printf '%s' "$text" | head -c 200)"; fi
}

step "health and admin"
inv "$SLUG" GET /
expect_code 200 "GET /"
expect_jq '.dryRun == true and .configured.webhookSecret == true and .configured.adminToken == true' "settings present"
inv "$SLUG" POST /setup
expect_code 401 "setup without the admin token"
inv "$SLUG" POST /setup "" "Authorization: Bearer $ADMIN"
expect_code 200 "setup"
expect_jq ".webhook == \"https://bot.example.test/s/$SLUG/webhook\"" "webhook URL from PUBLIC_URL"
inv "$SLUG" GET /admin/outbox "" "Authorization: Bearer $ADMIN"
expect_jq '[.entries[].method] | index("setWebhook") != null and index("setMyCommands") != null' "setWebhook + setMyCommands recorded"
expect_jq '[.entries[] | select(.method == "setWebhook")][0].params | .secret_token != null and .allowed_updates == ["message"]' "setWebhook carries the secret token"

step "webhook authentication"
inv "$SLUG" POST /webhook '{"update_id": 1}'
expect_code 401 "no secret header"
inv "$SLUG" POST /webhook '{"update_id": 1}' "X-Telegram-Bot-Api-Secret-Token: wrong-secret-0123456789"
expect_code 401 "wrong secret"
inv "$SLUG" POST /webhook '{"nope": true}' "X-Telegram-Bot-Api-Secret-Token: $SECRET"
expect_code 400 "not an update"

step "dialogs"
tg "/start"
expect_code 200 "/start"
expect_reply "/note" "help text"
inv "$SLUG" POST /webhook "$LAST_UPDATE" "X-Telegram-Bot-Api-Secret-Token: $SECRET"
expect_jq '.duplicate == true' "re-delivered update_id is ignored"

tg "/note"
expect_reply "note text" "/note asks for the text"
tg "buy milk"
expect_reply "Saved note #[0-9]+" "note saved"
tg "/notes"
expect_reply "buy milk" "/notes lists it"

tg "/remind"
expect_reply "remind you about" "/remind asks what"
tg "call mom"
expect_reply "When\\?" "then asks when"
tg "someday"
expect_reply "could not read" "bad time is rejected, dialog continues"
tg "2h"
expect_reply "Reply yes or no" "asks to confirm"
tg "yes"
expect_reply "Reminder #[0-9]+ set" "reminder created (delayed queue message)"
tg "/reminders"
expect_reply "call mom" "/reminders lists it"
tg "/cancel"
expect_reply "Nothing to cancel" "/cancel when idle"

tg "/subscribe"
expect_reply "Subscribed" "/subscribe"
tg "/rates"
expect_reply "rates|Rates" "/rates answers (cbu-rates-smoke connected or not)"

step "rate alert fan-out (queue)"
SVC="$(svc_id "$SLUG")"
KEY="smoke$(rand_hex 6)"
api POST "/v1/services/$SVC/queue" "$(jq -nc --arg k "$KEY" \
  '{body: {type: "cbu.rate_changed", key: $k, changes: [{ccy: "USD", date: "2026-10-07", rate: "11778.45", diff: "5.5", nominal: 1, previous: "11772.95"}]}}')"
expect_code "200|201|202" "enqueue a cbu.rate_changed message"
rates_sent() { [[ "$(last_reply)" == *"CBU rates changed"* ]]; }
if retry_until 30 rates_sent; then ok "subscriber got the rate alert"; else fail "no rate alert in the outbox after 30 s"; fi

if [[ "${SMOKE_SLOW:-0}" == "1" ]]; then
  step "reminder delivery (about 70 s)"
  tg "/remind 1m smoke reminder"
  expect_reply "Reminder #[0-9]+ set" "quick reminder"
  reminder_sent() { [[ "$(last_reply)" == "Reminder: smoke reminder" ]]; }
  if retry_until 100 reminder_sent; then ok "reminder delivered"; else fail "reminder not delivered"; fi
fi

finish
