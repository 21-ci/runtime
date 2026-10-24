#!/usr/bin/env bash
# Run every sample app's smoke.sh against a private node.
#
#   sample/apps/run-smokes.sh [rt-binary] [app ...]     # default binary target/debug/rt, all apps
#
# Starts the node with sample/apps/smoke-node.toml (ports 28080-28082, 29090) and the mock operator
# API (sample/operator-sdk/mock-platform-api, port 3000). It then creates a tenant on the `smoke`
# plan plus a key with every tenant scope, and runs each app's smoke.sh with RT_API / RT_INVOKE /
# RT_KEY set. Logs go to sample/apps/.smoke-data/*.log.
set -uo pipefail
REPO="$(cd "$(dirname "$0")/../.." && pwd)"
BIN="${1:-$REPO/target/debug/rt}"; shift || true
APPS=("$@"); [ ${#APPS[@]} -eq 0 ] && APPS=(cbu-rates events live-queue telegram-bot edge-gateway chat-room)
DATA="$REPO/sample/apps/.smoke-data"
export RT_ROOT_TOKEN=smoke-root-token RT_MASTER_KEY=$(printf '%064x' 7) RT_LOG=warn
export ACME_INTERNAL_TOKEN=smoke-internal-token
export RT_API=http://127.0.0.1:28081 RT_INVOKE=http://127.0.0.1:28080

rm -rf "$DATA" && mkdir -p "$DATA"
cd "$REPO"
node sample/operator-sdk/mock-platform-api/server.js 3000 > "$DATA/mock-api.log" 2>&1 & MOCK=$!
"$BIN" serve --config sample/apps/smoke-node.toml > "$DATA/node.log" 2>&1 & NODE=$!
trap 'kill $NODE $MOCK 2>/dev/null; wait $NODE $MOCK 2>/dev/null || true' EXIT
for _ in $(seq 1 300); do curl -fsS http://127.0.0.1:29090/readyz >/dev/null 2>&1 && break; sleep 0.2; done
curl -fsS http://127.0.0.1:29090/readyz >/dev/null || { echo "node did not start"; tail -20 "$DATA/node.log"; exit 1; }

H=(-H "Authorization: Bearer $RT_ROOT_TOKEN" -H 'Content-Type: application/json')
TEN=$(curl -sS -X POST "$RT_API/v1/tenants" "${H[@]}" -d '{"name":"smoke","plan":"smoke"}' | jq -r .id)
RT_KEY=$(curl -sS -X POST "$RT_API/v1/tenants/$TEN/keys" "${H[@]}" \
  -d '{"scopes":["services:read","services:write","deploy","env:write","secrets:write","queue:write","logs:read"]}' \
  | jq -r .secret)
export RT_KEY
[ -n "$RT_KEY" ] && [ "$RT_KEY" != null ] || { echo "could not create a tenant key"; exit 1; }

declare -a RESULTS=()
FAILED=0
for app in "${APPS[@]}"; do
  echo "################ $app"
  if bash "sample/apps/$app/smoke.sh" > "$DATA/$app.log" 2>&1; then
    RESULTS+=("PASS $app ($(grep -c '^  ok ' "$DATA/$app.log") checks)")
  else
    FAILED=1
    RESULTS+=("FAIL $app ($(grep -c '^  ok ' "$DATA/$app.log") ok, $(grep -c '^  FAIL' "$DATA/$app.log") failed)")
    grep -E '^  FAIL|^error' "$DATA/$app.log" | head -15
  fi
done
printf '%s\n' "${RESULTS[@]}"
exit $FAILED
