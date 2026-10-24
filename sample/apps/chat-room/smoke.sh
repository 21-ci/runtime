#!/usr/bin/env bash
# Smoke test for the chat-room sample against a running node: rooms as durable objects behind a
# gate service; join and presence (socket tags), history in the room's storage, the runtime's
# ping/pong auto-response, sockets held while the room has no isolate (ws.warm = false), across a
# redeploy (the next event runs the new version), and closed 1012 by a deploy with close_sockets.
#
#   RT_API=… RT_INVOKE=… RT_KEY=… ./smoke.sh
#
# Needs Node 22+ (global WebSocket) for test/chat.mjs. Env: GATE (default chat-smoke), ROOMS
# (default chat-rooms-smoke), SMOKE_CLEANUP=1 deletes both services at the end.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/smoke.sh
source "$HERE/../lib/smoke.sh"

GATE="${GATE:-chat-smoke}"
ROOMS="${ROOMS:-chat-rooms-smoke}"
command -v node >/dev/null 2>&1 || die "node (22+) is required for the WebSocket client"

step "deploy $ROOMS (durable) and $GATE"
deploy "$HERE/src/rooms.ts" "$ROOMS"
configure "$ROOMS" '{"durable": true}'
deploy "$HERE/src/gate.ts" "$GATE"
set_env "$GATE" "{\"ROOMS_SLUG\": \"$ROOMS\"}"

step "gate"
inv "$GATE" GET /
expect_code 200 "the page"
inv "$GATE" GET /rooms/Bad_Room/history
expect_code 404 "a bad room name"
inv "$GATE" GET /rooms/lobby/ws
expect_code 426 "the socket path wants an upgrade"

if RT_API="$RT_API" RT_INVOKE="$RT_INVOKE" RT_KEY="$RT_KEY" GATE="$GATE" ROOMS="$ROOMS" \
    node "$HERE/test/chat.mjs" "$HERE/src/rooms.ts" > "$SMOKE_TMP/chat.log" 2>&1; then
  cat "$SMOKE_TMP/chat.log"
  PASSED=$((PASSED + $(grep -c '^  ok ' "$SMOKE_TMP/chat.log")))
else
  cat "$SMOKE_TMP/chat.log"
  PASSED=$((PASSED + $(grep -c '^  ok ' "$SMOKE_TMP/chat.log" || true)))
  FAILED=$((FAILED + $(grep -c '^  FAIL' "$SMOKE_TMP/chat.log" || echo 1)))
fi

step "control API: objects and sockets"
ID="$(svc_id "$ROOMS")"
api GET "/v1/services/$ID/objects"
expect_code 200 "objects of the rooms service"
expect_jq '[.items[] | select(.storage == true)] | length >= 1' "the room has storage"

finish
