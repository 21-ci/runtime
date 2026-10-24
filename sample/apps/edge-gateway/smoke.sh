#!/usr/bin/env bash
# Smoke test for the edge-gateway sample against a running node. Deploys three services:
#   <prefix>-gateway  WASM component (public), environment "edge"
#   <prefix>-notes    Deno, access=internal, environment "edge-backend", allowed_callers=[gateway]
#   <prefix>-audit    Deno, same isolation; queue consumer + shipper through the operator SDK (retries)
# and publishes the operator SDK (sample/operator-sdk/acme-sdk) as acme_<run>, then checks token
# verification, scopes, isolation of the backends, validation, the per-subject and per-address
# rate limits, allowed_callers enforcement, the audit trail, and an SDK update, rollback and
# removal that reach the running gateway without a redeploy.
#
#   RT_API=… RT_INVOKE=… RT_KEY=… RT_ROOT_TOKEN=… ./smoke.sh
#
# Env: PREFIX (default edge-smoke), SMOKE_CLEANUP=1 deletes the services at the end,
#      RT_ROOT_TOKEN (required): the operator token that publishes and grants the SDK,
#      EXPECT_SHIPPED=1 asserts that events reached the operator API (needs the `acme` binding of
#      sample/operator-sdk on the node, its mock API, and a plan in the binding's allowed_plans).
# Needs cargo + the wasm32-wasip2 target when gateway/target has no component yet.
set -euo pipefail
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=../lib/smoke.sh
source "$HERE/../lib/smoke.sh"

PREFIX="${PREFIX:-edge-smoke}"
GW="$PREFIX-gateway"
NOTES="$PREFIX-notes"
AUDIT="$PREFIX-audit"
WASM="$HERE/gateway/target/wasm32-wasip2/release/edge_gateway.wasm"
GATEWAY_KEY_HEX="$(rand_hex 32)"
export GATEWAY_KEY_HEX
RUN="$(rand_hex 3)"
# The operator SDK, published for this run (names are node-wide).
SDK="acme_$RUN"
SDK_SRC="$HERE/../../operator-sdk/acme-sdk/mod.ts"

mint() { "$HERE/mint-token.sh" "$@"; }

step "operator SDK $SDK 1.0.0"
publish_sdk "$SDK" 1.0.0 "$SDK_SRC"
expect_code 201 "published $SDK 1.0.0 (stable)"
root_api PUT /v1/sdk-grants "$(jq -nc --arg s "$SDK" '{scope: "node", sdk: $s, effect: "allow"}')"
expect_code 200 "granted to every tenant"
expect_jq '.version_req == "^1"' "the grant is pinned to the stable major"

step "build and deploy"
if [[ ! -f "$WASM" ]]; then "$HERE/gateway/build.sh" > "$SMOKE_TMP/build.log" 2>&1 || { cat "$SMOKE_TMP/build.log" >&2; die "build failed"; }; fi
ok "component $(wc -c < "$WASM" | tr -d ' ') bytes"
deploy "$HERE/notes/main.ts" "$NOTES"
deploy "$HERE/audit/src" "$AUDIT"
deploy "$WASM" "$GW"
# Environments belong to a project: declare the two this smoke uses on the default project first.
api PATCH /v1/projects/default '{"environments": ["production", "edge", "edge-backend"]}'
expect_code 200 "default project has the edge environments"
BACKEND="$(jq -nc --arg gw "$GW" '{environment: "edge-backend", access: "internal", call: {allowed_callers: [$gw]}}')"
configure "$NOTES" "$BACKEND"
configure "$AUDIT" "$BACKEND"
configure "$GW" '{"environment": "edge"}'
set_env "$AUDIT" "$(jq -nc --arg s "$SDK" '{AUDIT_SDK: $s}')"
GW_ENV="$(jq -nc --arg n "$NOTES" --arg a "$AUDIT" --arg s "$SDK" \
  '{GATEWAY_NOTES_SERVICE: $n, GATEWAY_AUDIT_SERVICE: $a, GATEWAY_RATE_LIMIT: "10", GATEWAY_RATE_WINDOW: "60",
    GATEWAY_AUDIT: "all", GATEWAY_SDK: $s}')"
set_env "$GW" "$GW_ENV"
set_secret "$GW" GATEWAY_KEYS "k1:$GATEWAY_KEY_HEX"

ALICE="$(mint "alice-$RUN" "notes:read notes:write")"
BOB="$(mint "bob-$RUN" "notes:read notes:write")"
READER="$(mint "reader-$RUN" "notes:read")"
AUDITOR="$(mint "auditor-$RUN" "audit:read")"
A="Authorization: Bearer $ALICE"

step "backends are not public"
inv "$NOTES" GET /
expect_code 404 "notes service on the public listener"
inv "$AUDIT" GET /recent
expect_code 404 "audit service on the public listener"
inv "$GW" GET /healthz
expect_code 200 "gateway health"

step "token verification"
inv "$GW" GET /v1/notes
expect_code 401 "no token"
[[ "$(resp_header www-authenticate)" == Bearer* ]] && ok "WWW-Authenticate: Bearer" || fail "no WWW-Authenticate"
inv "$GW" GET /v1/notes "" "Authorization: Bearer $(GATEWAY_KEY_HEX="$(rand_hex 32)" mint "alice-$RUN" "notes:read")"
expect_code 401 "token signed with another key"
inv "$GW" GET /v1/notes "" "Authorization: Bearer $(MINT_NOW=$(( $(date +%s) - 1000 )) mint "alice-$RUN" "notes:read" 300)"
expect_code 401 "expired token"
inv "$GW" GET /v1/notes "" "Authorization: Bearer $(mint "alice-$RUN" "notes:read" 3600)"
expect_code 401 "long-lived token (exp - iat > 900 s)"
inv "$GW" GET /v1/notes "" "Authorization: Bearer $(GATEWAY_AUDIENCE=other mint "alice-$RUN" "notes:read")"
expect_code 401 "wrong audience"
NONE_HDR="$(printf '{"alg":"none","typ":"JWT"}' | openssl base64 -A | tr '+/' '-_' | tr -d '=')"
inv "$GW" GET /v1/notes "" "Authorization: Bearer $NONE_HDR.$(printf %s "$ALICE" | cut -d. -f2)."
expect_code 401 "alg=none"

step "notes through the gateway"
inv "$GW" POST /v1/notes '{"text": "hello from the edge", "tags": ["smoke"]}' "$A"
expect_code 201 "create"
ID="$(jget .id)"
[[ "$(resp_header location)" == "/v1/notes/$ID" ]] && ok "Location header passed through" || fail "location: $(resp_header location)"
[[ -n "$(resp_header ratelimit-remaining)" && -n "$(resp_header x-request-id)" ]] && ok "rate limit and request id headers" || fail "missing gateway headers"
inv "$GW" GET /v1/notes "" "$A"
expect_jq "[.notes[].id] | index($ID) != null" "listed"
inv "$GW" PUT "/v1/notes/$ID" '{"text": "edited"}' "$A"
expect_code 200 "update"
inv "$GW" GET "/v1/notes/$ID" "" "$A"
expect_jq '.text == "edited"' "read back"

step "isolation and scopes"
inv "$GW" GET "/v1/notes/$ID" "" "Authorization: Bearer $BOB" "x-gw-sub: alice-$RUN"
expect_code 404 "bob cannot read alice's note, even with a spoofed x-gw-sub"
inv "$GW" POST /v1/notes '{"text": "nope"}' "Authorization: Bearer $READER"
expect_code 403 "notes:read cannot write"
expect_jq '.error.code == "insufficient_scope"' "insufficient_scope"
inv "$GW" GET /v1/audit/recent "" "$A"
expect_code 403 "alice has no audit:read"

step "request validation"
V="Authorization: Bearer $(mint "val-$RUN" "notes:read notes:write")"
inv "$GW" POST /v1/notes 'text' "$V" "Content-Type: text/plain"
expect_code 415 "non-JSON content type"
inv "$GW" POST /v1/notes '{"text": ' "$V"
expect_code 400 "malformed JSON"
inv "$GW" GET "/v1/notes/%2e%2e/admin" "" "$V"
expect_code "400|404" "encoded traversal"
inv "$GW" GET /v1/admin "" "$V"
expect_code 404 "unknown route"
inv "$GW" DELETE /v1/audit/recent "" "Authorization: Bearer $AUDITOR"
expect_code 405 "method not allowed on the route"
inv "$GW" POST /v1/notes "{\"text\": \"$(head -c 70000 /dev/zero | tr '\0' 'x')\"}" "$V"
expect_code 413 "body over 64 KiB"

step "rate limit (10 per minute per subject)"
CAROL="Authorization: Bearer $(mint "carol-$RUN" "notes:read")"
for _ in $(seq 1 10); do inv "$GW" GET /v1/notes "" "$CAROL"; [[ "$CODE" == 200 ]] || break; done
expect_code 200 "10 requests pass"
[[ "$(resp_header ratelimit-remaining)" == "0" ]] && ok "remaining reaches 0" || fail "ratelimit-remaining: $(resp_header ratelimit-remaining)"
inv "$GW" GET /v1/notes "" "$CAROL"
expect_code 429 "11th request"
[[ -n "$(resp_header retry-after)" ]] && ok "Retry-After $(resp_header retry-after)s" || fail "no Retry-After"

step "allowed_callers is enforced by the runtime"
configure "$NOTES" '{"call": {"allowed_callers": ["some-other-service"]}}'
inv "$GW" GET /v1/notes "" "$A"
expect_code 502 "gateway no longer admitted -> 502"
expect_jq '.error.code == "upstream_unavailable"' "upstream_unavailable"
configure "$NOTES" "$(jq -nc --arg gw "$GW" '{call: {allowed_callers: [$gw]}}')"
inv "$GW" DELETE "/v1/notes/$ID" "" "$A"
expect_code 200 "admitted again; delete"

step "per-address limit (x-rt-client-ip), before the token check"
# Every request of this run came from this address, so with a limit of 2 the window is used up.
set_env "$GW" "$(jq -c '. + {GATEWAY_IP_RATE_LIMIT: "2"}' <<<"$GW_ENV")"
FORGED="Authorization: Bearer $(GATEWAY_KEY_HEX="$(rand_hex 32)" mint "mallory-$RUN" "notes:read")"
ip_limited() { inv "$GW" GET /v1/notes "" "$FORGED"; [[ "$CODE" == 429 ]]; }
if retry_until 15 ip_limited; then ok "forged tokens from one address -> 429 before any token check"; else fail "no per-address 429 (HTTP $CODE)"; fi
expect_jq '.error.code == "rate_limited"' "rate_limited"
[[ -n "$(resp_header retry-after)" ]] && ok "Retry-After $(resp_header retry-after)s" || fail "no Retry-After"
inv "$GW" GET /v1/notes "" "$A"
expect_code 429 "a valid token from the same address waits too"
set_env "$GW" "$GW_ENV"

step "audit trail (operator SDK + queue -> edge-audit)"
AUD="Authorization: Bearer $AUDITOR"
audit_has() {
  inv "$GW" GET "/v1/audit/recent?limit=200" "" "$AUD"
  [[ "$CODE" == 200 ]] && jq -e --arg a "alice-$RUN" \
    '([.events[] | select(.sub == $a and .method == "POST" and .status == 201)] | length) >= 1
     and ([.events[] | select(.reason == "call_not_allowed")] | length) >= 1
     and ([.events[] | select(.outcome == "limited" and .sub != null)] | length) >= 1
     and ([.events[] | select(.reason == "ip_rate_limited" and .sub == null and .ip != null)] | length) >= 1' "$BODY" >/dev/null
}
if retry_until 30 audit_has; then ok "create, call_not_allowed, rate-limit and per-address events recorded"; else fail "audit events missing"; fi
IP="$(jq -r '[.events[] | select(.reason == "ip_rate_limited")][0].ip // empty' "$BODY")"
inv "$GW" GET "/v1/audit/recent?sub=alice-$RUN" "" "$AUD"
expect_jq "all(.events[]; .sub == \"alice-$RUN\") and all(.events[]; .ip != null)" "filter by subject; client address recorded"
if [[ -n "$IP" ]]; then
  inv "$GW" GET "/v1/audit/recent?ip=$IP&limit=5" "" "$AUD"
  expect_jq "(.events | length) >= 1 and all(.events[]; .ip == \"$IP\")" "filter by client address ($IP)"
fi
printf '  info  shipping through the operator SDK: %s\n' "$(jq -c '.shipping' "$BODY")"
if [[ "${EXPECT_SHIPPED:-0}" == "1" ]]; then
  expect_jq '.shipping.last.ok == true' "events shipped through the SDK (by the gateway, retries by edge-audit)"
  inv "$GW" GET "/v1/audit/recent?sub=alice-$RUN" "" "$AUD"
  expect_jq 'all(.events[]; .shipped_at != null)' "alice's events are shipped"
fi

step "operator SDK update, rollback and removal without a redeploy"
PI="Authorization: Bearer $(mint "platform-$RUN" "notes:read")"
inv "$GW" GET /v1/platform "" "$PI"
expect_code 200 "GET /v1/platform: the gateway calls $SDK (sdk.invoke)"
expect_jq '.sdk == "1.0.0" and .integrations == null' "answered by 1.0.0"
GW_ID="$(_require_id "$GW")"
deployed() { api GET "/v1/services/$GW_ID/versions"; jq -c '[.[] | [.version, .source_digest]]' "$BODY"; }
BEFORE="$(deployed)"
sed 's/^const INFO_WITH_INTEGRATIONS = false;/const INFO_WITH_INTEGRATIONS = true;/' "$SDK_SRC" > "$SMOKE_TMP/acme-1.1.ts"
grep -q '^const INFO_WITH_INTEGRATIONS = true;' "$SMOKE_TMP/acme-1.1.ts" || die "cannot derive the 1.1.0 SDK source"
publish_sdk "$SDK" 1.1.0 "$SMOKE_TMP/acme-1.1.ts" stable
expect_code 201 "published $SDK 1.1.0 to stable (getPlatformInfo now lists the integrations)"
inv "$GW" GET /v1/platform "" "$PI"
expect_jq '.sdk == "1.1.0" and (.integrations | length) > 0' "the next call is answered by 1.1.0 with the new behavior"
[[ "$(deployed)" == "$BEFORE" ]] && ok "the gateway was not rebuilt or redeployed: $BEFORE" || fail "gateway versions changed: $(deployed)"
root_api PUT "/v1/sdks/$SDK/channels/stable" '{"version": "1.0.0"}'
expect_code 200 "stable rolled back to 1.0.0"
inv "$GW" GET /v1/platform "" "$PI"
expect_jq '.sdk == "1.0.0" and .integrations == null' "the next call is answered by 1.0.0 again"
root_api DELETE "/v1/sdks/$SDK"
expect_code 204 "removed $SDK"
inv "$GW" GET /v1/platform "" "$PI"
expect_code 502 "a removed SDK"
expect_jq '.error.code == "sdk_unavailable"' "calls get sdk_unavailable"
[[ "$(deployed)" == "$BEFORE" ]] && ok "still the same gateway version" || fail "gateway versions changed"

finish
