#!/usr/bin/env bash
# E2E 10: tenants and API keys: creation, scopes, secrets shown once, revocation.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

step "create a tenant"
plan="${RT_E2E_PLAN:-pro}"
op POST "$RT_API/v1/tenants" "$(jq -nc --arg n "e2e-tenant-$RANDOM" --arg p "$plan" '{name:$n, plan:$p}')"
if [[ "$HTTP_CODE" == "422" ]]; then plan="free"; op POST "$RT_API/v1/tenants" "$(jq -nc --arg n "e2e-tenant-$RANDOM" --arg p "$plan" '{name:$n, plan:$p}')"; fi
expect_code 201 "create tenant"
TENANT_ID="$(jqr .id)"
[[ "$TENANT_ID" == ten_* ]] || fail "tenant id '$TENANT_ID' is not ten_…"
expect_eq "$(jqr .plan)" "$plan" "tenant plan"

op GET "$RT_API/v1/tenants/$TENANT_ID"
expect_code 200 "get tenant"
op GET "$RT_API/v1/tenants"
expect_code 200 "list tenants"
jq -e --arg id "$TENANT_ID" '.items | map(.id) | index($id) != null' "$BODY_FILE" >/dev/null || fail "tenant is not listed"

step "create a key with deploy + service scopes; the secret is shown once"
op POST "$RT_API/v1/tenants/$TENANT_ID/keys" '{"scopes":["services:read","services:write","deploy","invoke"]}'
expect_code 201 "create key"
KEY_ID="$(jqr .id)"
SECRET="$(jqr .secret)"
[[ "$SECRET" == rtk_* ]] || fail "secret '${SECRET:0:8}…' is not rtk_…"
[[ "${#SECRET}" -gt 40 ]] || fail "secret is suspiciously short (${#SECRET} chars)"
op GET "$RT_API/v1/tenants/$TENANT_ID/keys"
expect_code 200 "list keys"
if grep -q "${SECRET##*_}" "$BODY_FILE"; then fail "the key secret reappears in GET /keys"; fi
jq -e '(if type == "array" then . else .items end) | all(has("secret") | not)' "$BODY_FILE" >/dev/null || fail "key listing must not contain a secret field"

step "the key authenticates and is bound to its tenant"
TENANT_KEY="$SECRET"
tkey GET "$RT_API/v1/services"
expect_code 200 "list services with the tenant key"
expect_jq '.items | length == 0' "a new tenant has no services"
tkey GET "$RT_API/v1/tenants"
expect_error_code 403 insufficient_scope "tenant keys cannot use operator routes"
tkey GET "$RT_API/v1/node/status"
expect_error_code 403 insufficient_scope "tenant keys cannot read node status"

step "scopes are enforced: a read-only key cannot create services"
op POST "$RT_API/v1/tenants/$TENANT_ID/keys" '{"scopes":["services:read"]}'
expect_code 201 "create read-only key"
RO_KEY="$(jqr .secret)"
HTTP_TOKEN="$RO_KEY" http POST "$RT_API/v1/services" '{"slug":"e2e-ro-denied","runtime":"deno"}'
expect_error_code 403 insufficient_scope "read-only key creating a service"
HTTP_TOKEN="$RO_KEY" http GET "$RT_API/v1/services"
expect_code 200 "read-only key listing services"

step "revoking a key takes effect within a second"
op DELETE "$RT_API/v1/tenants/$TENANT_ID/keys/$KEY_ID"
expect_code 204 "revoke key"
sleep 1
tkey GET "$RT_API/v1/services"
expect_error_code 401 unauthorized "revoked key"

pass
