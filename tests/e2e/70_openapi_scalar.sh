#!/usr/bin/env bash
# E2E 70: the OpenAPI 3.1 document and the Scalar reference page (API-01). The document must be
# valid JSON, cover the documented routes and (when committed) match openapi/openapi.json.
set -euo pipefail
source "$(dirname "${BASH_SOURCE[0]}")/lib.sh"

step "GET /v1/openapi.json"
op GET "$RT_API/v1/openapi.json"
expect_code 200 "openapi.json"
case "$(header content-type)" in *json*) ;; *) fail "openapi.json content-type is '$(header content-type)'" ;; esac
jq -e . "$BODY_FILE" >/dev/null || fail "openapi.json is not valid JSON"
cp "$BODY_FILE" "$WORK/openapi.json"
expect_jq '.openapi | startswith("3.1")' "OpenAPI version"
expect_jq '.info.title | type == "string" and length > 0' "info.title"
expect_jq '.paths | length >= 30' "documented paths"
expect_jq '.components.schemas.ErrorEnvelope | type == "object"' "ErrorEnvelope component"

step "the route map of docs/api.md is documented"
for p in /v1/node/status /v1/node/scheduler /v1/tenants '/v1/tenants/{t}/keys' /v1/services '/v1/services/{s}' \
         '/v1/services/{s}/versions' '/v1/services/{s}/live' '/v1/services/{s}/env' '/v1/services/{s}/secrets/{name}' \
         '/v1/services/{s}/schedules' '/v1/services/{s}/queue' '/v1/services/{s}/dlq' '/v1/services/{s}/logs' \
         '/v1/services/{s}/stats' '/v1/services/{s}/events' '/v1/services/{s}/data/export'; do
  jq -e --arg p "$p" '.paths | has($p)' "$WORK/openapi.json" >/dev/null || fail "path $p is not in the OpenAPI document"
done
jq -e '[.paths[] | to_entries[] | select(.key | IN("get","post","put","patch","delete")) | .value.responses | length] | all(. >= 1)' \
  "$WORK/openapi.json" >/dev/null || fail "an operation has no documented responses"

step "every \$ref resolves"
jq -r '[.. | objects | select(has("$ref")) | .["$ref"]] | unique[]' "$WORK/openapi.json" | while IFS= read -r ref; do
  path="${ref#\#/}"; jqpath="$(printf '%s' "$path" | awk -F/ '{for(i=1;i<=NF;i++) printf ".[\"%s\"]", $i}')"
  jq -e "$jqpath | . != null" "$WORK/openapi.json" >/dev/null || { echo "dangling \$ref $ref" >&2; exit 1; }
done || fail "the document has dangling \$ref entries"

step "the committed document (openapi/openapi.json) has not drifted from the running node"
committed="$REPO_DIR/openapi/openapi.json"
if [[ -f "$committed" ]]; then
  diff <(jq -S '.paths | keys' "$committed") <(jq -S '.paths | keys' "$WORK/openapi.json") >/dev/null \
    || fail "openapi/openapi.json lists different paths than GET /v1/openapi.json (run 'make openapi')"
else
  echo "  (no committed openapi/openapi.json yet; skipping the drift check)"
fi

step "GET /docs serves the Scalar reference as HTML"
HTTP_TOKEN="" http GET "$RT_API/docs"
expect_code 200 "/docs"
case "$(header content-type)" in text/html*) ;; *) fail "/docs content-type is '$(header content-type)'" ;; esac
grep -qi 'scalar' "$BODY_FILE" || fail "/docs does not look like the Scalar page"

pass
