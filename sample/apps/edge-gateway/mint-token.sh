#!/usr/bin/env bash
# Mint a short-lived HS256 JWT for the edge gateway (local use and the smoke test).
#
#   GATEWAY_KEY_HEX=<hex key> ./mint-token.sh <sub> "<scopes>" [ttl_seconds]
#   GATEWAY_KEY_HEX=$KEY ./mint-token.sh alice "notes:read notes:write" 300
#
# Env: GATEWAY_KEY_HEX   the key, as in GATEWAY_KEYS (kid:hex) on the gateway (required)
#      GATEWAY_KID       key id (default k1)
#      GATEWAY_AUDIENCE  aud claim (default edge-gateway)
#      GATEWAY_ISSUER    iss claim (optional)
#      MINT_NOW          issue time in unix seconds (default: now), e.g. to mint an expired token
#
# The key is passed to openssl on its command line, so other local users could see it in `ps`:
# use this on a development machine. Production clients mint tokens in their own code (any JWT
# library with HS256, or HMAC-SHA256 over "<b64url header>.<b64url payload>").
set -euo pipefail

[[ $# -ge 1 && $# -le 3 ]] || { sed -n '2,15p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//' >&2; exit 2; }
for bin in openssl jq; do command -v "$bin" >/dev/null || { echo "error: $bin is required" >&2; exit 1; }; done

SUB="$1"
SCOPES="${2-}"
TTL="${3:-300}"
KEY="${GATEWAY_KEY_HEX:?set GATEWAY_KEY_HEX}"
NOW="${MINT_NOW:-$(date +%s)}"

b64url() { openssl base64 -A | tr '+/' '-_' | tr -d '='; }

HEADER="$(jq -jnc --arg kid "${GATEWAY_KID:-k1}" '{alg: "HS256", typ: "JWT", kid: $kid}' | b64url)"
PAYLOAD="$(jq -jnc --arg sub "$SUB" --arg scope "$SCOPES" --arg aud "${GATEWAY_AUDIENCE:-edge-gateway}" \
  --arg iss "${GATEWAY_ISSUER:-}" --argjson iat "$NOW" --argjson ttl "$TTL" \
  '{sub: $sub, aud: $aud, iat: $iat, exp: ($iat + $ttl)}
   + (if $scope == "" then {} else {scope: $scope} end)
   + (if $iss == "" then {} else {iss: $iss} end)' | b64url)"
SIG="$(printf '%s.%s' "$HEADER" "$PAYLOAD" | openssl dgst -sha256 -mac HMAC -macopt "hexkey:$KEY" -binary | b64url)"
printf '%s.%s.%s\n' "$HEADER" "$PAYLOAD" "$SIG"
