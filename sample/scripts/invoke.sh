#!/usr/bin/env bash
# Invoke a deployed service: ANY /s/{slug}/{path} on the public listener.
#
#   invoke.sh <slug | http(s)://base> [path] [METHOD] [body]
#
#   path    default "/"; may carry a query string, e.g. '/?name=Alice'
#   METHOD  default GET
#   body    inline text (JSON is detected) or @file to send a file as-is
#
# Env:
#   RT_INVOKE        invoke listener base (default http://127.0.0.1:8080)
#   RT_INVOKE_KEY    optional `rtk_...` tenant key with the `invoke` scope, only for access=key services
#   RT_CONTENT_TYPE  override the request Content-Type (default: application/json if the body
#                    starts with { or [ or is a quoted string, otherwise text/plain)
#   RT_HEADERS       extra request headers, newline separated ("Name: value")
#
# Exit status: 0 for 1xx-3xx, 22 for HTTP >= 400 (like `curl --fail`).
set -euo pipefail

die() { echo "error: $*" >&2; exit 1; }

usage() { sed -n '2,/^set -euo/{/^set -euo/d;s/^# \{0,1\}//;p;}' "${BASH_SOURCE[0]}"; }

[[ "${1:-}" == "-h" || "${1:-}" == "--help" ]] && { usage; exit 0; }
[[ $# -ge 1 && $# -le 4 ]] || { usage >&2; exit 2; }

for bin in curl jq; do
  command -v "$bin" >/dev/null 2>&1 || die "'$bin' is required but not installed"
done

DEST="$1"
REQ_PATH="${2:-/}"
METHOD="$(printf '%s' "${3:-GET}" | tr 'a-z' 'A-Z')"
BODY="${4-}"

RT_INVOKE="${RT_INVOKE:-http://127.0.0.1:8080}"; RT_INVOKE="${RT_INVOKE%/}"
RT_INVOKE_KEY="${RT_INVOKE_KEY:-}"

# NB: never name a variable PATH here, it would break command lookup.
case "$REQ_PATH" in /*) ;; *) REQ_PATH="/$REQ_PATH" ;; esac   # '?a=1' -> '/?a=1', 'x' -> '/x'

if [[ "$DEST" == http://* || "$DEST" == https://* ]]; then
  URL="${DEST%/}$REQ_PATH"
else
  URL="$RT_INVOKE/s/$DEST$REQ_PATH"
fi

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

# Keep the key out of `ps`: hand it to curl through a config file descriptor.
auth_cfg() { [[ -n "$RT_INVOKE_KEY" ]] && printf 'header = "Authorization: Bearer %s"\n' "$RT_INVOKE_KEY" || true; }

ARGS=(-sS -o "$WORK/body" -D "$WORK/headers" -w '%{http_code}')
if [[ "$METHOD" == "HEAD" ]]; then ARGS+=(--head); else ARGS+=(-X "$METHOD"); fi

if [[ -n "$BODY" ]]; then
  if [[ "$BODY" == @* ]]; then
    ARGS+=(--data-binary "$BODY")
    CT="${RT_CONTENT_TYPE:-application/octet-stream}"
  else
    ARGS+=(--data-binary "$BODY")
    if [[ "$BODY" == \{* || "$BODY" == \[* || "$BODY" == \"* ]]; then CT="${RT_CONTENT_TYPE:-application/json}"
    else CT="${RT_CONTENT_TYPE:-text/plain}"; fi
  fi
  ARGS+=(-H "Content-Type: $CT")
fi
if [[ -n "${RT_HEADERS:-}" ]]; then
  while IFS= read -r h; do [[ -n "$h" ]] && ARGS+=(-H "$h"); done <<<"$RT_HEADERS"
fi

echo "-> $METHOD $URL"
[[ -n "$BODY" ]] && echo "   body: $BODY"

CODE="$(curl "${ARGS[@]}" -K <(auth_cfg) "$URL")" || die "request failed (is the invoke listener reachable at $RT_INVOKE?)"

echo "<- HTTP $CODE"
# Headers the runtime adds plus the error reason (docs/api.md).
# `|| true`: grep exits 1 when the response carries none of them.
tr -d '\r' < "$WORK/headers" \
  | grep -iE '^(content-type|x-rt-task-id|x-rt-version|x-rt-reason|server-timing|retry-after):' | sed 's/^/   /' || true
echo

CTYPE="$(tr -d '\r' < "$WORK/headers" | grep -i '^content-type:' | tail -n 1 | tr 'A-Z' 'a-z' || true)"
if [[ "$METHOD" == "HEAD" || ! -s "$WORK/body" ]]; then
  :   # nothing to print (curl --head writes the headers to the body file)
elif [[ "$CTYPE" == *json* ]] && jq . "$WORK/body" 2>/dev/null; then
  :
elif [[ -z "$CTYPE" || "$CTYPE" == *text/* || "$CTYPE" == *xml* || "$CTYPE" == *javascript* || "$CTYPE" == *json* ]]; then
  cat "$WORK/body"; echo
else
  echo "(binary body, $(wc -c < "$WORK/body" | tr -d ' ') bytes; fetch it with: curl -o out '$URL')"
fi

[[ "$CODE" -lt 400 ]] || exit 22
