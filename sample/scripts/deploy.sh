#!/usr/bin/env bash
# Deploy a service version to the runtime control API (docs/api.md) and make it live.
#
#   deploy.sh <source> [slug]
#
#   <source>  one of
#     file.ts | file.js | file.mjs   single file    (Content-Type: application/typescript | text/javascript)
#     file.wasm                      WASI P2 component (application/wasm, runtime=wasm)
#     file.zip | file.tar            archive         (application/zip | application/x-tar)
#     <dir>                          directory:
#         with rt.json {entry, imports?}  -> JSON bundle {entry, files, imports?} built with jq
#         without rt.json                 -> a lone source file is sent as-is, several files as a JSON bundle
#   [slug]    service slug (^[a-z][a-z0-9-]{1,62}$); default: derived from the file/dir name
#
# Steps: find the service by slug (create it if missing: POST /v1/services),
#        POST /v1/services/{id}/versions?activate=1, print the invoke URL.
#
# Env:
#   RT_API      control API base            (default http://127.0.0.1:8081)
#   RT_INVOKE   invoke listener base        (default http://127.0.0.1:8080)
#   RT_KEY      Bearer token, root token or tenant key `rtk_...` (default: $RT_ROOT_TOKEN).
#               Empty only works against a debug build with no root token.
#   RT_COMPILE  lazy | eager  (default: server profile). `eager` waits until the build is ready.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

die() { echo "error: $*" >&2; exit 1; }

usage() { sed -n '2,/^set -euo/{/^set -euo/d;s/^# \{0,1\}//;p;}' "${BASH_SOURCE[0]}"; }

[[ "${1:-}" == "-h" || "${1:-}" == "--help" ]] && { usage; exit 0; }
[[ $# -ge 1 && $# -le 2 ]] || { usage >&2; exit 2; }

for bin in curl jq; do
  command -v "$bin" >/dev/null 2>&1 || die "'$bin' is required but not installed"
done

SOURCE="$1"
RT_API="${RT_API:-http://127.0.0.1:8081}";    RT_API="${RT_API%/}"
RT_INVOKE="${RT_INVOKE:-http://127.0.0.1:8080}"; RT_INVOKE="${RT_INVOKE%/}"
RT_KEY="${RT_KEY:-${RT_ROOT_TOKEN:-}}"
RT_COMPILE="${RT_COMPILE:-}"
export RT_API RT_KEY

case "$RT_COMPILE" in ""|lazy|eager) ;; *) die "RT_COMPILE must be 'lazy' or 'eager'" ;; esac
[[ -n "$RT_KEY" ]] || echo "warning: RT_KEY is empty; this only works against a debug build without a root token" >&2

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT
RESP="$WORK/resp.json"

# Keep the token out of `ps`: hand it to curl through a config file descriptor.
auth_cfg() { [[ -n "$RT_KEY" ]] && printf 'header = "Authorization: Bearer %s"\n' "$RT_KEY" || true; }

# call METHOD PATH [CONTENT_TYPE [BODY_FILE]]  ->  HTTP status in $CODE, body in $RESP
call() {
  local method="$1" path="$2" ctype="${3:-}" body="${4:-}"
  local args=(-sS -X "$method" -o "$RESP" -w '%{http_code}')
  [[ -n "$ctype" ]] && args+=(-H "Content-Type: $ctype")
  [[ -n "$body" ]]  && args+=(--data-binary "@$body")
  CODE="$(curl "${args[@]}" -K <(auth_cfg) "$RT_API$path")" \
    || die "cannot reach the control API: $method $RT_API$path"
}

# Print the control error envelope {"error": {code, message}} (or the raw body) and exit.
fail_resp() {
  echo "error: $1 -> HTTP $CODE" >&2
  if jq -e '.error.code' "$RESP" >/dev/null 2>&1; then
    jq -r '.error | "  \(.code): \(.message)"' "$RESP" >&2
    jq -e '.error.details | select(. != null and . != {})' "$RESP" >/dev/null 2>&1 \
      && jq '.error.details' "$RESP" >&2
  else
    # 422 on a bad deno source is a list of {file, line, col, message}
    jq . "$RESP" >&2 2>/dev/null || cat "$RESP" >&2
  fi
  exit 1
}

# ---------------------------------------------------------------- slug / runtime
slugify() { printf '%s' "$1" | tr 'A-Z_.' 'a-z--' | sed -E 's/[^a-z0-9-]+/-/g; s/-+/-/g; s/^-+//; s/-+$//'; }

RUNTIME="deno"
ARTIFACT=""
CONTENT_TYPE=""
DEFAULT_SLUG=""

# Single-file source: sets CONTENT_TYPE / RUNTIME; ARTIFACT is the file itself.
single_file() {
  local f="$1"
  case "$f" in
    *.ts)  CONTENT_TYPE="application/typescript" ;;
    *.js|*.mjs) CONTENT_TYPE="text/javascript" ;;
    *.wasm) CONTENT_TYPE="application/wasm"; RUNTIME="wasm" ;;
    *.zip) CONTENT_TYPE="application/zip" ;;
    *.tar) CONTENT_TYPE="application/x-tar" ;;
    *) die "unsupported file type: $f (use .ts, .js, .wasm, .zip, .tar or a directory)" ;;
  esac
  ARTIFACT="$f"
}

# Directory with rt.json (or several sources): JSON bundle {entry, files, imports?}.
dir_bundle() {
  local dir="$1" entry="" imports="null" rel
  local files_list="$WORK/files.list" parts="$WORK/parts.jsonl"

  if [[ -f "$dir/rt.json" ]]; then
    jq -e 'type == "object"' "$dir/rt.json" >/dev/null 2>&1 || die "$dir/rt.json is not a JSON object"
    entry="$(jq -r '.entry // empty' "$dir/rt.json")"
    imports="$(jq -c '.imports // null' "$dir/rt.json")"
  fi

  # Text sources only; skip dot files/dirs, node_modules and rt.json itself.
  ( cd "$dir" && find . -type f \
      \( -name '*.ts' -o -name '*.tsx' -o -name '*.mts' -o -name '*.js' -o -name '*.jsx' -o -name '*.mjs' -o -name '*.json' \) \
      ! -path '*/node_modules/*' ! -path '*/.*' ! -name rt.json -print | sed 's|^\./||' | LC_ALL=C sort ) > "$files_list"
  [[ -s "$files_list" ]] || die "no source files found in $dir"

  if [[ -z "$entry" ]]; then
    for rel in main.ts main.js index.ts index.js mod.ts; do
      [[ -f "$dir/$rel" ]] && { entry="$rel"; break; }
    done
    [[ -n "$entry" ]] || die "cannot guess the entry point in $dir; add rt.json with {\"entry\": \"...\"}"
  fi
  [[ -f "$dir/$entry" ]] || die "entry '$entry' not found in $dir"

  # Without rt.json and with a single source file, a plain single-file upload is enough.
  if [[ ! -f "$dir/rt.json" && "$(wc -l < "$files_list" | tr -d ' ')" == "1" ]]; then
    single_file "$dir/$entry"
    return
  fi

  : > "$parts"
  while IFS= read -r rel; do
    jq -n --arg p "$rel" --rawfile c "$dir/$rel" '{($p): $c}' >> "$parts"
  done < "$files_list"
  jq -s 'add' "$parts" > "$WORK/files.json"

  ARTIFACT="$WORK/bundle.json"
  jq -n --arg entry "$entry" --slurpfile files "$WORK/files.json" --argjson imports "$imports" \
    '{entry: $entry, files: $files[0]} + (if $imports == null then {} else {imports: $imports} end)' > "$ARTIFACT"
  CONTENT_TYPE="application/json"
}

if [[ -f "$SOURCE" ]]; then
  single_file "$SOURCE"
  DEFAULT_SLUG="$(slugify "$(basename "${SOURCE%.*}")")"
elif [[ -d "$SOURCE" ]]; then
  dir_bundle "$SOURCE"
  DEFAULT_SLUG="$(slugify "$(basename "$(cd "$SOURCE" && pwd)")")"
else
  die "source not found: $SOURCE"
fi

SLUG="${2:-$DEFAULT_SLUG}"
[[ "$SLUG" =~ ^[a-z][a-z0-9-]{1,62}$ ]] \
  || die "invalid slug '$SLUG' (must match ^[a-z][a-z0-9-]{1,62}\$); pass one explicitly: deploy.sh <source> <slug>"

SIZE="$(wc -c < "$ARTIFACT" | tr -d ' ')"

# ---------------------------------------------------------------- 1. find or create the service
SVC_ID=""
if SVC_ID="$(bash "$SCRIPT_DIR/svc-id.sh" "$SLUG")"; then
  echo "Service : $SVC_ID (slug: $SLUG, existing)"
else
  rc=$?
  [[ $rc -eq 1 ]] || exit "$rc"          # 2 = a real error, already printed by svc-id.sh
  jq -n --arg slug "$SLUG" --arg runtime "$RUNTIME" --arg name "$SLUG" \
    '{slug: $slug, runtime: $runtime, name: $name}' > "$WORK/create.json"
  call POST /v1/services application/json "$WORK/create.json"
  case "$CODE" in
    2*) SVC_ID="$(jq -r '.id // empty' "$RESP")"
        [[ -n "$SVC_ID" ]] || die "POST /v1/services returned no id"
        echo "Service : $SVC_ID (slug: $SLUG, runtime: $RUNTIME, created)" ;;
    409) # slug_taken: somebody created it between our lookup and the POST
        SVC_ID="$(bash "$SCRIPT_DIR/svc-id.sh" "$SLUG")" || fail_resp "POST /v1/services"
        echo "Service : $SVC_ID (slug: $SLUG, existing)" ;;
    *)  fail_resp "POST /v1/services" ;;
  esac
fi

# ---------------------------------------------------------------- 2. upload + activate
QUERY="activate=1"
[[ -n "$RT_COMPILE" ]] && QUERY+="&compile=$RT_COMPILE"
echo "Upload  : $CONTENT_TYPE, $SIZE bytes"
call POST "/v1/services/$SVC_ID/versions?$QUERY" "$CONTENT_TYPE" "$ARTIFACT"
case "$CODE" in 2*) ;; *) fail_resp "POST /v1/services/$SVC_ID/versions" ;; esac

VERSION="$(jq -r '.version // empty' "$RESP")"
STATE="$(jq -r '.build_state // empty' "$RESP")"
DIGEST="$(jq -r '.source_digest // empty' "$RESP")"
echo "Version : ${VERSION:-?} (build_state: ${STATE:-?}${DIGEST:+, $DIGEST})"

# lazy: the first request triggers the build. eager (202): `live` flips only once the build is ready.
if [[ "$RT_COMPILE" == "eager" || "$CODE" == "202" ]] && [[ -n "$VERSION" ]]; then
  echo -n "Building: "
  for _ in $(seq 1 120); do
    call GET "/v1/services/$SVC_ID/versions/$VERSION"
    [[ "$CODE" == 2* ]] || fail_resp "GET /v1/services/$SVC_ID/versions/$VERSION"
    STATE="$(jq -r '.build_state // empty' "$RESP")"
    case "$STATE" in
      ready)  echo "ready"; break ;;
      failed) echo "failed"; jq '.failure' "$RESP" >&2; exit 1 ;;
    esac
    sleep 1
  done
  [[ "$STATE" == "ready" ]] || { echo "still '$STATE' after 120 s (check GET /v1/services/$SVC_ID/versions/$VERSION/build/logs)"; }
fi

echo
echo "Live. Invoke at:"
echo "  $RT_INVOKE/s/$SLUG/"
echo "  $SCRIPT_DIR/invoke.sh $SLUG /"
