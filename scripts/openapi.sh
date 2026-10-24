#!/usr/bin/env bash
# Regenerate openapi/openapi.json from the code: `rt openapi > openapi/openapi.json`.
#
#   scripts/openapi.sh            write openapi/openapi.json (atomically; the old file stays on failure)
#   scripts/openapi.sh --check    do not write; exit 1 when the committed file differs (API-01 drift check)
#
# The document comes from rt-api (`rt_api::openapi_json_string()`), exposed by the `rt openapi`
# subcommand of the rt-server binary. That subcommand is added by the rt-server lane; until it exists
# this script stops with an explanation instead of writing an empty file.
#
# Env: CARGO (default: cargo), OPENAPI_OUT (default: openapi/openapi.json)
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

die() { echo "error: $*" >&2; exit 1; }
usage() { sed -n '2,/^set -euo/{/^set -euo/d;s/^# \{0,1\}//;p;}' "${BASH_SOURCE[0]}"; }

CHECK=0
case "${1:-}" in
  -h|--help) usage; exit 0 ;;
  --check)   CHECK=1 ;;
  "")        ;;
  *)         die "unknown option '$1' (see --help)" ;;
esac

CARGO="${CARGO:-cargo}"
OUT="${OPENAPI_OUT:-openapi/openapi.json}"
command -v "$CARGO" >/dev/null 2>&1 || die "$CARGO not found (install Rust: https://rustup.rs)"
command -v jq >/dev/null 2>&1 || die "jq is required to validate the generated document"

RT=("$CARGO" run --quiet -p rt-server --bin rt --)

# Fail clearly when the subcommand is not there yet (clap exits 2 with "unrecognized subcommand").
if ! PROBE="$("${RT[@]}" openapi --help 2>&1)"; then
  if grep -qiE "unrecognized subcommand|unexpected argument|found argument" <<<"$PROBE"; then
    die "the \`rt openapi\` subcommand is missing from crates/rt-server (src/cli.rs, main.rs). It must print
       rt_api::openapi_json_string() to stdout; the rt-server lane adds it. Nothing was written."
  fi
  echo "$PROBE" | tail -n 20 >&2
  die "building or running \`rt\` failed (output above). Nothing was written."
fi

TMP="$(mktemp "${TMPDIR:-/tmp}/rt-openapi.XXXXXX")"
trap 'rm -f "$TMP"' EXIT

"${RT[@]}" openapi > "$TMP" || die "\`rt openapi\` failed"
[[ -s "$TMP" ]] || die "\`rt openapi\` printed nothing"
jq -e '.openapi and .paths' "$TMP" >/dev/null 2>&1 \
  || die "\`rt openapi\` output is not an OpenAPI document (no .openapi / .paths)"

if [[ "$CHECK" == 1 ]]; then
  [[ -f "$OUT" ]] || die "$OUT does not exist; run scripts/openapi.sh"
  if diff -u "$OUT" "$TMP" >&2; then
    echo "$OUT is up to date"
  else
    die "$OUT is out of date; run: make openapi"
  fi
  exit 0
fi

mkdir -p "$(dirname "$OUT")"
if [[ -f "$OUT" ]] && cmp -s "$OUT" "$TMP"; then
  echo "$OUT is already up to date"
else
  cat "$TMP" > "$OUT"
  echo "wrote $OUT ($(jq '.paths | length' "$OUT") paths)"
fi
