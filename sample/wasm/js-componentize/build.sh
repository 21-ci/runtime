#!/usr/bin/env bash
# Build src/app.js into a WASI P2 `wasi:http` component with `jco componentize`
# and check it against the runtime's contract.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

command -v node >/dev/null || { echo "error: node not found (need Node.js >= 20)" >&2; exit 1; }
command -v npm  >/dev/null || { echo "error: npm not found" >&2; exit 1; }

if [[ ! -d node_modules ]]; then
  echo "Installing dependencies (npm ci)..."
  npm ci --no-audit --no-fund
fi

mkdir -p dist

# --disable stdio keeps the import set small (without it the engine also imports
# wasi:cli/stdin and wasi:cli/terminal-*, which the runtime links too, so the flag
# is optional). Trade-off: console.log() output is not captured; drop the flag to
# get it in the service logs.
# Keep `http` (needed for the fetch event) and `clocks`/`random` (all allowed).
echo "Componentizing src/app.js -> dist/app.wasm ..."
npx --no-install jco componentize src/app.js \
  --wit wit \
  --world-name hello \
  --disable stdio \
  --out dist/app.wasm

OUT="dist/app.wasm"
[[ -f "$OUT" ]] || { echo "error: $OUT was not produced" >&2; exit 1; }
echo "Built $OUT ($(wc -c <"$OUT" | tr -d ' ') bytes)"

# Optional contract check (needs `wasm-tools`): the runtime answers
# `422 invalid_artifact` if the component lacks wasi:http/incoming-handler or
# imports something it does not link: anything but wasi:{io,clocks,random,cli,http,
# filesystem,sockets} 0.2.x and std:host/* (filesystem has nothing to open, sockets
# deny every address).
if command -v wasm-tools >/dev/null; then
  WIT="$(wasm-tools component wit "$OUT")"
  grep -Eq '^\s*export wasi:http/incoming-handler' <<<"$WIT" \
    || { echo "error: component does not export wasi:http/incoming-handler" >&2; exit 1; }
  IMPORTS="$(grep -E '^\s*import ' <<<"$WIT" | sed -E 's/^ *import //; s/;$//')"
  BAD="$(grep -Ev '^(wasi:(http|io|clocks|random|cli|filesystem|sockets)/[a-z-]+@0\.2|std:host/)' <<<"$IMPORTS" || true)"
  if [[ -n "$BAD" ]]; then
    echo "error: imports outside the runtime allowlist:" >&2
    sed 's/^/  /' <<<"$BAD" >&2
    exit 1
  fi
  echo "Imports (all allowed):"
  sed 's/^/  /' <<<"$IMPORTS"
else
  echo "note: install wasm-tools to verify exports/imports against the runtime allowlist"
fi

echo
echo "Deploy with:"
echo "  ../../scripts/deploy.sh $OUT js-componentize"
