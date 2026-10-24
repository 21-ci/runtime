#!/usr/bin/env bash
# Build the edge gateway WASI P2 component and check it against the runtime's import policy.
#
#   ./build.sh            release build -> target/wasm32-wasip2/release/edge_gateway.wasm
#   ./build.sh --test     also run the unit tests (token, request, rate limit, audit, config) on the host
#
# Needs cargo with the wasm32-wasip2 target (the repo's rust-toolchain.toml lists it). Optional:
# wasm-tools, to list and check the component's imports.
set -euo pipefail
cd "$(dirname "${BASH_SOURCE[0]}")"

command -v cargo >/dev/null || { echo "error: cargo not found (install Rust: https://rustup.rs)" >&2; exit 1; }
if command -v rustup >/dev/null && ! rustup target list --installed 2>/dev/null | grep -qx wasm32-wasip2; then
  rustup target add wasm32-wasip2
fi

# wit/rt-host.wit is a vendored copy of the runtime's ../../../../wit/rt-host.wit: warn if they drift.
UPSTREAM="../../../../wit/rt-host.wit"
if [[ -f "$UPSTREAM" ]] && ! cmp -s "$UPSTREAM" wit/rt-host.wit; then
  echo "warning: wit/rt-host.wit differs from $UPSTREAM; copy it over if the host interface changed" >&2
fi

if [[ "${1:-}" == "--test" ]]; then
  cargo test --target-dir target
fi

echo "Building edge-gateway -> WASI P2 component (wasm32-wasip2, release)..."
cargo build --release --target wasm32-wasip2 --target-dir target

OUT="target/wasm32-wasip2/release/edge_gateway.wasm"
[[ -f "$OUT" ]] || { echo "error: $OUT was not produced" >&2; exit 1; }
echo "Built $OUT ($(wc -c <"$OUT" | tr -d ' ') bytes)"

# The runtime links wasi:{io,clocks,random,cli,http,filesystem,sockets}/* and std:host/*; anything
# else, or a missing wasi:http/incoming-handler export, is `422 invalid_artifact` at deploy.
if command -v wasm-tools >/dev/null; then
  WIT="$(wasm-tools component wit "$OUT")"
  grep -Eq '^\s*export wasi:http/incoming-handler@0\.2\.' <<<"$WIT" \
    || { echo "error: component does not export wasi:http/incoming-handler@0.2.x" >&2; exit 1; }
  IMPORTS="$(grep -E '^\s*import ' <<<"$WIT" | sed -E 's/^ *import //; s/;$//')"
  BAD="$(grep -Ev '^(wasi:(io|clocks|random|cli|http|filesystem|sockets)/[a-z-]+@0\.2\.[0-9]+|std:host/[a-z]+@0\.1\.0)$' <<<"$IMPORTS" || true)"
  if [[ -n "$BAD" ]]; then
    echo "error: imports outside the runtime policy:" >&2
    sed 's/^/  /' <<<"$BAD" >&2
    exit 1
  fi
  echo "Imports (all linked by the runtime):"
  sed 's/^/  /' <<<"$IMPORTS"
else
  echo "note: install wasm-tools to check the component's imports and export"
fi

echo
echo "Deploy with:"
echo "  ../../../scripts/deploy.sh $PWD/$OUT edge-gateway"
