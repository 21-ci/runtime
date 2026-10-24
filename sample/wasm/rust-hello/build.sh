#!/usr/bin/env bash
# Build the Rust WASI P2 `wasi:http` component and check it against the
# runtime's contract.
set -euo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")"

command -v cargo >/dev/null || { echo "error: cargo not found (install Rust: https://rustup.rs)" >&2; exit 1; }

# The repo pins a toolchain in ../../../rust-toolchain.toml (it already lists
# wasm32-wasip2). Only install the target if it is somehow missing.
if command -v rustup >/dev/null && ! rustup target list --installed 2>/dev/null | grep -qx wasm32-wasip2; then
  rustup target add wasm32-wasip2
fi

echo "Building Rust -> WASI P2 component (wasm32-wasip2, release)..."
cargo build --release --target wasm32-wasip2

OUT="target/wasm32-wasip2/release/rust_hello_wasm.wasm"
[[ -f "$OUT" ]] || { echo "error: $OUT was not produced" >&2; exit 1; }
echo "Built $OUT ($(wc -c <"$OUT" | tr -d ' ') bytes)"

# Optional contract check (needs `wasm-tools`). The runtime rejects a component
# with `422 invalid_artifact` unless it exports wasi:http/incoming-handler and
# its imports are within: wasi:{io,clocks,random,cli,http,filesystem,sockets} 0.2.x
# and std:host/* (filesystem has nothing to open, sockets deny every address).
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
echo "  ../../scripts/deploy.sh $OUT rust-hello"
