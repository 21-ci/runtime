#!/usr/bin/env bash
# Build every WASM guest fixture under fixtures/wasm/<name>/ into fixtures/wasm/out/<name>.wasm.
#
#   scripts/build-fixtures.sh            # build all
#   scripts/build-fixtures.sh hello kv   # build selected fixtures
#
# Each fixture is a standalone crate (own `[workspace]`, own Cargo.lock) built for
# `wasm32-wasip2`: a cdylib for that target is already a WebAssembly *component* exporting
# `wasi:http/incoming-handler`. Tests in crates/rt-exec-wasm load `fixtures/wasm/out/*.wasm`.
#
# Also writes two deliberately invalid artifacts (no toolchain needed):
#   out/invalid-core.wasm  a plain core module (WSM-08: `422 invalid_artifact`, not a component)
#   out/no-handler.wasm    a valid but empty component (WSM-08: no `wasi:http/incoming-handler`)
#
# Disk is tight (V8 + wasmtime): all fixtures share ONE cargo target dir under runtime/target.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FIXTURES="$ROOT/fixtures/wasm"
OUT="$FIXTURES/out"
export CARGO_TARGET_DIR="${FIXTURE_TARGET_DIR:-$ROOT/target/fixtures-wasm}"

command -v cargo >/dev/null || { echo "error: cargo not found (install Rust: https://rustup.rs)" >&2; exit 1; }
if command -v rustup >/dev/null && ! rustup target list --installed 2>/dev/null | grep -qx wasm32-wasip2; then
  rustup target add wasm32-wasip2
fi

mkdir -p "$OUT"

if [[ $# -gt 0 ]]; then
  NAMES=("$@")
else
  NAMES=()
  for dir in "$FIXTURES"/*/; do
    name="$(basename "$dir")"
    [[ "$name" == _* || "$name" == out || ! -f "$dir/Cargo.toml" ]] && continue
    NAMES+=("$name")
  done
fi

built=()
for name in "${NAMES[@]}"; do
  dir="$FIXTURES/$name"
  [[ -f "$dir/Cargo.toml" ]] || { echo "error: no fixture crate at $dir" >&2; exit 1; }
  pkg="$(sed -n 's/^name *= *"\(.*\)"/\1/p' "$dir/Cargo.toml" | head -n1)"
  artifact="${pkg//-/_}.wasm"
  echo "==> $name ($pkg)"
  ( cd "$dir" && cargo build --release --target wasm32-wasip2 --quiet )
  cp "$CARGO_TARGET_DIR/wasm32-wasip2/release/$artifact" "$OUT/$name.wasm"
  built+=("$name")
done

# Deliberately invalid artifacts (raw bytes).
#   core module: (module (func (export "_start")))
printf '\x00asm\x01\x00\x00\x00\x01\x04\x01\x60\x00\x00\x03\x02\x01\x00\x07\x0a\x01\x06_start\x00\x00\x0a\x04\x01\x02\x00\x0b' \
  > "$OUT/invalid-core.wasm"
#   empty component: magic, version 0x0d, layer 0x01
printf '\x00asm\x0d\x00\x01\x00' > "$OUT/no-handler.wasm"

# WLOAD-004: `hello` with a second memory in its core module (a multi-memory component), derived
# from out/hello.wasm: print to WAT, add the memory after the first one, parse back.
if command -v wasm-tools >/dev/null && [[ -f "$OUT/hello.wasm" ]]; then
  tmp="$(mktemp -d)"
  wasm-tools print "$OUT/hello.wasm" > "$tmp/hello.wat"
  awk '{ print } /^    \(memory \(;0;\) [0-9]+\)$/ && !done { print "    (memory (;1;) 1)"; done = 1 }' \
    "$tmp/hello.wat" > "$tmp/multimem.wat"
  grep -q '^    (memory (;1;) 1)$' "$tmp/multimem.wat" || { echo "error: could not add a second memory to hello" >&2; exit 1; }
  wasm-tools parse "$tmp/multimem.wat" -o "$OUT/multimem.wasm"
  wasm-tools validate --features all "$OUT/multimem.wasm"
  rm -rf "$tmp"
fi

# Optional contract check (needs `wasm-tools`): every built fixture is a component that exports
# wasi:http/incoming-handler and imports only what contracts C-7 links.
if command -v wasm-tools >/dev/null; then
  for name in "${built[@]}"; do
    wasm-tools validate "$OUT/$name.wasm"
    wit="$(wasm-tools component wit "$OUT/$name.wasm")"
    grep -Eq '^\s*export wasi:http/incoming-handler@0\.2\.' <<<"$wit" \
      || { echo "error: $name does not export wasi:http/incoming-handler" >&2; exit 1; }
    bad="$(grep -E '^\s*import ' <<<"$wit" | sed -E 's/^ *import //; s/;$//' \
      | grep -Ev '^(wasi:(io|clocks|random|cli|http|filesystem|sockets)/[a-z-]+@0\.2\.[0-9]+|std:host/[a-z-]+(@[0-9.]+)?)$' || true)"
    [[ -z "$bad" ]] || { echo "error: $name imports outside the C-7 policy:" >&2; sed 's/^/  /' <<<"$bad" >&2; exit 1; }
  done
  wasm-tools validate "$OUT/invalid-core.wasm"
  wasm-tools validate "$OUT/no-handler.wasm"
else
  echo "note: install wasm-tools to verify exports/imports against contracts C-7"
fi

echo
echo "fixtures in $OUT:"
( cd "$OUT" && ls -l -- *.wasm | awk '{printf "  %-22s %8d bytes\n", $NF, $5}' )
