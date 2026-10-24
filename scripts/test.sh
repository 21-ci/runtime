#!/usr/bin/env bash
# Unit + integration tests, then the Python tooling tests.
#
#   scripts/test.sh [--fast] [--perf | --perf-only] [-- <extra cargo test args>]
#
# Steps:
#   1. scripts/build-fixtures.sh              WASM guests -> fixtures/wasm/out/*.wasm (rt-exec-wasm tests load them)
#   2. cargo test --workspace                 unit tests, crates/*/tests, doc-tests
#   3. python3 -m unittest (tools/test_*.py)  menuconfig / config CLI
#   4. --perf: cargo test --release -p rt-sched -p rt-exec-wasm -- --ignored --nocapture
#
# Flags:
#   --fast        skip the fixture rebuild when fixtures/wasm/out already has .wasm files, skip doc-tests
#                 (cargo test --lib --bins --tests) and skip the Python tests
#   --perf        also run the `#[ignore = "perf"]` benchmarks (release build, slow) after the normal tests
#   --perf-only   run only step 4 (what `make test-perf` does)
#   -- ARGS       appended to the step-2 `cargo test` command, e.g. `-- -p rt-sched` or `-- ac_sch_03`
#
# Env: RT_PERF_CRATES  crates whose perf tests to run (default: every crate with an `#[ignore = "perf"]` test)
# Runs every step even when one fails, prints a summary, exits non-zero if any step failed.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

die() { echo "error: $*" >&2; exit 1; }
usage() { sed -n '2,/^set -euo/{/^set -euo/d;s/^# \{0,1\}//;p;}' "${BASH_SOURCE[0]}"; }

FAST=0 PERF=0 PERF_ONLY=0
CARGO_EXTRA=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)   usage; exit 0 ;;
    --fast)      FAST=1 ;;
    --perf)      PERF=1 ;;
    --perf-only) PERF=1; PERF_ONLY=1 ;;
    --)          shift; CARGO_EXTRA=("$@"); break ;;
    *)           die "unknown option '$1' (see --help)" ;;
  esac
  shift
done

command -v cargo >/dev/null 2>&1 || die "cargo not found (install Rust: https://rustup.rs)"

RESULTS=()
FAILED=0
step() {   # step <label> <cmd...>
  local label="$1" start rc=0
  shift
  echo
  echo "==> $label"
  start="$(date +%s)"
  "$@" || rc=$?
  local took=$(( $(date +%s) - start ))
  if [[ $rc -eq 0 ]]; then RESULTS+=("PASS  $label (${took}s)")
  else RESULTS+=("FAIL  $label (${took}s, exit $rc)"); FAILED=1; fi
  return 0
}

has_fixtures() { compgen -G "$ROOT/fixtures/wasm/out/*.wasm" >/dev/null; }

if [[ "$PERF_ONLY" == 0 ]]; then
  # Without the fixtures every rt-exec-wasm test fails with a confusing "file not found".
  if [[ "$FAST" == 1 ]] && has_fixtures; then
    echo "==> fixtures: reusing fixtures/wasm/out (--fast)"
  else
    scripts/build-fixtures.sh || die "building the WASM fixtures failed (rustup target add wasm32-wasip2?)"
  fi

  # `-- -p <crate>` narrows the run; --workspace would override it and test every crate.
  SCOPE=(--workspace)
  for a in ${CARGO_EXTRA[@]+"${CARGO_EXTRA[@]}"}; do
    [[ "$a" == -p || "$a" == --package || "$a" == --package=* || "$a" == -p?* ]] && SCOPE=()
  done
  if [[ "$FAST" == 1 ]]; then
    step "cargo test ${SCOPE[*]:-} --lib --bins --tests" \
      cargo test ${SCOPE[@]+"${SCOPE[@]}"} --lib --bins --tests ${CARGO_EXTRA[@]+"${CARGO_EXTRA[@]}"}
  else
    step "cargo test ${SCOPE[*]:-}" cargo test ${SCOPE[@]+"${SCOPE[@]}"} ${CARGO_EXTRA[@]+"${CARGO_EXTRA[@]}"}
  fi

  if [[ "$FAST" == 0 ]]; then
    if command -v python3 >/dev/null 2>&1; then
      step "python3 -m unittest (tools)" python3 -m unittest discover -s tools -p 'test_*.py' -v
    else
      RESULTS+=("FAIL  python3 -m unittest (tools): python3 not found"); FAILED=1
    fi
  fi
fi

if [[ "$PERF" == 1 ]]; then
  if [[ "$PERF_ONLY" == 1 ]] && ! has_fixtures; then
    scripts/build-fixtures.sh || die "building the WASM fixtures failed (rustup target add wasm32-wasip2?)"
  fi
  PERF_ARGS=()
  # Found, not listed: a crate that gains a perf test is picked up without editing this script.
  PERF_CRATES="${RT_PERF_CRATES:-$(grep -rl 'ignore = "perf"' crates | cut -d/ -f2 | sort -u | tr '\n' ' ')}"
  for c in $PERF_CRATES; do PERF_ARGS+=(-p "$c"); done
  step "perf tests (release, --ignored)" \
    cargo test --release "${PERF_ARGS[@]}" -- --ignored --nocapture
fi

echo
echo "==== summary ===="
printf '%s\n' "${RESULTS[@]}"
exit "$FAILED"
