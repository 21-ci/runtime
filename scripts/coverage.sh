#!/usr/bin/env bash
# Line and function coverage for the whole system or chosen modules, with an lcov report.
#
#   scripts/coverage.sh [MODULE...] [options] [-- <extra cargo test args>]
#
# MODULE is a crate (rt-sched, or just sched) or `tools` (the Python tooling). No MODULE means everything.
#
# Options:
#   --unit          only the unit tests inside src/ (cargo --lib --bins)
#   --integration   only the integration tests in crates/*/tests (cargo --test '*')
#   --html          also write an HTML report to target/coverage/html/index.html
#   --missing       print the uncovered line ranges of every file
#   --exclusions    print every LCOV_EXCL exclusion with its reason
#   --fail-under N  exit 2 when any selected module's line coverage is below N percent (CI uses 90)
#   --floor M=N     module M's own floor instead (repeatable; CI: tools=45, the curses menuconfig UI is untested)
#   --keep          keep the profile data of earlier runs (accumulate several partial runs)
#   --prune         delete the instrumented build (target/llvm-cov-target) afterwards to free disk
#
# Output in target/coverage/:
#   lcov.info       merged lcov (Rust + Python), what IDE plugins and codecov read
#   summary.txt     the per-module table, also printed
#   summary.md      the same table as markdown
#   html/           with --html
#
# Lines that cannot run in a test are excluded in the source with a reason, the lcov way:
#   // LCOV_EXCL_LINE: <reason>      or      // LCOV_EXCL_START: <reason> ... // LCOV_EXCL_STOP
# Not measured: js/bootstrap.js (runs inside V8; its behaviour is covered through the Deno executor tests).
#
# Needs cargo-llvm-cov (brew install cargo-llvm-cov, or cargo install cargo-llvm-cov) and the llvm-tools
# rustup component; Python coverage needs `pip install coverage` and is skipped with a note otherwise.
# RT_COV_OUT moves the output directory (parallel runs keep their own reports; the runs themselves queue on a lock).
# Debug info is off by default (coverage does not need it and it triples the build size); RT_COV_DEBUG=1 keeps it.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

die() { echo "error: $*" >&2; exit 1; }
usage() { sed -n '2,/^set -euo/{/^set -euo/d;s/^# \{0,1\}//;p;}' "${BASH_SOURCE[0]}"; }

OUT="${RT_COV_OUT:-$ROOT/target/coverage}"
SCOPE=() MODULES=() REPORT_ARGS=() CARGO_EXTRA=()
HTML=0 KEEP=0 PRUNE=0
while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help)     usage; exit 0 ;;
    --unit)        SCOPE=(--lib --bins) ;;
    --integration) SCOPE=(--test '*') ;;
    --html)        HTML=1 ;;
    --missing)     REPORT_ARGS+=(--missing) ;;
    --exclusions)  REPORT_ARGS+=(--exclusions) ;;
    --fail-under)  [[ $# -ge 2 ]] || die "--fail-under needs a number"; REPORT_ARGS+=(--fail-under "$2"); shift ;;
    --floor)       [[ $# -ge 2 ]] || die "--floor needs MODULE=PCT"; REPORT_ARGS+=(--floor "$2"); shift ;;
    --keep)        KEEP=1 ;;
    --prune)       PRUNE=1 ;;
    --)            shift; CARGO_EXTRA=("$@"); break ;;
    -*)            die "unknown option '$1' (see --help)" ;;
    *)             MODULES+=("$1") ;;
  esac
  shift
done

cargo llvm-cov --version >/dev/null 2>&1 || die "cargo-llvm-cov not found (brew install cargo-llvm-cov)"

ALL_CRATES=()
for d in crates/*/; do ALL_CRATES+=("$(basename "$d")"); done

# Resolve module names: `sched` -> rt-sched; `tools` stays tools.
CRATES=() PYTHON=0
if [[ ${#MODULES[@]} -eq 0 ]]; then
  CRATES=("${ALL_CRATES[@]}"); PYTHON=1
else
  for m in "${MODULES[@]}"; do
    if [[ "$m" == tools ]]; then PYTHON=1
    elif [[ -d "crates/$m" ]]; then CRATES+=("$m")
    elif [[ -d "crates/rt-$m" ]]; then CRATES+=("rt-$m")
    else die "unknown module '$m' (crates: ${ALL_CRATES[*]}, or tools)"; fi
  done
fi

# One coverage run at a time: runs share target/llvm-cov-target and its profile data, and each run clears it.
LOCK="$ROOT/target/.coverage.lock"
mkdir -p "$ROOT/target"
until mkdir "$LOCK" 2>/dev/null; do
  holder="$(cat "$LOCK/pid" 2>/dev/null || true)"
  if [[ -n "$holder" ]] && ! kill -0 "$holder" 2>/dev/null; then rm -rf "$LOCK"; continue; fi
  echo "waiting for another coverage run (pid ${holder:-?}) to finish..." >&2
  sleep 10
done
echo $$ > "$LOCK/pid"
trap 'rm -rf "$LOCK"' EXIT

mkdir -p "$OUT"
rm -f "$OUT"/*.lcov "$OUT/lcov.info"
RC=0
[[ "${RT_COV_DEBUG:-0}" == 1 ]] || export CARGO_PROFILE_DEV_DEBUG=0 CARGO_PROFILE_TEST_DEBUG=0

if [[ ${#CRATES[@]} -gt 0 ]]; then
  # rt-exec-wasm and rt-server load the WASM guests from fixtures/wasm/out.
  if [[ " ${CRATES[*]} " =~ \ rt-(exec-wasm|server)\  ]] && ! compgen -G "fixtures/wasm/out/*.wasm" >/dev/null; then
    scripts/build-fixtures.sh || die "building the WASM fixtures failed"
  fi
  # Only the profile data: cleaning the build too (--workspace) made every run rebuild every crate.
  [[ "$KEEP" == 1 ]] || cargo llvm-cov clean --profraw-only

  PKGS=()
  for c in "${CRATES[@]}"; do PKGS+=(-p "$c"); done
  echo "==> tests under coverage: ${CRATES[*]}"
  cargo llvm-cov --no-report "${PKGS[@]}" ${SCOPE[@]+"${SCOPE[@]}"} \
    ${CARGO_EXTRA[@]+"${CARGO_EXTRA[@]}"} || { echo "some tests failed (report still written)" >&2; RC=1; }

  # Test code, test support and fixtures are not the code under test.
  IGNORE='(/\.cargo/|/rustc/|/target/|/fixtures/|/benches/|crates/[^/]+/tests/|crates/rt-testkit/'
  for c in "${ALL_CRATES[@]}"; do
    [[ " ${CRATES[*]} " == *" $c "* ]] || IGNORE+="|crates/$c/"
  done
  IGNORE+=')'
  cargo llvm-cov report --lcov --output-path "$OUT/rust.lcov" --ignore-filename-regex "$IGNORE"
  if [[ "$HTML" == 1 ]]; then
    cargo llvm-cov report --html --output-dir "$OUT" --ignore-filename-regex "$IGNORE"
  fi
fi

if [[ "$PYTHON" == 1 ]]; then
  if python3 -c 'import coverage' 2>/dev/null; then
    echo "==> python tool tests under coverage"
    (cd tools && python3 -m coverage run --data-file="$OUT/.coverage" --source=. --omit='test_*.py' \
       -m unittest discover -p 'test_*.py' -q) || { echo "python tool tests failed" >&2; RC=1; }
    python3 -m coverage lcov --data-file="$OUT/.coverage" -o "$OUT/python.lcov" -q
  else
    echo "note: python coverage skipped (pip install coverage)"
  fi
fi

compgen -G "$OUT/*.lcov" >/dev/null || die "no coverage data was produced"
cat "$OUT"/*.lcov > "$OUT/lcov.info"

REPORT_MODULES=()
for c in ${CRATES[@]+"${CRATES[@]}"}; do REPORT_MODULES+=(--module "$c"); done
[[ "$PYTHON" == 1 ]] && REPORT_MODULES+=(--module tools)

echo
echo "==== coverage ===="
REPORT_RC=0
python3 tools/coverage_report.py "$OUT/lcov.info" --root "$ROOT" --markdown "$OUT/summary.md" \
  "${REPORT_MODULES[@]}" ${REPORT_ARGS[@]+"${REPORT_ARGS[@]}"} | tee "$OUT/summary.txt" || REPORT_RC=$?

echo
echo "lcov:    $OUT/lcov.info"
echo "summary: $OUT/summary.md"
[[ "$HTML" == 1 ]] && echo "html:    $OUT/html/index.html"
[[ "$PRUNE" == 1 ]] && rm -rf "$ROOT/target/llvm-cov-target" && echo "pruned target/llvm-cov-target"

[[ "$REPORT_RC" -ne 0 ]] && exit "$REPORT_RC"
exit "$RC"
