#!/usr/bin/env bash
# Run the CI and security checks locally, with the same commands as .github/workflows/{ci,security}.yml.
#
#   scripts/ci-local.sh [STAGE...] [--fast]
#
# Stages (default: lint security docs test smokes):
#   lint       cargo fmt --check, cargo clippy -D warnings
#   security   cargo-deny, cargo-audit, gitleaks (full history), semgrep, zizmor, actionlint, shellcheck,
#              hadolint, trivy fs + config
#   docs       mkdocs --strict build, no internal references (make docs-check)
#   test       every test of the workspace under coverage, with the CI floors (scripts/coverage.sh)
#   smokes     the sample apps against the rt binary of the test stage
#   image      docker build of rt and 21ci, trivy + grype on the images (slow; not in the default set)
#   packages   signed deb/rpm/apk, the apt/rpm/apk repositories and installs from them in containers
#              (scripts/test-packages.sh; needs Docker and nfpm; not in the default set)
#
#   --fast     lint security docs only (a few minutes): what to run before every push
#
# Tools (macOS): brew install cargo-deny cargo-audit gitleaks semgrep zizmor actionlint shellcheck hadolint trivy grype
# A missing tool fails its stage with the install hint. Every stage runs even when an earlier one fails; the
# summary lists the failures and the exit code is non-zero if there were any. Logs: target/ci-local/<stage>.log.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT" || exit 1
LOGS="$ROOT/target/ci-local"
mkdir -p "$LOGS"

usage() { sed -n '2,/^set -uo/{/^set -uo/d;s/^# \{0,1\}//;p;}' "${BASH_SOURCE[0]}"; }

STAGES=()
for a in "$@"; do
  case "$a" in
    -h|--help) usage; exit 0 ;;
    --fast) STAGES+=(lint security docs) ;;
    lint|security|docs|test|smokes|image|packages) STAGES+=("$a") ;;
    *) echo "unknown stage or option '$a' (see --help)" >&2; exit 2 ;;
  esac
done
[[ ${#STAGES[@]} -gt 0 ]] || STAGES=(lint security docs test smokes)

RESULTS=() FAILED=0
need() { command -v "$1" >/dev/null || { echo "missing tool: $1 (brew install $2)"; return 1; }; }
# check <name> <cmd...>: run one check, log it, record PASS/FAIL.
check() {
  local name=$1 rc=0 start
  shift
  start=$(date +%s)
  printf '  %-22s' "$name"
  "$@" >"$LOGS/$name.log" 2>&1 || rc=$?
  local took=$(( $(date +%s) - start ))
  if [[ $rc -eq 0 ]]; then
    echo "ok (${took}s)"; RESULTS+=("PASS  $name")
  else
    echo "FAIL (exit $rc, ${took}s) -> target/ci-local/$name.log"; RESULTS+=("FAIL  $name"); FAILED=1
  fi
}

lint() {
  check fmt cargo fmt --all -- --check
  check clippy cargo clippy --workspace --all-targets --locked -- -D warnings
}

audit_ignores() {
  python3 -c 'import tomllib; print(" ".join("--ignore " + i["id"] for i in tomllib.load(open("deny.toml","rb"))["advisories"].get("ignore", [])))'
}

security() {
  check cargo-deny sh -c 'command -v cargo-deny >/dev/null || { echo "brew install cargo-deny"; exit 1; }
    cargo deny --workspace check advisories licenses bans sources'
  # shellcheck disable=SC2046
  check cargo-audit sh -c "command -v cargo-audit >/dev/null || { echo 'brew install cargo-audit'; exit 1; }
    cargo audit --deny warnings $(audit_ignores)"
  check gitleaks sh -c 'command -v gitleaks >/dev/null || { echo "brew install gitleaks"; exit 1; }
    gitleaks git --redact --no-banner .'
  check semgrep sh -c 'command -v semgrep >/dev/null || { echo "brew install semgrep"; exit 1; }
    semgrep scan --metrics=off --config p/rust --config p/javascript --config p/typescript --config p/secrets \
      --config p/github-actions --exclude target --exclude fixtures/wasm/out --exclude "sample/**/package-lock.json" \
      --severity ERROR --error'
  check zizmor sh -c 'command -v zizmor >/dev/null || { echo "brew install zizmor"; exit 1; }
    zizmor --min-severity medium .github/workflows'
  check actionlint sh -c 'command -v actionlint >/dev/null || { echo "brew install actionlint"; exit 1; }
    actionlint'
  check shellcheck sh -c 'command -v shellcheck >/dev/null || { echo "brew install shellcheck"; exit 1; }
    shellcheck -s sh packaging/install.sh packaging/repo/*.sh packaging/scripts/*.sh packaging/homebrew/render.sh &&
    git ls-files -z "*.sh" | xargs -0 shellcheck -S error'
  check hadolint sh -c 'command -v hadolint >/dev/null || { echo "brew install hadolint"; exit 1; }
    hadolint --failure-threshold warning Dockerfile Dockerfile.21ci'
  check trivy-fs sh -c 'command -v trivy >/dev/null || { echo "brew install trivy"; exit 1; }
    trivy fs --quiet --scanners vuln,secret,misconfig --severity HIGH,CRITICAL --ignore-unfixed \
      --ignorefile .trivyignore --skip-dirs target --skip-dirs fixtures/wasm/out --exit-code 1 .'
}

docs() { check docs make docs-check; }

test_stage() {
  check test-coverage scripts/coverage.sh --fail-under 90 --floor tools=45 --exclusions
}

smokes() {
  local bin=target/llvm-cov-target/debug/rt
  [[ -x $bin ]] || bin=target/debug/rt
  if [[ ! -x $bin ]]; then
    check build-rt cargo build --locked -p rt-server --bin rt
    bin=target/debug/rt
  fi
  check smokes sample/apps/run-smokes.sh "$bin"
}

image() {
  check docker-build sh -c 'docker build -t rt:ci -f Dockerfile . && docker build -t 21ci:ci -f Dockerfile.21ci .'
  check trivy-image sh -c 'trivy image --quiet --severity HIGH,CRITICAL --ignore-unfixed --ignorefile .trivyignore \
    --exit-code 1 rt:ci && trivy image --quiet --severity HIGH,CRITICAL --ignore-unfixed --ignorefile .trivyignore \
    --exit-code 1 21ci:ci'
  check grype-image sh -c 'grype --only-fixed --fail-on high rt:ci && grype --only-fixed --fail-on high 21ci:ci'
}

packages() { check packages scripts/test-packages.sh; }

for s in "${STAGES[@]}"; do
  echo "==> $s"
  case "$s" in
    test) test_stage ;;
    *) "$s" ;;
  esac
done

echo
echo "==== summary ===="
printf '%s\n' "${RESULTS[@]}"
exit "$FAILED"
