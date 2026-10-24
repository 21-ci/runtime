#!/bin/sh
# Install rt (the runtime node) and/or 21ci (the CLI) from GitHub Releases.
#
#   curl -fsSL https://21-ci.github.io/runtime/install.sh | sh
#   curl -fsSL https://21-ci.github.io/runtime/install.sh | sh -s -- --only 21ci --version 1.2.3
#
# Options:
#   --only rt|21ci        install one binary (default: both; on musl systems only 21ci)
#   --version X.Y.Z       a release (default: the latest)
#   --bin-dir DIR         where to put the binaries (default: /usr/local/bin as root, else ~/.local/bin)
#   --verify-signature    also verify SHA256SUMS with cosign (keyless, Sigstore); fails if cosign is missing
#   --no-verify-signature skip the cosign check even when cosign is installed
#   -h, --help
#
# Every download is checked against the release's SHA256SUMS. When cosign is on PATH, SHA256SUMS itself is verified
# against the release workflow's Sigstore identity, so a tampered checksum file is caught too.
#
# Environment: RT_INSTALL_REPO (default 21-ci/runtime), RT_INSTALL_BASE_URL (mirror of the release downloads),
# GITHUB_TOKEN (optional, raises the API rate limit for the latest-version lookup).
set -eu

REPO=${RT_INSTALL_REPO:-21-ci/runtime}
BASE_URL=${RT_INSTALL_BASE_URL:-https://github.com/$REPO/releases/download}
ONLY=""
VERSION=""
BIN_DIR=""
VERIFY_SIG=auto

say() { printf '%s\n' "install: $*" >&2; }
die() {
  say "error: $*"
  exit 1
}
need() { command -v "$1" >/dev/null 2>&1 || die "this installer needs '$1' on PATH"; }

usage() {
  cat <<'EOF'
usage: install.sh [--only rt|21ci] [--version X.Y.Z] [--bin-dir DIR] [--verify-signature | --no-verify-signature]

Installs rt and/or 21ci from GitHub Releases, verifying SHA256SUMS (and its Sigstore signature when cosign is on PATH).
EOF
}

while [ "$#" -gt 0 ]; do
  case "$1" in
    --only)
      [ "$#" -ge 2 ] || die "--only needs rt or 21ci"
      ONLY=$2
      shift 2
      ;;
    --version)
      [ "$#" -ge 2 ] || die "--version needs X.Y.Z"
      VERSION=${2#v}
      shift 2
      ;;
    --bin-dir)
      [ "$#" -ge 2 ] || die "--bin-dir needs a directory"
      BIN_DIR=$2
      shift 2
      ;;
    --verify-signature)
      VERIFY_SIG=yes
      shift
      ;;
    --no-verify-signature)
      VERIFY_SIG=no
      shift
      ;;
    -h | --help)
      usage
      exit 0
      ;;
    *) die "unknown option: $1 (see --help)" ;;
  esac
done

case "$ONLY" in
  "" | rt | 21ci) ;;
  *) die "--only takes rt or 21ci, not '$ONLY'" ;;
esac

# --- downloader ----------------------------------------------------------------------------------------------------
if command -v curl >/dev/null 2>&1; then
  fetch() { curl --proto '=https' --tlsv1.2 -fsSL --retry 3 -o "$2" "$1"; }
  fetch_stdout() { curl --proto '=https' --tlsv1.2 -fsSL --retry 3 ${GITHUB_TOKEN:+-H "Authorization: Bearer $GITHUB_TOKEN"} "$1"; }
elif command -v wget >/dev/null 2>&1; then
  fetch() { wget -q --https-only -O "$2" "$1"; }
  fetch_stdout() { wget -q --https-only -O - ${GITHUB_TOKEN:+--header="Authorization: Bearer $GITHUB_TOKEN"} "$1"; }
else
  die "this installer needs curl or wget"
fi
need tar
need uname
need mktemp

if command -v sha256sum >/dev/null 2>&1; then
  sha256() { sha256sum "$1" | awk '{print $1}'; }
elif command -v shasum >/dev/null 2>&1; then
  sha256() { shasum -a 256 "$1" | awk '{print $1}'; }
elif command -v openssl >/dev/null 2>&1; then
  sha256() { openssl dgst -sha256 -r "$1" | awk '{print $1}'; }
else
  die "this installer needs sha256sum, shasum or openssl to verify downloads"
fi

# --- platform ------------------------------------------------------------------------------------------------------
os=$(uname -s)
arch=$(uname -m)
case "$arch" in
  x86_64 | amd64) arch=x86_64 ;;
  aarch64 | arm64) arch=aarch64 ;;
  *) die "unsupported CPU architecture: $arch (release builds: x86_64, aarch64)" ;;
esac

libc=gnu
case "$os" in
  Linux)
    if [ -e /etc/alpine-release ] || { command -v ldd >/dev/null 2>&1 && ldd --version 2>&1 | grep -qi musl; }; then
      libc=musl
    fi
    ;;
  Darwin) ;;
  *) die "unsupported OS: $os (release builds: Linux, macOS). On Windows use WSL2 or the container image." ;;
esac

target_for() {
  # $1 = rt | 21ci
  case "$os" in
    Darwin) echo "$arch-apple-darwin" ;;
    Linux)
      if [ "$1" = 21ci ]; then
        # 21ci is a static musl binary on Linux: runs on any distribution.
        echo "$arch-unknown-linux-musl"
      else
        echo "$arch-unknown-linux-gnu"
      fi
      ;;
  esac
}

if [ -n "$ONLY" ]; then
  bins=$ONLY
elif [ "$libc" = musl ]; then
  say "musl libc detected: rt needs glibc (the prebuilt V8 does), installing only 21ci; run rt as the image ghcr.io/21-ci/rt"
  bins=21ci
else
  bins="rt 21ci"
fi
if [ "$libc" = musl ] && [ "$ONLY" = rt ]; then
  die "rt needs glibc; on Alpine/musl run the container image ghcr.io/21-ci/rt instead"
fi

# --- version -------------------------------------------------------------------------------------------------------
if [ -z "$VERSION" ]; then
  VERSION=$(fetch_stdout "https://api.github.com/repos/$REPO/releases/latest" |
    sed -n 's/.*"tag_name": *"v\{0,1\}\([^"]*\)".*/\1/p' | head -n 1)
  [ -n "$VERSION" ] || die "could not find the latest release of $REPO (set --version, or GITHUB_TOKEN if rate-limited)"
fi
case "$VERSION" in
  *[!0-9A-Za-z.+-]*) die "invalid version: $VERSION" ;;
esac

# --- install dir ---------------------------------------------------------------------------------------------------
if [ -z "$BIN_DIR" ]; then
  if [ "$(id -u)" -eq 0 ]; then
    BIN_DIR=/usr/local/bin
  else
    BIN_DIR=$HOME/.local/bin
  fi
fi
mkdir -p "$BIN_DIR" || die "cannot create $BIN_DIR"
[ -w "$BIN_DIR" ] || die "$BIN_DIR is not writable (use --bin-dir, or run as root)"

tmp=$(mktemp -d 2>/dev/null || mktemp -d -t rt-install)
trap 'rm -rf "$tmp"' EXIT INT TERM

# --- checksums (and their signature) -------------------------------------------------------------------------------
url="$BASE_URL/v$VERSION"
say "release v$VERSION from $url"
fetch "$url/SHA256SUMS" "$tmp/SHA256SUMS" || die "download failed: $url/SHA256SUMS"

if [ "$VERIFY_SIG" != no ]; then
  if command -v cosign >/dev/null 2>&1; then
    fetch "$url/SHA256SUMS.sigstore.json" "$tmp/SHA256SUMS.sigstore.json" ||
      die "download failed: $url/SHA256SUMS.sigstore.json"
    cosign verify-blob \
      --bundle "$tmp/SHA256SUMS.sigstore.json" \
      --certificate-identity-regexp "^https://github.com/$REPO/\.github/workflows/release\.yml@refs/tags/v" \
      --certificate-oidc-issuer https://token.actions.githubusercontent.com \
      "$tmp/SHA256SUMS" >/dev/null 2>&1 || die "cosign could not verify SHA256SUMS: refusing to install"
    say "SHA256SUMS signature verified (Sigstore, $REPO release workflow)"
  elif [ "$VERIFY_SIG" = yes ]; then
    die "--verify-signature needs cosign on PATH (https://docs.sigstore.dev/cosign/system_config/installation/)"
  else
    say "cosign not found: verifying checksums only (install cosign to also verify the signature)"
  fi
fi

# --- binaries ------------------------------------------------------------------------------------------------------
for bin in $bins; do
  target=$(target_for "$bin")
  name="$bin-$VERSION-$target"
  file="$name.tar.gz"
  fetch "$url/$file" "$tmp/$file" || die "download failed: $url/$file (no build for $target?)"

  want=$(awk -v f="$file" '$2 == f || $2 == "*" f { print $1 }' "$tmp/SHA256SUMS")
  [ -n "$want" ] || die "$file is not listed in SHA256SUMS"
  got=$(sha256 "$tmp/$file")
  [ "$want" = "$got" ] || die "checksum mismatch for $file (expected $want, got $got)"

  tar -xzf "$tmp/$file" -C "$tmp"
  [ -f "$tmp/$name/$bin" ] || die "$file does not contain $name/$bin"
  chmod 0755 "$tmp/$name/$bin"
  mv -f "$tmp/$name/$bin" "$BIN_DIR/$bin"
  say "installed $BIN_DIR/$bin ($VERSION, $target)"
done

case ":$PATH:" in
  *":$BIN_DIR:"*) ;;
  *) say "add $BIN_DIR to your PATH, e.g.: export PATH=\"$BIN_DIR:\$PATH\"" ;;
esac

cat >&2 <<EOF

Next steps:
  Installed:    $bins (check with --version)
  Run a node:   https://21-ci.github.io/runtime/ (quickstart; production config: deploy/rt.toml in the repository)
  Deploy:       21ci config set-context local --api http://127.0.0.1:8081 --token <root token>
EOF
