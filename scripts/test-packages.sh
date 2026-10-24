#!/usr/bin/env bash
# Test the release packaging end to end with throwaway keys, the way release.yml and docs.yml run it:
# nfpm builds signed deb/rpm/apk packages, packaging/repo/{apt,rpm,apk}.sh build and sign the repositories, and
# clean Debian, Fedora and Alpine containers add those repositories as the docs tell users to and install from them
# (signatures enforced).
#
#   scripts/test-packages.sh [BIN_DIR]
#
# BIN_DIR holds `rt` and `21ci` (any executables: the install test only checks the files land and run). Without it,
# small stand-in scripts are used. Needs nfpm and Docker; the containers are arm64 or amd64 like the host.
# Output: target/test-packages/ (packages, repositories, logs). Exit non-zero on the first failing step.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT" || exit 1
W="$ROOT/target/test-packages"
rm -rf "$W" && mkdir -p "$W"/{bin,comp,pkgs,site,keys,gnupg}
chmod 700 "$W/gnupg"
command -v nfpm >/dev/null || { echo "nfpm not found (brew install nfpm)" >&2; exit 1; }
docker info >/dev/null 2>&1 || { echo "Docker is not running" >&2; exit 1; }

step() { echo "==> $*"; }
UBUNTU=ubuntu:24.04 DEBIAN=debian:12 FEDORA=fedora:41 ALPINE=alpine:3.22

if [[ $# -ge 1 ]]; then
  cp "$1/rt" "$1/21ci" "$W/bin/"
else
  printf '#!/bin/sh\necho "rt test build"\n' > "$W/bin/rt"
  printf '#!/bin/sh\necho "21ci test build"\n' > "$W/bin/21ci"
  chmod +x "$W/bin/rt" "$W/bin/21ci"
fi
: > "$W/comp/21ci.bash"; : > "$W/comp/_21ci"; : > "$W/comp/21ci.fish"

step "throwaway signing keys (GPG for apt/rpm, RSA for apk)"
docker run --rm -v "$W:/t" "$UBUNTU" sh -ec '
  apt-get update -qq >/dev/null && apt-get install -y -qq gnupg >/dev/null
  export GNUPGHOME=/t/gnupg
  gpg --batch --passphrase "" --quick-gen-key "Package Test <test@example.invalid>" rsa3072 sign 1d 2>/dev/null
  gpg --batch --armor --export-secret-keys > /t/keys/pkg.asc
  gpg --batch --with-colons --list-secret-keys | awk -F: "/^fpr:/ { print \$10; exit }" > /t/keys/fpr
  chmod -R a+rwX /t/gnupg /t/keys'
openssl genrsa -out "$W/keys/21cloud.rsa" 2048 2>/dev/null

step "nfpm packages (signed), amd64 and arm64"
export VERSION=0.0.0-test BIN_DIR="$W/bin" COMPLETIONS_DIR="$W/comp" NFPM_GPG_KEY_FILE="$W/keys/pkg.asc" \
  NFPM_RPM_PASSPHRASE="" APK_KEY_FILE="$W/keys/21cloud.rsa"
for ARCH in amd64 arm64; do
  export ARCH
  for fmt in deb rpm; do nfpm package -f packaging/nfpm/rt.yaml -p "$fmt" -t "$W/pkgs/" >/dev/null; done
  for fmt in deb rpm apk; do nfpm package -f packaging/nfpm/21ci.yaml -p "$fmt" -t "$W/pkgs/" >/dev/null; done
done
ls "$W/pkgs"

step "apt and rpm repositories (packaging/repo/apt.sh, rpm.sh)"
docker run --rm -v "$ROOT:/w:ro" -v "$W:/t" -w /w "$UBUNTU" sh -ec '
  apt-get update -qq >/dev/null && apt-get install -y -qq --no-install-recommends apt-utils createrepo-c gnupg >/dev/null
  # gpg-agent cannot put its socket on a bind mount from macOS: use a copy of the keyring inside the container.
  cp -r /t/gnupg /root/gnupg && chmod 700 /root/gnupg
  export GNUPGHOME=/root/gnupg PKG_GPG_KEY_ID=$(cat /t/keys/fpr)
  sh packaging/repo/apt.sh /t/pkgs /t/site/apt
  sh packaging/repo/rpm.sh /t/pkgs /t/site/rpm file:///t/site/rpm
  gpg --batch --armor --export "$PKG_GPG_KEY_ID" > /t/site/gpg.key
  chmod -R a+rwX /t/site'

step "apk repository (packaging/repo/apk.sh)"
docker run --rm -v "$ROOT:/w:ro" -v "$W:/t" -w /w -e APK_KEY_FILE=/t/keys/21cloud.rsa "$ALPINE" \
  sh -ec 'sh packaging/repo/apk.sh /t/pkgs /t/site/apk; chmod -R a+rwX /t/site'

step "install with apt (Debian), signature enforced"
docker run --rm -v "$W/site:/repo:ro" "$DEBIAN" sh -ec '
  apt-get update -qq >/dev/null && apt-get install -y -qq gnupg >/dev/null
  install -d /etc/apt/keyrings && gpg --dearmor -o /etc/apt/keyrings/21cloud.gpg < /repo/gpg.key
  echo "deb [signed-by=/etc/apt/keyrings/21cloud.gpg] file:/repo/apt stable main" > /etc/apt/sources.list.d/21cloud.list
  apt-get update -qq 2>&1 | grep -iE "not signed|NO_PUBKEY|BADSIG" && exit 1
  apt-get install -y -qq rt 21ci >/dev/null
  rt && 21ci && id rt && test -f /etc/rt/rt.toml && echo "apt: ok"'

step "install with dnf (Fedora), gpgcheck and repo_gpgcheck on"
docker run --rm -v "$W/site:/repo:ro" "$FEDORA" sh -ec '
  sed "s#file:///t/site/rpm#file:///repo/rpm#; s#^gpgkey=.*#gpgkey=file:///repo/gpg.key#" /repo/rpm/21cloud.repo \
    > /etc/yum.repos.d/21cloud.repo
  grep -q "^gpgcheck=1" /etc/yum.repos.d/21cloud.repo && grep -q "^repo_gpgcheck=1" /etc/yum.repos.d/21cloud.repo
  dnf -y -q install rt 21ci && rt && 21ci && id rt && echo "dnf: ok"'

step "install with apk (Alpine), signed index"
docker run --rm -v "$W/site:/repo:ro" "$ALPINE" sh -ec '
  cp /repo/apk/21cloud.rsa.pub /etc/apk/keys/
  echo /repo/apk >> /etc/apk/repositories
  apk update -q && apk add -q 21ci && 21ci && echo "apk: ok"'

echo
echo "packaging test passed: packages, apt/rpm/apk repositories and installs from them"
