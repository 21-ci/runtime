#!/bin/sh
# Build and sign the apt repository from a directory of .deb files.
#
#   packaging/repo/apt.sh DEB_DIR OUT_DIR
#
# Layout (OUT_DIR is served as https://21-ci.github.io/runtime/packages/apt):
#   pool/main/<package>/<file>.deb
#   dists/stable/main/binary-{amd64,arm64}/Packages{,.gz}
#   dists/stable/{Release,Release.gpg,InRelease}
#
# Env: PKG_GPG_KEY_ID (key already in the gpg keyring), PKG_GPG_PASSPHRASE (optional).
# Needs apt-ftparchive (apt-utils), gpg, gzip.
set -eu

if [ "$#" -ne 2 ]; then
  echo "usage: $0 DEB_DIR OUT_DIR" >&2
  exit 2
fi
deb_dir=$1
out=$2
: "${PKG_GPG_KEY_ID:?set PKG_GPG_KEY_ID}"

for tool in apt-ftparchive gpg gzip; do
  command -v "$tool" >/dev/null 2>&1 || {
    echo "apt.sh: needs $tool" >&2
    exit 1
  }
done

rm -rf "$out"
mkdir -p "$out/pool/main"
found=0
for deb in "$deb_dir"/*.deb; do
  [ -f "$deb" ] || continue
  name=$(basename "$deb")
  pkg=${name%%_*}
  mkdir -p "$out/pool/main/$pkg"
  cp "$deb" "$out/pool/main/$pkg/"
  found=$((found + 1))
done
[ "$found" -gt 0 ] || {
  echo "apt.sh: no .deb files in $deb_dir" >&2
  exit 1
}

cd "$out"
for arch in amd64 arm64; do
  dir=dists/stable/main/binary-$arch
  mkdir -p "$dir"
  apt-ftparchive --arch "$arch" packages pool/main >"$dir/Packages"
  gzip -9 -n -k "$dir/Packages"
done

apt-ftparchive \
  -o APT::FTPArchive::Release::Origin="21cloud" \
  -o APT::FTPArchive::Release::Label="21cloud" \
  -o APT::FTPArchive::Release::Suite="stable" \
  -o APT::FTPArchive::Release::Codename="stable" \
  -o APT::FTPArchive::Release::Architectures="amd64 arm64" \
  -o APT::FTPArchive::Release::Components="main" \
  -o APT::FTPArchive::Release::Description="21 Cloud runtime packages (rt, 21ci)" \
  release dists/stable >dists/stable/Release

sign() {
  if [ -n "${PKG_GPG_PASSPHRASE:-}" ]; then
    printf '%s' "$PKG_GPG_PASSPHRASE" | gpg --batch --yes --pinentry-mode loopback --passphrase-fd 0 \
      --local-user "$PKG_GPG_KEY_ID" "$@"
  else
    gpg --batch --yes --local-user "$PKG_GPG_KEY_ID" "$@"
  fi
}
sign --clearsign --digest-algo SHA512 -o dists/stable/InRelease dists/stable/Release
sign --armor --detach-sign --digest-algo SHA512 -o dists/stable/Release.gpg dists/stable/Release

echo "apt.sh: $found package(s) in $out"
