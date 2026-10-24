#!/bin/sh
# Build and sign the Alpine (apk) repository from a directory of .apk files. Runs inside an Alpine container:
#
#   docker run --rm -v "$PWD:/w" -w /w -e APK_KEY_FILE=/w/keys/21cloud.rsa alpine:3.22 \
#     sh packaging/repo/apk.sh dist/pkg out/packages/apk
#
# Layout (OUT_DIR is served as https://21-ci.github.io/runtime/packages/apk):
#   {x86_64,aarch64}/<name>-<version>.apk + APKINDEX.tar.gz (signed with the RSA key)
#   21cloud.rsa.pub     the public key users copy to /etc/apk/keys/
#
# Env: APK_KEY_FILE (RSA private key, PEM). The key file must be named 21cloud.rsa: abuild-sign names the signature
# after it (.SIGN.RSA.21cloud.rsa.pub), and nfpm signs the packages with the same key name (packaging/nfpm/21ci.yaml).
set -eu

if [ "$#" -ne 2 ]; then
  echo "usage: $0 APK_DIR OUT_DIR" >&2
  exit 2
fi
apk_dir=$1
out=$2
: "${APK_KEY_FILE:?set APK_KEY_FILE}"
[ "$(basename "$APK_KEY_FILE")" = 21cloud.rsa ] || {
  echo "apk.sh: APK_KEY_FILE must be named 21cloud.rsa" >&2
  exit 1
}

apk add --no-cache --quiet abuild openssl tar >/dev/null

rm -rf "$out"
mkdir -p "$out"
found=0
for pkg in "$apk_dir"/*.apk; do
  [ -f "$pkg" ] || continue
  info=$(tar -xzOf "$pkg" .PKGINFO 2>/dev/null || true)
  name=$(printf '%s\n' "$info" | sed -n 's/^pkgname = //p')
  ver=$(printf '%s\n' "$info" | sed -n 's/^pkgver = //p')
  arch=$(printf '%s\n' "$info" | sed -n 's/^arch = //p')
  if [ -z "$name" ] || [ -z "$ver" ] || [ -z "$arch" ]; then
    echo "apk.sh: cannot read .PKGINFO of $pkg" >&2
    exit 1
  fi
  mkdir -p "$out/$arch"
  # apk fetches <name>-<pkgver>.apk, so the file name must follow the package metadata.
  cp "$pkg" "$out/$arch/$name-$ver.apk"
  found=$((found + 1))
done
[ "$found" -gt 0 ] || {
  echo "apk.sh: no .apk files in $apk_dir" >&2
  exit 1
}

for dir in "$out"/*/; do
  (
    cd "$dir"
    # apk-tools 2 syntax: run in alpine:3.22 (apk-tools 3 replaces `index` with `mkndx`).
    apk index --quiet --allow-untrusted -d "21cloud runtime packages" -o APKINDEX.tar.gz ./*.apk
    abuild-sign -q -k "$APK_KEY_FILE" APKINDEX.tar.gz
  )
done

openssl rsa -in "$APK_KEY_FILE" -pubout -out "$out/21cloud.rsa.pub" 2>/dev/null
echo "apk.sh: $found package(s) in $out"
