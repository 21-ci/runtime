#!/bin/sh
# Render the Homebrew formulae of a release from the templates and the release's SHA256SUMS.
#
#   packaging/homebrew/render.sh VERSION SHA256SUMS OUT_DIR
#
# Writes OUT_DIR/Formula/rt.rb, OUT_DIR/Formula/twentyone-ci.rb and the alias OUT_DIR/Aliases/21ci, the layout of the
# 21-ci/homebrew-tap repository. Placeholders: @VERSION@ and @SHA256_<bin>_<target>@, e.g.
# @SHA256_rt_aarch64-apple-darwin@ is the checksum of rt-VERSION-aarch64-apple-darwin.tar.gz.
set -eu

if [ "$#" -ne 3 ]; then
  echo "usage: $0 VERSION SHA256SUMS OUT_DIR" >&2
  exit 2
fi
version=$1
sums=$2
out=$3
here=$(cd "$(dirname "$0")" && pwd)

mkdir -p "$out/Formula" "$out/Aliases"

render() {
  # $1 template, $2 output
  tmp=$2.tmp
  sed "s/@VERSION@/$version/g" "$1" >"$tmp"
  # Placeholders never contain whitespace, so word splitting is what we want here.
  # shellcheck disable=SC2013
  for placeholder in $(grep -o '@SHA256_[A-Za-z0-9_-]*@' "$tmp" | sort -u); do
    key=${placeholder#@SHA256_}
    key=${key%@}
    bin=${key%%_*}
    target=${key#*_}
    file="$bin-$version-$target.tar.gz"
    sum=$(awk -v f="$file" '$2 == f || $2 == "*" f { print $1 }' "$sums")
    if [ -z "$sum" ]; then
      echo "render.sh: no checksum for $file in $sums" >&2
      rm -f "$tmp"
      exit 1
    fi
    sed "s/$placeholder/$sum/g" "$tmp" >"$tmp.2" && mv "$tmp.2" "$tmp"
  done
  mv "$tmp" "$2"
}

render "$here/rt.rb.tmpl" "$out/Formula/rt.rb"
render "$here/twentyone-ci.rb.tmpl" "$out/Formula/twentyone-ci.rb"
ln -sf ../Formula/twentyone-ci.rb "$out/Aliases/21ci"
echo "rendered rt.rb and twentyone-ci.rb (alias 21ci) for $version into $out"
