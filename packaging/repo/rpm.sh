#!/bin/sh
# Build and sign the rpm (dnf/yum/zypper) repository from a directory of .rpm files.
#
#   packaging/repo/rpm.sh RPM_DIR OUT_DIR BASE_URL
#
# Layout (OUT_DIR is served as BASE_URL, e.g. https://21-ci.github.io/runtime/packages/rpm):
#   {x86_64,aarch64}/*.rpm + repodata/ (repomd.xml signed: repomd.xml.asc)
#   21cloud.repo        the file users drop into /etc/yum.repos.d/
#
# The packages themselves are signed by nfpm at build time (same GPG key). Env: PKG_GPG_KEY_ID, PKG_GPG_PASSPHRASE
# (optional). Needs createrepo_c and gpg.
set -eu

if [ "$#" -ne 3 ]; then
  echo "usage: $0 RPM_DIR OUT_DIR BASE_URL" >&2
  exit 2
fi
rpm_dir=$1
out=$2
base_url=${3%/}
: "${PKG_GPG_KEY_ID:?set PKG_GPG_KEY_ID}"

for tool in createrepo_c gpg; do
  command -v "$tool" >/dev/null 2>&1 || {
    echo "rpm.sh: needs $tool" >&2
    exit 1
  }
done

rm -rf "$out"
found=0
for rpm in "$rpm_dir"/*.rpm; do
  [ -f "$rpm" ] || continue
  case "$rpm" in
    *.x86_64.rpm) arch=x86_64 ;;
    *.aarch64.rpm) arch=aarch64 ;;
    *)
      echo "rpm.sh: skipping $rpm (unknown arch)" >&2
      continue
      ;;
  esac
  mkdir -p "$out/$arch"
  cp "$rpm" "$out/$arch/"
  found=$((found + 1))
done
[ "$found" -gt 0 ] || {
  echo "rpm.sh: no .rpm files in $rpm_dir" >&2
  exit 1
}

for dir in "$out"/*/; do
  # gzip: readable by every dnf, yum and zypper, and supported by the createrepo-c Ubuntu ships (no zstd).
  createrepo_c --quiet --general-compress-type=gz "$dir"
  if [ -n "${PKG_GPG_PASSPHRASE:-}" ]; then
    printf '%s' "$PKG_GPG_PASSPHRASE" | gpg --batch --yes --pinentry-mode loopback --passphrase-fd 0 \
      --local-user "$PKG_GPG_KEY_ID" --armor --detach-sign "$dir/repodata/repomd.xml"
  else
    gpg --batch --yes --local-user "$PKG_GPG_KEY_ID" --armor --detach-sign "$dir/repodata/repomd.xml"
  fi
done

cat >"$out/21cloud.repo" <<EOF
[21cloud]
name=21 Cloud runtime packages (rt, 21ci)
baseurl=$base_url/\$basearch
enabled=1
gpgcheck=1
repo_gpgcheck=1
gpgkey=${base_url%/rpm}/gpg.key
# Fedora and EPEL have an unrelated package named rt (Request Tracker) at a higher version; without a priority
# below their 99, dnf would install that one.
priority=10
EOF

echo "rpm.sh: $found package(s) in $out"
