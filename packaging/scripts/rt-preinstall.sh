#!/bin/sh
# deb/rpm pre-install of `rt`: create the `rt` system user and group (home /var/lib/rt, no login shell).
set -eu

if ! getent group rt >/dev/null 2>&1; then
  if command -v addgroup >/dev/null 2>&1 && [ -f /etc/debian_version ]; then
    addgroup --system rt >/dev/null
  else
    groupadd --system rt
  fi
fi

if ! getent passwd rt >/dev/null 2>&1; then
  if command -v adduser >/dev/null 2>&1 && [ -f /etc/debian_version ]; then
    adduser --system --ingroup rt --home /var/lib/rt --no-create-home --shell /usr/sbin/nologin \
      --gecos "21 Cloud runtime" rt >/dev/null
  else
    useradd --system --gid rt --home-dir /var/lib/rt --no-create-home --shell /sbin/nologin \
      --comment "21 Cloud runtime" rt
  fi
fi

exit 0
