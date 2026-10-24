#!/bin/sh
# deb/rpm post-install of `rt`: fix ownership, reload systemd, restart on upgrade. A fresh install is not started:
# the node needs RT_ROOT_TOKEN and RT_MASTER_KEY first.
set -eu

chown root:rt /etc/rt /etc/rt/rt.toml 2>/dev/null || true
chmod 0750 /etc/rt 2>/dev/null || true
chown rt:rt /var/lib/rt 2>/dev/null || true

if [ -d /run/systemd/system ]; then
  systemctl daemon-reload >/dev/null 2>&1 || true
  if systemctl is-active --quiet rt; then
    systemctl try-restart rt >/dev/null 2>&1 || true
  else
    for f in /etc/default/rt /etc/sysconfig/rt; do
      [ -f "$f" ] && env_file=$f && break
    done
    cat <<EOF
rt is installed but not started. Next steps:
  1. put RT_ROOT_TOKEN and RT_MASTER_KEY in ${env_file:-/etc/default/rt}   (generate: openssl rand -hex 32)
  2. review /etc/rt/rt.toml
  3. sudo systemctl enable --now rt
Docs: https://21-ci.github.io/runtime/
EOF
  fi
fi

exit 0
