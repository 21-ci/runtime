#!/bin/sh
# deb/rpm post-remove of `rt`: reload systemd. The data in /var/lib/rt, the `rt` user and /etc/rt are kept, even on
# purge: tenant data and sealed secrets are never deleted by the package manager. Remove them by hand.
set -eu

if [ -d /run/systemd/system ]; then
  systemctl daemon-reload >/dev/null 2>&1 || true
fi

case "${1:-}" in
  purge)
    echo "rt: /var/lib/rt, /etc/rt and the rt user were kept; remove them by hand if you no longer need the data." >&2
    ;;
esac

exit 0
