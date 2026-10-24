#!/bin/sh
# deb/rpm pre-remove of `rt`: stop and disable the unit on removal (not on upgrade).
# deb passes "remove" / "upgrade"; rpm passes the number of versions left (0 = removal).
set -eu

case "${1:-}" in
  remove | purge | 0)
    if [ -d /run/systemd/system ]; then
      systemctl disable --now rt >/dev/null 2>&1 || true
    fi
    ;;
esac

exit 0
