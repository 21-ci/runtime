#!/usr/bin/env bash
# Restore drill: encrypted backup of a live local node, restore into an empty directory with the
# age identity, start a second node on it, compare. No Docker. Not in run_all.sh (it starts nodes).
#   RT_BIN=target/debug/rt tests/e2e/restore_drill.sh
# Needs curl, jq, age-keygen.
set -euo pipefail
RT_BIN="${RT_BIN:-target/debug/rt}"
WORK="$(mktemp -d)"
TOKEN="drill-root-token-0123456789abcdef"
PIDS=()
cleanup() { for p in "${PIDS[@]:-}"; do [ -n "$p" ] && kill "$p" 2>/dev/null || true; done; [ -n "${KEEP_WORK:-}" ] && echo "$WORK" || rm -rf "$WORK"; }
trap cleanup EXIT
fail() { echo "FAIL restore_drill: $*"; exit 1; }
command -v age-keygen >/dev/null || { echo "SKIP restore_drill: no age-keygen"; exit 0; }

age-keygen -o "$WORK/identity.txt" 2>/dev/null
RECIPIENT="$(age-keygen -y "$WORK/identity.txt")"
head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n' >"$WORK/master.key"
chmod 600 "$WORK/master.key"
export RT_ROOT_TOKEN="$TOKEN" RT_MASTER_KEY_FILE="$WORK/master.key"

conf() { # conf data_dir public control metrics
  sed -E -e "s|\./data|$1|g" \
    -e "s|127\.0\.0\.1:8080|127.0.0.1:$2|; s|127\.0\.0\.1:8081|127.0.0.1:$3|; s|127\.0\.0\.1:9090|127.0.0.1:$4|" \
    config/defconfigs/dev.toml |
    awk -v r="$RECIPIENT" '/^\[backup\]/{b=1} /^\[backup\.s3\]/{b=0}
      b&&/^enabled/{print "enabled = true";next} b&&/^encrypt_to/{print "encrypt_to = [\"" r "\"]";next} {print}'
}
api() { curl -sS -H "authorization: Bearer $TOKEN" "$@"; }
wait_up() { for _ in $(seq 120); do curl -fsS "$1/healthz" >/dev/null 2>&1 && return 0; sleep 0.5; done; fail "node at $1 did not start"; }

conf "$WORK/a" 18180 18181 19190 >"$WORK/a.toml"
"$RT_BIN" --config "$WORK/a.toml" >"$WORK/a.log" 2>&1 & PIDS+=($!)
A=http://127.0.0.1:18181
wait_up http://127.0.0.1:19190

TENANT="$(api -X POST "$A/v1/tenants" -H 'content-type: application/json' -d '{"name":"drill","plan":"free"}' | jq -r .id)"
[ -n "$TENANT" ] && [ "$TENANT" != null ] || fail "no tenant"

ID="$(api -X POST "$A/v1/node/backups" | jq -r .id)"
for _ in $(seq 240); do
  STATE="$(api "$A/v1/node/backups/$ID" | jq -r .state)"
  [ "$STATE" = ok ] && break
  [ "$STATE" = failed ] && fail "backup failed: $(api "$A/v1/node/backups/$ID" | jq -c .failure)"
  sleep 0.5
done
[ "$STATE" = ok ] || fail "backup did not finish"
ARCHIVE="$WORK/a/backups/rt-backup-$ID.tar.age"
[ -f "$ARCHIVE" ] || fail "no encrypted archive"
[ ! -e "${ARCHIVE%.age}" ] || fail "a plaintext archive exists"
[ "$(head -c 21 "$ARCHIVE")" = "age-encryption.org/v1" ] || fail "the archive is not age"

"$RT_BIN" --config "$WORK/a.toml" backup restore "$ARCHIVE" --data-dir "$WORK/nope" >/dev/null 2>&1 &&
  fail "restore without an identity worked"
"$RT_BIN" --config "$WORK/a.toml" backup verify "$ARCHIVE" --age-identity "$WORK/identity.txt" >/dev/null || fail "verify"
"$RT_BIN" --config "$WORK/a.toml" backup restore "$ARCHIVE" --data-dir "$WORK/b" --age-identity "$WORK/identity.txt" >/dev/null || fail "restore"

conf "$WORK/b" 18280 18281 19290 >"$WORK/b.toml"
"$RT_BIN" --config "$WORK/b.toml" >"$WORK/b.log" 2>&1 & PIDS+=($!)
B=http://127.0.0.1:18281
wait_up http://127.0.0.1:19290
[ "$(api "$B/v1/tenants/$TENANT" | jq -r .id)" = "$TENANT" ] || fail "the tenant did not come back"
[ "$(api "$B/v1/node/backups/status" | jq -r '.last_restored_at != null')" = true ] || fail "no node.restored"
echo "PASS restore_drill"
