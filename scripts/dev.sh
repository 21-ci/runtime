#!/usr/bin/env bash
# Local dev node: `cargo run -p rt-server --bin rt -- serve --config config/defconfigs/dev.toml`.
#
#   scripts/dev.sh [--release] [--watch] [--config FILE] [-- <extra rt args>]
#
#   --release       cargo run --release
#   --watch         rebuild and restart on source changes (needs `cargo install cargo-watch`)
#   --config FILE   node config (default: config/defconfigs/dev.toml; loopback listeners, small limits)
#   -- ARGS         passed to `rt` after `serve`
#
# Env (all optional):
#   RT_LOG          tracing filter (default: info,rt_server=debug); RUST_LOG is the fallback when RT_LOG is unset
#   DATA_DIR        data dir (default: data); anything but ./data is applied as RT__NODE__DATA_DIR
#   RT_ROOT_TOKEN   control API token (default: dev-root-token, fine because dev binds 127.0.0.1)
#   RT_MASTER_KEY   64 hex chars; when neither it nor RT_MASTER_KEY_FILE is set, a random key is
#                   created once in $DATA_DIR/dev-master.key and reused, so sealed secrets survive restarts
#
# Control API: http://127.0.0.1:8081  invoke: http://127.0.0.1:8080  metrics: http://127.0.0.1:9090
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

die() { echo "error: $*" >&2; exit 1; }
usage() { sed -n '2,/^set -euo/{/^set -euo/d;s/^# \{0,1\}//;p;}' "${BASH_SOURCE[0]}"; }

CONFIG="config/defconfigs/dev.toml"
RELEASE=()
WATCH=0
EXTRA=()
while [[ $# -gt 0 ]]; do
  case "$1" in
    -h|--help) usage; exit 0 ;;
    --release) RELEASE=(--release) ;;
    --watch)   WATCH=1 ;;
    --config)  [[ $# -ge 2 ]] || die "--config needs a file"; CONFIG="$2"; shift ;;
    --)        shift; EXTRA=("$@"); break ;;
    *)         die "unknown option '$1' (see --help)" ;;
  esac
  shift
done

command -v cargo >/dev/null 2>&1 || die "cargo not found (install Rust: https://rustup.rs)"
[[ -f "$CONFIG" ]] || die "config file not found: $CONFIG"

DATA_DIR="${DATA_DIR:-data}"
mkdir -p "$DATA_DIR"

rand_hex32() {
  if command -v openssl >/dev/null 2>&1; then openssl rand -hex 32
  else head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n'; fi
}

export RT_LOG="${RT_LOG:-${RUST_LOG:-info,rt_server=debug}}"
export RT_ROOT_TOKEN="${RT_ROOT_TOKEN:-dev-root-token}"

if [[ -z "${RT_MASTER_KEY:-}" && -z "${RT_MASTER_KEY_FILE:-}" ]]; then
  KEY_FILE="$DATA_DIR/dev-master.key"
  if [[ ! -s "$KEY_FILE" ]]; then
    ( umask 077; printf '%s' "$(rand_hex32)" > "$KEY_FILE" )
    echo "dev: created master key $KEY_FILE (dev only, never reuse it elsewhere)" >&2
  fi
  export RT_MASTER_KEY_FILE="$KEY_FILE"
fi

# dev.toml hardcodes ./data; a different DATA_DIR is applied through the env overlay.
case "$DATA_DIR" in
  data|./data|data/) ;;
  *)
    export RT__NODE__DATA_DIR="$DATA_DIR"
    export RT__LISTENERS__INTERNAL="unix:$DATA_DIR/rt.sock"
    ;;
esac

RT_ARGS=(serve --config "$CONFIG")
[[ ${#EXTRA[@]} -gt 0 ]] && RT_ARGS+=("${EXTRA[@]}")

echo "dev: config=$CONFIG data=$DATA_DIR RT_LOG=$RT_LOG" >&2
if [[ "$RT_ROOT_TOKEN" == "dev-root-token" ]]; then TOKEN_NOTE="token dev-root-token"; else TOKEN_NOTE="token from RT_ROOT_TOKEN"; fi
echo "dev: control http://127.0.0.1:8081 ($TOKEN_NOTE)  invoke http://127.0.0.1:8080  metrics http://127.0.0.1:9090" >&2

if [[ "$WATCH" == 1 ]]; then
  cargo watch --version >/dev/null 2>&1 || die "--watch needs cargo-watch: cargo install cargo-watch"
  RUN_CMD="run ${RELEASE[*]:-} -p rt-server --bin rt -- ${RT_ARGS[*]}"
  exec cargo watch -w crates -w js -w wit -w config -w Cargo.toml -x "$RUN_CMD"
fi

exec cargo run ${RELEASE[@]+"${RELEASE[@]}"} -p rt-server --bin rt -- "${RT_ARGS[@]}"
