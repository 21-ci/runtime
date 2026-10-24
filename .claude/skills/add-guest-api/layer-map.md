# Layer map: `std:lock` traced through every layer

Use this as a template for a new or changed API. Line numbers drift, so search for the names.

## Host (shared by both executors)
- `crates/rt-host/src/lock.rs` holds `LockTable`, `LockHold`, `validate` (gives `validation_failed`), `acquire` and
  `abandon`. It also has the consts `LOCK_KEY_MAX_BYTES`, `LOCK_WAIT_MAX`, `LOCK_LEASE_MAX`,
  `LOCK_KEYS_PER_SERVICE` and `LOCK_WAITERS_PER_KEY`. Its codes are `lock_limit` and `lock_timeout`.
- `crates/rt-host/src/lib.rs` has `pub mod lock;` plus `pub use`.
- `crates/rt-host/src/handle.rs`:
  - `HostServices.locks: Arc<LockTable>`;
  - `MAX_HELD_LOCKS`;
  - `HostHandle::lock_acquire` (detached gives `unavailable`), `lock_release` and `release_locks` (task end).
- `crates/rt-host/src/facade.rs` has `HostError { code: &'static str, message }`, `HostError::new` and
  `HostError::from_store`, which keeps the stable code only. `FacadeLimits` holds the config-driven sizes.
- The tests are `crates/rt-host/tests/lock.rs`. It is a flat file: rt-host has no `tests/it/` binary.

## Deno
- `js/rt_lock.js` is a doc header plus `export const lock = globalThis.__rt.lock;`.
- `js/bootstrap.js`:
  - the op names in the `core.ops` destructure;
  - `coded()` / `guard()`, which turn an op error message into `err.code`;
  - the `std:lock` section: JS mirrors of the bounds, option validation, the `try`/`finally` release;
  - `globalThis.__rt` defined frozen.
- `crates/rt-exec-deno/src/ops.rs`:
  - `host_of`, `host_err`, `code_err`;
  - `#[op2] async fn op_rt_lock_acquire` and `#[op2(fast)] fn op_rt_lock_release`.
  - Larger surfaces have their own files: `ops_durable.rs`, `ops_sqlite.rs`, `ops_ws.rs`.
- `crates/rt-exec-deno/src/exts.rs` has the `deno_core::extension!(rt_ext, ..., ops = [...])` list.
- `crates/rt-exec-deno/build.rs` has the `stub_ops!` list (`asynchronous` / `fast`, matching arity) and the
  second `rt_ext` op list. These snapshot `bootstrap.js` and `sqlite.js`; the `rt_*.js` modules are not in the
  snapshot.
- `crates/rt-exec-deno/src/snapshot.rs` has `verify()`, which fails when `build.rs` and `exts.rs` drift.
- `crates/rt-exec-deno/src/js.rs` has the `include_str!` per module and `RT_MODULES` (specifier to source), served by
  `bundle_loader.rs` through `rt_module_source`.
- `crates/rt-build/src/resolve.rs` has `BUILTIN_MODULES` (the importable `std:*` allowlist) and `BUILTIN_ALIASES`
  (`bun:sqlite` → `std:sqlite`).
- `crates/rt-exec-deno/src/executor.rs` calls `handle.release_locks()` at task end.
- The tests are `crates/rt-exec-deno/tests/it/sdk_lock.rs`, which writes the guest inline and uses
  `Harness::new(cfg0())`, `h.prepare(&ts_upload(SRC))` and `spec().http_get(..)`. Whole-app fixtures live in
  `fixtures/deno/<name>/`.

## WASM
- `wit/rt-host.wit` has `package std:host@0.1.0`; `interface lock { record error {code, message}; resource lease
  { release: func(); } acquire: func(...) -> result<lease, error>; }`; and `import lock;` in `world guest` and
  the other worlds.
- `crates/rt-exec-wasm/src/host_wit.rs`:
  - `bindgen!({ path: "../../wit", world: "runtime-host", with: { "std:host/lock.lease": super::LockLease } })`;
  - `add_to_linker`;
  - `impl lock::Host for WasmState` and `impl lock::HostLease`;
  - the `UNAVAILABLE` const.
- `crates/rt-exec-wasm/src/prepared.rs` calls `host_wit::add_to_linker`.
- `crates/rt-exec-wasm/src/executor.rs` calls `handle.release_locks()` at task end.
- `fixtures/wasm/hello/src/lib.rs` has `wit_bindgen::generate!({ path: "../../../wit", world: "guest" })` and the
  `/lock` route. Build it with `make fixtures`, which writes `fixtures/wasm/out/hello.wasm` (gitignored).
- The tests are `crates/rt-exec-wasm/tests/it/wsm_lock.rs`, using `Rig::new()`, `prepared("hello")` and
  `Req::get("/lock?...")`. Helpers live in `tests/common/mod.rs`.

## Node level
- `crates/rt-server/tests/flows/lock_schedule.rs`.

## Docs touched by the lock commits
`README.md`, `SECURITY.md` (the `std:lock` paragraph), `docs/handlers.md` (`### std:lock`),
`sample/apps/telegram-bot/src/main.ts` (usage). The user-facing skill references
(`skills/21cloud-deno-service/references/std-modules.md` `## std:lock`, `skills/21cloud-wasm-service/references/std-host-wit.md`)
must change along with them.

## Smaller change to an existing API (161d2f8, kv cas/expire/ttl)
1. `js/rt_kv.js` and `js/bootstrap.js`
2. `crates/rt-exec-deno/{build.rs,src/exts.rs,src/ops.rs}`
3. A new `tests/it/sdk_kv_cas_expire.rs`
4. `wit/rt-host.wit` and `crates/rt-exec-wasm/src/host_wit.rs`
5. `fixtures/wasm/kv/src/lib.rs` and `tests/it/wsm_kv_cas.rs`

Plus the docs.

## Known drift (fix when you are there)
- `sample/apps/edge-gateway/gateway/wit/rt-host.wit` is behind `wit/rt-host.wit`. It lacks `sockets`, `durable`,
  `ws-client` and `socket-handler`.
- When the `std:*` surface changes, check `docs/reference/guest-api.md` and `docs/reference/wit.md` for every module
  (lock, secrets and schedule sections included).
