---
name: add-guest-api
description: Use when adding or changing a guest-facing `std:*` API of this runtime (a new module, method, option, limit or error code in std:kv, std:sql, std:sqlite, std:blob, std:queue, std:services, std:lock, std:schedule, std:secrets, std:sockets, std:durable, std:platform, std:sdk, std:log...) or the matching `std:host/*` WIT interface - i.e. anything in js/*.js, js/bootstrap.js, crates/rt-exec-deno ops, wit/rt-host.wit, crates/rt-exec-wasm/src/host_wit.rs or the shared logic in crates/rt-host. Gives the end-to-end recipe across both executors, tests, docs and samples.
---

# Adding or changing a guest API (`std:*` / `std:host/*`)

A guest API is a **stable contract** in two languages. Deno and WASM must behave the same: the same bounds, the same
error codes and the same scoping. So put the logic once in `rt-host`, and keep the executors thin.
The layer-by-layer file map, with `std:lock` as the worked example, is in [layer-map.md](layer-map.md).

Before you start:
- Read the "Guest-facing contracts" rule in [`AGENTS.md`](../../../AGENTS.md).
- Read the SECURITY.md sections "Deno sandbox", "WASM sandbox" and "Storage isolation".
- If the API takes guest input or holds host resources, also use the `security-review` skill.

## Recipe (in commit order, as `std:lock` was done: 241f76a → bcfb54c → 2ece5f0 → 1de5763)

### 1. Shared host logic: `feat(host): ...`
- Put the logic in `crates/rt-host/src/<area>.rs`, export it in `lib.rs`, and expose it as a method on `HostHandle`
  in `crates/rt-host/src/handle.rs`. Both executors call only the handle.
- **Scope everything to the calling task's service and tenant**, taken from the handle, never from a guest
  argument.
- **Limits:**
  - Hard bounds are `pub const`s next to the logic (e.g. `LOCK_KEY_MAX_BYTES`, `LOCK_KEYS_PER_SERVICE`).
  - Plan or config-driven limits go through `FacadeLimits` (`crates/rt-host/src/facade.rs`) or
    `rt_types::Limits` (`crates/rt-types/src/limits.rs`). For a new config key, use the `config-change` skill.
- **Arithmetic:** use saturating math on every guest number (TTL, delay, size).
- **Errors:**
  - Return `HostError::new("<code>", msg)`. The code is a stable `&'static str`, so reuse an existing one when it
    fits: `validation_failed`, `too_large`, `not_found`, `unavailable`, `<area>_limit`, `<area>_timeout`, ...
  - Store errors go through `HostError::from_store`, which keeps only the code.
  - Never put host paths, SQL driver text or stacks in the message.
- **Resources:**
  - Memory a guest can grow is charged to the governor.
  - Blocking work runs on the blocking pool, with its CPU charged to the task.
  - Release per-task resources at task end (as `release_locks` is called from both executors).
- Tests go in `crates/rt-host/tests/<area>.rs`. Assert the exact codes, e.g. with `code_of()` as in `tests/lock.rs`.

### 2. Deno: `feat(<area>): ... in Deno guests`
1. **Op.** Write `#[op2]` (async or `fast`) in `crates/rt-exec-deno/src/ops.rs`, or in `ops_<area>.rs` for a big
   surface. Get the handle with `host_of(&state)?` and map errors with `host_err` (the JS error message *is* the
   code).
2. **Register the op in three places, in the same order with the same arity and kind:**
   - the `deno_core::extension!(rt_ext, ops = [...])` list in `crates/rt-exec-deno/src/exts.rs`;
   - the `stub_ops!` list in `crates/rt-exec-deno/build.rs`;
   - the second `rt_ext` op list in `crates/rt-exec-deno/build.rs`.

   Drift fails `verify()` in `src/snapshot.rs`. Stub bodies must be unique (`a532a5f`).
3. **JS.** Add to `js/bootstrap.js`:
   - the op in the `core.ops` destructure;
   - the implementation, which validates options in JS too (throw `validation_failed`, mirroring the Rust consts);
   - wrap async ops in `guard(...)` so that `err.code` is set;
   - expose it on the frozen `globalThis.__rt`. Never add a writable global (`c65c59c`).

   Then add the module file `js/rt_<area>.js`, a header doc comment plus
   `export const x = globalThis.__rt.x;`.
4. **Make the module importable**, in two places:
   - `include_str!` and the `RT_MODULES` entry in `crates/rt-exec-deno/src/js.rs`;
   - the `BUILTIN_MODULES` allowlist in `crates/rt-build/src/resolve.rs`. Add to `BUILTIN_ALIASES` too if it has
     an alias.
5. **Test.** Add `crates/rt-exec-deno/tests/it/sdk_<area>.rs` and register it in `tests/it/main.rs`. Write the guest
   inline and run it with `Harness`. Name tests after the AC id, e.g. `ac_dno_34_*`. Test should-pass,
   should-fail (each code) and the kill/limit path.

### 3. WASM: `feat(<area>): std:host/<area> in WASM components`
1. **WIT.** In `wit/rt-host.wit`:
   - Add `interface <area> { record error { code: string, message: string } ... }`, with the codes documented in
     comments.
   - Add `import <area>;` to `world guest` and to the other worlds that need it (`rpc-service`,
     `socket-service`, `runtime-host`).
   - It is `package std:host@0.1.0`. Adding things is fine; changing or removing a function breaks every
     deployed component.
2. **Host impl.** In `crates/rt-exec-wasm/src/host_wit.rs` (or a new file like `host_ws.rs`):
   - Add a `with:` mapping in `bindgen!` for any resource.
   - Write `impl <area>::Host for WasmState`, which calls `self.host.<method>` through `self.clock.io(...)`
     and maps errors to `<area>::Error { code: e.code.to_string(), message }`.
   - Add `<area>::add_to_linker` in `add_to_linker`.
   - A call outside a task returns `UNAVAILABLE`.
3. **Fixture.** Use the API from a fixture: `fixtures/wasm/<name>/src/lib.rs` (`hello` holds several small routes),
   or a new crate in `fixtures/wasm/`. Rebuild with `make fixtures` (`scripts/build-fixtures.sh`). If `wasm-tools`
   is installed, it checks imports against the C-7 allowlist (`std:host/[a-z-]+`).
4. **Test.** Add `crates/rt-exec-wasm/tests/it/wsm_<area>.rs` and register it in `tests/it/main.rs`. Use `Rig`,
   `prepared("<fixture>")` and `Req`, with the same cases as the Deno test (`ac_wsm_NN_*`).

### 4. End to end (when triggers, durability or the node are involved)
Add a module in `crates/rt-server/tests/flows/` and register it in `tests/flows.rs` (e.g. `lock_schedule.rs`).

### 5. Docs and samples: `feat|docs(<area>): ...`

| File | What to change |
|---|---|
| `docs/handlers.md` | The `### std:<area>` section under "SDK modules (Deno)" (table, codes, the WASM line); the Imports row; the "Linked imports" list under "WASM component"; "Deno and WASM parity"; "Limits" |
| `README.md` | A row in the `std:*` module table (`\| Module \| What it does \|`), and any list of `std:host` interfaces |
| `SECURITY.md` | Scoping, caps and codes when the API holds resources or crosses a boundary (see the `std:lock` / `std:schedule` paragraphs under "Secrets") |
| `skills/21cloud-deno-service/references/std-modules.md`, `skills/21cloud-wasm-service/references/std-host-wit.md` (and `limits.md`) | The user-facing skills, which agents writing services read |
| `llms.txt` | When a module is added |
| `docs/` (this repo) | `docs/handlers.md` (guest contract), `docs/reference/guest-api.md`, `docs/reference/wit.md`, `docs/reference/errors.md`, `docs/reference/limits.md`. The site includes `wit/rt-host.wit` by path, so there is no copy to regenerate; `make docs-check` builds the site. |
| Samples | `sample/deno/*` or `sample/apps/*` if the API deserves an example. `sample/apps/edge-gateway/gateway/wit/rt-host.wit` is a vendored copy, so refresh it when the WIT changes. Run `sample/apps/run-smokes.sh` for apps you touched. |

There are no `.d.ts` files in this repo: types live in the header comments of `js/rt_*.js` and in the docs.

## Done when
- [ ] Bounds and codes are identical in `rt-host` consts, the `bootstrap.js` mirror, the WIT comments and the docs.
- [ ] Tests exist at each level: rt-host, Deno, WASM and, if relevant, flows. Each covers should-pass, each error
      code and cleanup on kill.
- [ ] Run `cargo test -p rt-host`, `cargo test -p rt-exec-deno --test it sdk_<area>::` and
      `cargo test -p rt-exec-wasm --test it wsm_<area>::`, then `make lint`.
- [ ] Run `make coverage MODULES="host exec-deno exec-wasm"`. The new API's logic (every limit, error code and state change) must be covered; keep each module at or above the CI floor (90 %), with an `LCOV_EXCL` reason for
      anything unreachable (e.g. the `bindgen!` closing line).
- [ ] Docs, skills and samples are updated in the same PR.
