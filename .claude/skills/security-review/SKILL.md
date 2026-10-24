---
name: security-review
description: Use when reviewing or writing a change to this runtime that touches a trust boundary - auth, keys, scopes, x-rt-tenant or the system tenant, projects/environments, the invoke path and header rewriting, guest sandboxes (V8 ops, js/bootstrap.js, wasmtime host imports), egress/SSRF, the SQLite authorizer or other storage isolation, upload/bundle extraction, artifact MACs, secrets and log redaction, or the MCP endpoint. Also use when fixing a security bug, to write its regression test.
---

# Security review (runtime contributors)

Read [`SECURITY.md`](../../../SECURITY.md) first: it is the contract. A change that alters behaviour described there
must update it in the same PR. [`AGENTS.md`](../../../AGENTS.md) has the general repo rules.
The bugs this codebase has actually had are in [bug-history.md](bug-history.md). Skim it before you review, because
most new bugs repeat an old class.

## Trust model in one screen

| Actor | Trust | Enters through |
|---|---|---|
| Operator | full | root token (`RT_ROOT_TOKEN[_FILE]`); picks a tenant with `x-rt-tenant` |
| Tenant | partial | `rtk_` keys: scopes, `project_ids`, `environments`, expiry, parent key |
| Guest code | **none, hostile** | V8 isolate or wasmtime component in the **same process** as `meta.db` and the master key |
| Internet | none | public listener `/s/{slug}` and custom hosts; control if mis-exposed |

The sandbox is the boundary that matters most, because a V8 or wasmtime escape owns the node. There is no V8 sandbox
and no seccomp filter (SECURITY.md "Known limitations"). So every host op is an attack surface.

## Where each boundary lives

| Boundary | Code | Tests |
|---|---|---|
| Credentials, root token, open debug control API, failed-auth limiter | `crates/rt-sec/src/auth.rs` (`authenticate`, `control`, `public`, `check_open_control_bind`, `LIMITER_CAPACITY`, `change-me` check) | `crates/rt-sec/tests/ac_sec_auth.rs`, `ac_sec_ingress_auth.rs` |
| Key derivation, delegation (child ⊆ parent), revocation cascade | `crates/rt-sec/src/keys.rs`, `delegate.rs`; `crates/rt-api/src/tokens.rs`; `crates/rt-store/src/meta.rs` | `ac_sec_delegation.rs`, `crates/rt-server/tests/api/{tokens,edge_keys}.rs`, `crates/rt-store/tests/it/meta_key_scope.rs` |
| Principal, scopes, project/env restriction | `Principal::{has_scope,can_touch}` in `crates/rt-sec/src/auth.rs`; `Scope` + `scope_admits` in `crates/rt-types/src/model.rs`; `Auth` extractor in `crates/rt-api/src/auth.rs`; `load_service`, `restricted`, `not_reserved` in `crates/rt-api/src/control.rs` | `crates/rt-server/tests/api/{access,projects,tenancy,edge_control}.rs` |
| System tenant `sys` (read-only via `x-rt-tenant`, no keys, `sys-` slugs reserved) | `is_system_tenant` in `crates/rt-types/src/sdk.rs`; `crates/rt-api/src/auth.rs` (non-GET becomes `reserved`) | `crates/rt-server/tests/api/edge_control.rs` (`ten_005`, `ten_006`) |
| Invoke path: client IP / XFF, header strip and add, path normalisation | `crates/rt-api/src/invoke.rs` (`strip_hop_by_hop`, `strip_runtime_headers`, `client_ip`); `crates/rt-sec/src/ingress.rs` (`TrustedProxies::client_ip`) | `crates/rt-server/tests/flows/{edge_invoke,ingress}.rs` |
| Quotas and plans | `crates/rt-sec/src/quota.rs`; `check_limits` in `control.rs` | `crates/rt-server/tests/api/{tenancy,edge_control}.rs` |
| Deno sandbox: extensions, `__rt_seal`, banned globals, frozen `globalThis.__rt` | `crates/rt-exec-deno/src/exts.rs`, `snapshot.rs` (`seal`), `ops.rs`; `js/bootstrap.js` (`__rt_seal`, near the "Sandbox hardening" comment) | `crates/rt-exec-deno/tests/it/{sandbox,warm_reuse}.rs`, `fixtures/deno/forbidden_apis` |
| WASM sandbox: imports, memory, outbound HTTP | `crates/rt-exec-wasm/src/state.rs` (`GovernorLimiter`, `GuardedHttpHooks`), `host_wit.rs`; `crates/rt-build/src/wasm_policy.rs`; import allowlist in `scripts/build-fixtures.sh` | `crates/rt-exec-wasm/tests/it/{wsm_exec,wsm_host_ops,wsm_detached}.rs` |
| Egress / SSRF | `crates/rt-sec/src/egress.rs` (`is_blocked_ip`); `crates/rt-host/src/egress.rs` (`GuardedEgress`, `validate_endpoint`, `ClientPool`); `crates/rt-host/src/self_endpoints.rs` (self-denylist) | `crates/rt-sec/tests/ac_sec_egress.rs`, `crates/rt-host/tests/{egress,egress_pool}.rs` |
| Storage isolation, SQLite authorizer | `crates/rt-store/src/data.rs` (`guest_auth`, `DENIED_PRAGMAS`, `precheck`); `sync_sql.rs` (statement-by-statement batches) | `crates/rt-store/tests/it/{sql_store,hardening,sync_sql}.rs` |
| Upload / bundle extraction | `crates/rt-build/src/extract.rs` (`normalize_path`: absolute, `..`, file count, zip-bomb ratio), `validate.rs`, `sniff.rs`; `fetch.rs` + `lock.rs` (`rt.lock`, https only) | `crates/rt-build/tests/it/{validation,fetch_lock,limits_failures}.rs` |
| Artifact MAC (native code caches) | `crates/rt-sec/src/mac.rs`; `crates/rt-cache/src/t2.rs` (`artifact_tamper`) | `crates/rt-cache/tests/` |
| Secrets: sealing, write-only API, `Debug` redaction, log redaction | `crates/rt-sec/src/{secrets,master}.rs`; `redact_secrets` / `redact_value` in `crates/rt-host/src/handle.rs`; `redact_config` in `control.rs` | `crates/rt-sec/tests/ac_sec_crypto.rs`, `crates/rt-exec-deno/tests/it/{sdk_secrets,sdk_log_fields}.rs` |
| Guest-visible errors (no host paths or driver text) | `HostError::from_store` in `crates/rt-host/src/facade.rs`; `From<StoreError>` in `crates/rt-api/src/error.rs` | the tests of each API |
| Service calls, RPC, operator SDKs, bindings | `crates/rt-host/src/{calls,rpc}.rs`; `crates/rt-api/src/sdks.rs` | `crates/rt-server/tests/flows/{services_rpc,sdk}.rs`, `tests/api/sdk.rs` |
| MCP | `crates/rt-api/src/mcp/mod.rs` (`check_origin`, operator refused), `mcp/tools.rs` (`allowed`, `Def.scopes`; tools replay `/v1` in-process) | `crates/rt-server/tests/api/{mcp,edge_mcp}.rs` |
| WebSockets / durable objects | `crates/rt-triggers/src/sockets/hub.rs`; `crates/rt-host/src/sockets.rs`, `ws_client.rs` | `crates/rt-server/tests/flows/{websockets,durable}.rs` |

## Review checklist

Ask each question of the diff. Each "no" is a finding.

**Identity and authorization**
- [ ] Every new `/v1` handler starts with `a.require_scope(..)` or `a.require_operator()`. Tenant-owned objects are
      loaded through `load_service` or something like it, so that "not yours" and "missing" both give `404`.
- [ ] A restricted key (`project_ids` / `environments`) cannot create, move or see outside its set: `can_touch` is
      checked, and environment moves go through `restricted()`.
- [ ] `x-rt-tenant` as `sys` is read-only, and `not_reserved` guards tenant-targeted operator routes.
- [ ] Delegation only narrows: scopes, projects, environments and expiry are all ⊆ the parent. Check revocation that
      races the request (`e14fae3`).
- [ ] Every auth failure gives the same answer and costs the same (one HMAC).

**Guest input (treat every byte as hostile)**
- [ ] Every number from a guest is bounded and saturating, never `as` or wrapping. TTLs, delays, intervals and
      timestamps have all overflowed here before (`0b457c8`, `d40bf51`, `3f0308e`).
- [ ] Counts, sizes and collections have a cap with a stable code (`validation_failed`, `too_large`, `lock_limit`,
      ...). An op must **never panic** on input (`4c81d39`).
- [ ] Guest-visible error text is a stable code plus a message, with no host path, SQL driver text or stack.
- [ ] New JS on `globalThis` is defined non-writable and non-configurable (`c65c59c`). Deno and WASM get the same
      checks, because the checks live in `rt-host`, not in one executor.

**Resources shared between tenants**
- [ ] Every per-key, per-IP, per-tenant or per-service map on a request path has a cap or eviction (SECURITY.md "Long-lived maps").
- [ ] Pools are keyed so that they never cross a tenant: warm isolates and instances by (tenant, service, version,
      artifact); egress clients by (tenant, scheme, host, port, pinned IP).
- [ ] Memory a guest can make the host allocate is charged to the governor (`rt_mem::MemoryGovernor::reserve`).
      CPU spent on host-side work is charged to the task (`crates/rt-store/src/blocking.rs`).

**Egress, storage, builds**
- [ ] New outbound traffic (guest, binding, build, operator endpoint) goes through `GuardedEgress`. Check ports, DNS
      pinning, every resolved address, re-checks on redirects and the self-denylist (`abc4aa4`).
- [ ] New guest SQL surface passes the authorizer, statement by statement. Check `_rt_*` names, pragmas and native
      pointers (`daba865`, `9c51e61`).
- [ ] Paths from uploads go through `normalize_path`. The host names blob files. Lock entries bind to their URL (`4536560`).

**Secrets and logs**
- [ ] No secret, key, token or guest payload goes into `tracing`, `Debug`, audit `data` or errors (`9dbcddd`). New
      log record fields pass through `redact_secrets` (`d6d6a76`).
- [ ] Write-only stays write-only: secret APIs return `{name, updated_at}` only.

**Parsing at the edge**
- [ ] Header and URL parsing handles malformed input: brackets, ports, `[v6`, dot segments, non-ASCII hosts and
      whitespace around tokens (`ce0dea0`, `333237e`, `3d46cf7`, `d9fe2fe`, `b16dbbc`). Malformed input fails closed.

**Docs**
- [ ] SECURITY.md is updated, including its tables: MCP scope table, "Long-lived maps", limits. Note that the MCP
      table currently lacks `list_sdks`, `get_sdk_contract`, `list_objects`, `list_sockets` and `close_socket` from
      `DEFS`.

## Writing the regression test (do this first)

1. Reproduce the attack as a test that **fails before the fix**. Put it at the lowest layer that shows the bug:
   - pure classifier or parser: a unit test in `src/` (e.g. `normalize_path` in `rt-build/src/extract.rs`,
     `check_origin` in `rt-api/src/mcp/mod.rs`). `is_blocked_ip` is pinned in `crates/rt-sec/tests/ac_sec_egress.rs`;
   - host op: `crates/rt-host/tests/*.rs`, or the executor binaries `crates/rt-exec-deno/tests/it/` and
     `crates/rt-exec-wasm/tests/it/`;
   - storage: a module in `crates/rt-store/tests/it/`;
   - API or invoke: `crates/rt-server/tests/api/edge_*.rs` or `tests/flows/edge_invoke.rs`. The `edge_*` modules
     are where boundary cases go (CONTRIBUTING.md).
2. Assert the **exact** status and stable code, and that nothing happened. For example `edge_control.rs`
   `ten_005_no_keys_for_the_system_tenant_through_v1_keys` checks `(403, "reserved")` and also that the key list is
   still empty.
3. Add the neighbouring should-pass case, so the fix does not just deny everything (e.g. a legitimate `localhost`
   origin still passes; the operator can still `GET` as `sys`).
4. Cover both executors when the surface exists in both: a Deno test plus a `wsm_*` test.
5. Doc-comment the test with the rule it pins, and cite the SECURITY.md section.
6. Commit as `fix(<scope>): <the rule that now holds>`, with the test in the same commit (see bug-history.md for
   the house style).

Run only what you touched: for example `cargo test -p rt-server --test api edge_control::` or
`cargo test -p rt-store --test it sql_store::`. Then run `make lint`, and `make coverage MODULES="<crate>"`
(every trust-boundary check needs a test that fails without it).
