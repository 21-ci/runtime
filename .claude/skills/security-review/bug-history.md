# Bug classes this runtime has actually had

These come from `git log --oneline | grep -E '^[0-9a-f]+ fix\('` (about 80 fixes). Read the diff of a commit with
`git show <hash>`; most of them include their regression test. When you review a change, look for the same shape.

## 1. Authorization gaps on a secondary path

The main route is checked, but a sibling route or tool is not.

| Commit | What was wrong |
|---|---|
| `0190b4e` | A key limited to some projects/environments could create services outside them (REST and MCP `create_service`) |
| `e5e53d7` | The operator acting as `sys` through `x-rt-tenant` could write; plan quotas were skipped on PATCH and under races |
| `e14fae3` | A key revoked during the request could still create a live child key |
| `845b1c2` | Revocation did not cascade; a weaker key could revoke a stronger one; the auth cache re-cached revoked keys |
| `ea2810b` | The open debug control API (no root token) served non-local callers |
| `48f2f47`, `b16dbbc` | The `change-me` placeholder, short tokens off loopback, and tokens with surrounding whitespace were accepted |
| `04df211` | The MCP service argument could be a path, not an id or slug |
| `525c16f`, `e9f7cb6` | SDK calls were not capped per calling tenant; a reload counted the system tenant's plan |

**Look for:** every route and tool that reaches the same store method. MCP tools replay `/v1`, so fix it in the
handler, not in the tool.

## 2. Parsing at the edge fails open

| Commit | What was wrong |
|---|---|
| `ce0dea0` | `X-Forwarded-For` hops written with a port or brackets were misread |
| `333237e` | An unterminated `[v6` `Origin` was taken as loopback by the MCP DNS-rebinding guard |
| `3d46cf7` | Invoke passed the root token to the guest, did not resolve dot segments, and let the guest own framing headers |
| `77c0c43` | Invoke dropped the guest's own `Authorization`, and accepted an invalid `traceparent` |
| `d9fe2fe` | Malformed `ip_allow` CIDRs and non-ASCII egress hosts were accepted |
| `d213e6d` | Tenant names, overrides on PATCH and usage dates were not validated |

**Look for:** hand-written parsers of headers, hosts, CIDRs and origins. Malformed input must be refused, or must
match nobody.

## 3. Unbounded guest-controlled numbers and sizes

| Commit | What was wrong |
|---|---|
| `0b457c8` | A guest's queue retry delay was not capped; queue timestamps overflowed |
| `d40bf51` | Interval schedules were not bounded; schedule arithmetic overflowed |
| `3f0308e` | `kv.set` / `incr` / `cas` accepted a bad `ttlSeconds` (`kv.expire` already refused it) |
| `87bcae4` | `kv.getMany` and durable `storage.get` had no key-count or byte cap |
| `4c81d39` | The response-headers op panicked instead of returning a cap error |
| `c979f24` | `memory.budget_mb` byte math overflowed |
| `eb0f4fc` | The socket listing was not clamped |

**Look for:** `as u64`, `* 1000`, `+ now` on guest values. Use `saturating_*` and checked bounds. When one method of
an API checks an argument, every other method that takes it must check it too.

## 4. Unbounded host state (memory DoS)

| Commit | What was wrong |
|---|---|
| `f4b2876` | The idempotency store had no bound on fresh entries or on key length |
| `2d274b7` | SSE counters and deleted services' ingress state were never pruned |
| `6d1e3d7` | Concurrent artifact uploads were neither capped nor charged to the governor |
| `3c3b2f8` | An open outbound WebSocket did not hold a `max_concurrent_fetches` slot |
| `af76725` | KV writes were charged per overwrite, not for growth |

**Look for:** a new `HashMap` or `DashMap` keyed by anything a client controls. Keep the SECURITY.md
"Long-lived maps" list in sync with the code.

## 5. Sandbox and guest surface

| Commit | What was wrong |
|---|---|
| `c65c59c` | The guest could replace or delete `globalThis.__rt`, which every `std:*` module reads |
| `e826798` | Reserved `/__rt/` call paths and the RPC cap were enforced in JS only, not in the call ops |
| `f8b93f2`, `1f7f713` | Review fixes for the Deno sandbox, reuse and streaming, and for WASM bindings |
| `2be11eb` | A failed WASM pre-instantiation did not reset its pool key inside the hook call |

**Look for:** checks done only in `js/bootstrap.js`. JS is guest-reachable: the host op must check again.

## 6. Storage isolation

| Commit | What was wrong |
|---|---|
| `daba865` | Guest SQL could call `fts3_tokenizer` (returns a native pointer) and rename tables into `_rt_*` |
| `9c51e61` | A sync SQL batch was checked only at its first statement; a failed commit could silently lose a transaction |
| `da54277` | A deleted service's data came back |
| `92bcfd6` | `blob.get` racing an overwrite read `null` |
| `daa781d` | Re-uploading an artifact's bytes did not repair a damaged copy |

## 7. Egress / SSRF

| Commit | What was wrong |
|---|---|
| `bb39eb3` | 6to4, Teredo and reserved ranges were not blocked; local interface addresses were missing from the self-denylist |
| `abc4aa4` | Operator endpoints (bindings, S3/OTLP) on a denied port were not refused |

## 8. Secrets leaking into logs or Debug

| Commit | What was wrong |
|---|---|
| `4bcd4bf` | Secret values were not redacted from a task's log records |
| `d6d6a76` | Redaction was not longest-first, and missed nested fields and keys |
| `9dbcddd` | `Debug` of a parsed key and of the load options printed secrets |

## 9. Supply chain

| Commit | What was wrong |
|---|---|
| `b0d7262` | Build imports could use plain `http` and could carry credentials in URLs |
| `4536560` | A lock could read a vendored module fetched for a different URL |
| `eb7fa05` | `rt.lock` was not enforced on rebuild |

## Commit style for a fix

`fix(<scope>): <the rule that now holds>`, in lowercase imperative, with the regression test in the same commit.
Common scopes: `api`, `sec`, `keys`, `mcp`, `invoke`, `egress`, `storage`, `host`, `deno`, `wasm`, `ws`, `build`, `sdk`.
