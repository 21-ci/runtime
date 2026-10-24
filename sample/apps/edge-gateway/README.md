# edge-gateway

An authenticating API gateway compiled to a WebAssembly component, the pattern where latency and
security matter most. Every request to `/v1/*` is checked at the edge: a per-address rate limit, a
short-lived HS256 token (signature checked in constant time), the scope for the route, a per-subject
rate limit and the shape of the request. Only then does the gateway forward it, in-process, to
internal services that cannot be reached any other way. Every decision is audited: after answering,
the gateway ships the event to the operator's own API through the operator SDK (a runtime service:
no SDK code in the component) and hands it to an audit service that keeps a queryable trail and
retries what could not be shipped.

```
client ──HTTPS──▶ edge-gateway  (WASM component, public, environment "edge")
                    1. path / method / query validation, routing        → 400 404 405
                    2. rate limit per client address (x-rt-client-ip,   → 429 Retry-After
                       IPv6 per /64; std:host/kv incr, fixed window)
                    3. Bearer JWT: alg pinned to HS256, kid → key,      → 401
                       HMAC-SHA256 verify (constant time), exp/nbf/iat,
                       lifetime ≤ 900 s, aud, optional iss
                    4. scope for route + method                         → 403
                    5. rate limit per `sub` (std:host/kv incr)           → 429 Retry-After
                    6. body: application/json, ≤ 64 KiB, well-formed    → 400 413 415
                    7. std:host/call ──────────────▶ edge-notes  (Deno, access=internal, env "edge-backend",
                       + x-gw-sub, x-gw-scopes,       │             call.allowed_callers = [edge-gateway])
                         x-gw-request-id,             └─ per-subject notes in std:sql
                         x-gw-client-ip
                    ── response sent (buffered, content-length) ──
                    8. std:host/sdk invoke("acme", "audit.ship", {source, events: [event]})
                       the operator SDK runs in the runtime; its binding injects the credentials
                    9. std:host/queue ─────────────▶ edge-audit  (Deno, same isolation)
                       {event, shipped, ship_error},   ├─ std:sql, INSERT OR IGNORE by event id
                       dedup id = task id              │  (shipped ones are stored as shipped)
                                                       └─ not shipped: sdk("acme").audit.ship(…) in batches,
                                                          now and on its schedule
```

## Why WASM here

| | WASM component (this gateway) | A JS isolate |
|---|---|---|
| Start per request | a fresh, pre-instantiated instance: microseconds | an isolate plus module evaluation (milliseconds without a warm pool) |
| Memory | linear memory of a ~215 KB component, a few MB at most | a V8 heap per isolate (tens of MB) |
| Isolation | no shared state between requests, capability imports only (`std:host/{log,kv,queue,call,sdk}`, WASI env and clocks); no filesystem preopens, sockets denied | strong, but a larger runtime surface |
| Predictability | no JIT warm-up or GC pauses, so steady tail latency | JIT and GC |

A gateway runs on every request and holds the signing keys, so it should be the fastest and most locked-down
piece in the path. The backends can be ordinary Deno services: in the runtime's product split,
WASM is for the hot, stateless, security-critical path and Deno for simple services.

## Platform features used

| Feature | Where |
|---|---|
| WASI P2 component exporting `wasi:http/incoming-handler` | `gateway/src/component.rs` (`wasip2` crate) |
| `std:host/kv` `incr` with TTL | exact rate-limit counters, created with the window's TTL: `rl/ip/<address or /64>/<window>`, `rl/sub/<sub>/<window>` |
| `x-rt-client-ip` | the client address the platform resolved (behind `ingress.trusted_proxies`, a client-sent one is stripped): per-address limit, audit trail, `x-gw-client-ip` for backends |
| `std:host/sdk` `invoke` | the gateway ships each audit event with the operator SDK's `audit.ship`, and serves `GET /v1/platform` from its `getPlatformInfo`; an SDK release, rollback or removal reaches it without a redeploy |
| `std:host/call` | in-process forwarding to `edge-notes` / `edge-audit`; the runtime enforces `call.allowed_callers` |
| `std:host/queue` | audit events with the shipping result to `edge-audit`, with a dedup id |
| `std:host/log` | one line per request plus warnings |
| Secrets via the WASI environment | `GATEWAY_KEYS` (several keys for rotation, picked by `kid`) |
| `access: "internal"`, `environment`, `call.allowed_callers` | the backends are reachable only through the gateway |
| `std:sdk` (Deno) | `edge-audit` ships, in batches, what the gateway could not, with `sdk(AUDIT_SDK).audit.ship` |
| Queue consumer (Form B `queue`) | `edge-audit` |

## Setup

Build (needs cargo and the `wasm32-wasip2` target; `wasm-tools` is optional and checks the imports):

```bash
cd sample/apps/edge-gateway
gateway/build.sh --test          # unit tests on the host, then target/wasm32-wasip2/release/edge_gateway.wasm
```

Create the services with their isolation first, then push:

```bash
21ci service create edge-notes --runtime deno --access internal --env edge-backend --allowed-caller edge-gateway
21ci service create edge-audit --runtime deno --access internal --env edge-backend --allowed-caller edge-gateway
21ci service create edge-gateway --runtime wasm --env edge
21ci push notes/main.ts --service edge-notes --wait
21ci push audit/src --service edge-audit --wait
21ci push gateway/target/wasm32-wasip2/release/edge_gateway.wasm --service edge-gateway --wait

KEY=$(openssl rand -hex 32)                         # keep it: clients sign tokens with it
printf %s "k1:$KEY" | 21ci secret set edge-gateway GATEWAY_KEYS
21ci env set edge-gateway GATEWAY_RATE_LIMIT=120 GATEWAY_IP_RATE_LIMIT=600 GATEWAY_RATE_WINDOW=60
21ci schedule add edge-audit --every 5m             # optional: retries shipping, prunes after 30 days
```

The operator SDK for audit shipping and `/v1/platform`: set up [`sample/operator-sdk`](../../operator-sdk/) (the `acme`
binding, the mock API on port 3000, a plan in the binding's `allowed_plans`), then, with the operator token:

```bash
21ci sdk publish acme 1.0.0 ../../operator-sdk/acme-sdk
21ci sdk grants set node acme --allow
```

Without it, events stay in `edge-audit`'s own database and `/v1/audit/recent` reports why they were not shipped
(`sdk_not_found`, `sdk_not_granted`, `sdk_unavailable`, or the SDK's own code such as `binding_not_allowed`). The gateway
and `edge-audit` use the same SDK (`GATEWAY_SDK` / `AUDIT_SDK`): keep them equal.

### Gateway settings

| Name | Kind | Default | Meaning |
|---|---|---|---|
| `GATEWAY_KEYS` | secret | required | `kid:hex[,kid:hex]`, each key ≥ 32 bytes. Rotation: add `k2`, move clients to it, remove `k1` |
| `GATEWAY_AUDIENCE` | env | `edge-gateway` | required `aud` |
| `GATEWAY_ISSUER` | env | unset | required `iss` when set |
| `GATEWAY_MAX_TOKEN_TTL` | env | `900` | max `exp - iat` in seconds |
| `GATEWAY_RATE_LIMIT` | env | `120` | requests per window per `sub`; `0` turns it off |
| `GATEWAY_IP_RATE_LIMIT` | env | `600` | requests per window per client address (IPv6: per /64), checked before the token; `0` turns it off. Skipped when the request has no `x-rt-client-ip` |
| `GATEWAY_RATE_WINDOW` | env | `60` | window in seconds, for both limits |
| `GATEWAY_NOTES_SERVICE` / `GATEWAY_AUDIT_SERVICE` | env | `edge-notes` / `edge-audit` | backend slugs |
| `GATEWAY_AUDIT` | env | `mutations` | `all`, `mutations` (writes and every denial) or `off` |
| `GATEWAY_SDK` | env | `acme` | operator SDK the gateway ships each event with (`audit.ship`) and answers `/v1/platform` from; `off`: neither, `edge-audit` ships every event in batches |
| `GATEWAY_MAX_BODY` | env | `65536` | body limit in bytes |

`edge-audit`: `AUDIT_SDK` (default `acme`; `off`: it never ships, so events the gateway could not ship stay local),
`AUDIT_RETENTION_DAYS` (default 30, for shipped events). To keep every event local, set both `GATEWAY_SDK=off` and
`AUDIT_SDK=off`.

## Tokens

A compact JWT: header `{"alg": "HS256", "typ": "JWT", "kid": "k1"}`, payload
`{"sub": "alice", "aud": "edge-gateway", "iat": …, "exp": …, "scope": "notes:read notes:write"}`, signed with HMAC-SHA256.
Scopes: `notes:read` (GET), `notes:write` (POST, PUT, PATCH, DELETE), `audit:read`.

```bash
GATEWAY_KEY_HEX=$KEY ./mint-token.sh alice "notes:read notes:write" 300      # for local use
```

In a token issuer (Deno, Node, browsers: WebCrypto):

```ts
const b64 = (b: Uint8Array) => btoa(String.fromCharCode(...b)).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
const enc = (o: unknown) => b64(new TextEncoder().encode(JSON.stringify(o)));
const hexToBytes = (h: string) => new Uint8Array(h.match(/../g)!.map((x) => parseInt(x, 16)));
const now = Math.floor(Date.now() / 1000);
const input = `${enc({ alg: "HS256", typ: "JWT", kid: "k1" })}.${enc({ sub: "alice", aud: "edge-gateway", iat: now, exp: now + 300, scope: "notes:read" })}`;
const key = await crypto.subtle.importKey("raw", hexToBytes(KEY), { name: "HMAC", hash: "SHA-256" }, false, ["sign"]);
const token = `${input}.${b64(new Uint8Array(await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(input))))}`;
```

## API (through the gateway)

| Method and path | Scope | Backend |
|---|---|---|
| `GET /healthz` | none | the gateway itself (`503` when misconfigured) |
| `GET /v1/notes?limit=&before=` | `notes:read` | `edge-notes GET /`: own notes, newest first |
| `POST /v1/notes` `{text, tags?}` | `notes:write` | `201` with `Location` |
| `GET /v1/notes/{id}` | `notes:read` | own note or `404` |
| `PUT /v1/notes/{id}` `{text, tags?}` | `notes:write` | |
| `DELETE /v1/notes/{id}` | `notes:write` | |
| `GET /v1/audit/recent?limit=&sub=&ip=` | `audit:read` | `edge-audit`: events (with `ip` and `shipped_at`) plus shipping state: `{pending, last: {at, ok, error, by: "gateway" \| "edge-audit", sdk?}}` |
| `GET /v1/platform` | `notes:read` | the operator SDK's `getPlatformInfo` (`sdk` says which release answered); `502` with the SDK's code (`sdk_unavailable`, …) when it cannot |

Gateway errors are `{"error": {"code", "message"}}`: `invalid_request`, `not_found`, `method_not_allowed`, `invalid_token`
(with `WWW-Authenticate: Bearer`), `insufficient_scope`, `rate_limited` (with `Retry-After`; the audit reason tells
`ip_rate_limited` from the per-subject `rate_limited`), `unsupported_media_type`, `too_large`, `invalid_json`,
`upstream_unavailable` (the call failed: `not_found`, `call_not_allowed`, …), `upstream_error` (the backend answered 5xx;
its body is not passed on). Every answer carries `x-request-id` (the task id) and `content-length`, and
`ratelimit-limit`, `ratelimit-remaining` and `ratelimit-reset` for the per-subject limit once the client is known (on a
per-address `429` they describe the per-address limit).

```bash
B=http://127.0.0.1:8080/s/edge-gateway
T=$(GATEWAY_KEY_HEX=$KEY ./mint-token.sh alice "notes:read notes:write")
curl -sS -X POST $B/v1/notes -H "Authorization: Bearer $T" -H 'Content-Type: application/json' -d '{"text": "hi"}'
curl -sS $B/v1/notes -H "Authorization: Bearer $T"
curl -sS http://127.0.0.1:8080/s/edge-notes/      # 404: the backend is not on the public listener
```

## Tests

```bash
gateway/build.sh --test                      # 21 Rust tests: tokens (incl. a token from mint-token.sh), routing, paths,
                                             # client address, limits and keys, audit event and shipping, config
(cd audit && node --test test/*.test.ts)     # 5 tests: event validation, shipping state (Node >= 22.18), or: deno test
./smoke.sh                                   # against a node: deploys edge-smoke-{gateway,notes,audit}
```

`smoke.sh` covers forged, expired, long-lived, wrong-audience and `alg=none` tokens, scopes, per-subject isolation (a spoofed
`x-gw-sub` is dropped), validation, the per-subject limit, the per-address limit cutting forged tokens before any token check,
the runtime refusing the gateway once it leaves `allowed_callers`, the audit trail (filters by subject and address;
with `EXPECT_SHIPPED=1`, that events reached the operator API), and the operator SDK: it publishes the SDK as `acme_<run>`
(1.0.0), then 1.1.0 on `stable` (`getPlatformInfo` lists the integrations), a rollback and a removal, each reaching the
running gateway on its next call while its version stays the same. It needs `RT_ROOT_TOKEN` (the operator token).

## Notes and limits

- **Fail closed, fail open.** Missing or invalid `GATEWAY_KEYS` → `503` for everything (nothing is forwarded unverified).
  A KV error in either rate limiter lets the request through and logs a warning (availability over strictness).
- **Rate limits are exact within a window.** `kv.incr` counts atomically, so concurrent requests never share a count; the
  counter gets its TTL when the first request of a window creates it. Denied requests count too. A fixed window still
  admits up to twice the limit across a window boundary (the end of one window plus the start of the next), so keep the
  service's `ingress.rate_limit` as the global cap.
- **Per-address limit.** It is keyed on `x-rt-client-ip`, which the platform resolves like the ingress `ip_allow` rule:
  the peer address, or what a proxy listed in `ingress.trusted_proxies` forwarded. A client-sent `x-rt-client-ip` is
  stripped first, so it cannot be spoofed. Clients behind one NAT or proxy share a budget: size `GATEWAY_IP_RATE_LIMIT` for that. IPv6 counts per /64.
  Requests without the header (not over the public listener) skip this limit.
- **The bearer token reaches the gateway unchanged.** The platform removes only its own API keys (`Bearer rtk_…`) from
  `Authorization`; a JWT passes through to the component.
- **Clock.** WASM clocks are frozen at task start (real time on a reused instance), which is the right reference for `exp`.
  A 30 s leeway covers skew between the issuer and the node.
- **Backends trust `x-gw-*`** only because nothing else can call them: `access: internal`, `call.allowed_callers`, and their
  own check that `ctx.trigger === "call"`. The gateway drops every client header except `content-type`, `accept`,
  `if-match` and `if-none-match`, and the platform's `x-rt-*` too; the client address goes on as `x-gw-client-ip`.
- **Audit work happens after the response, but holds the instance.** The gateway answers first (a buffered body with
  `content-length`, so the client has the whole response at once), then ships the event and enqueues it. The runtime ends
  the response stream only when the handler returns, so the instance and its concurrency slot stay busy for that round
  trip to the operator API, bounded by the service's wall limit. Without `content-length` a client would wait for it too.
  To keep the hot path free of the outbound call, set `GATEWAY_SDK=off`: `edge-audit` then ships every event in batches.
- **Delivery is at least once.** Shipping never fails a request: a refusal (`sdk_not_granted`, `sdk_unavailable`, the
  SDK's `binding_not_allowed` or `rate_limited` from its binding, …) is logged, the event reaches `edge-audit` with
  `shipped: false`, and `edge-audit` retries it in a batch right away and on its schedule. An attempt that timed out after
  the operator API had accepted it is shipped again, so the operator API should deduplicate by event `id` (the task id).
  If the enqueue itself fails (logged), a shipped event is missing from the local trail only.
- `std:host/call` and `std:host/sdk` buffer request and response bodies (no streaming yet), which is fine for JSON APIs
  within `GATEWAY_MAX_BODY`.
