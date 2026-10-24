---
title: About
description: What 21 Cloud Functions Runtime is, the goals behind it, the open core and paid editions, and the license.
---

# About

21 Cloud Functions Runtime is a self-hosted, multi-tenant serverless runtime built by 21 Cloud. This page explains
what the project sets out to do, how the editions differ, and what the license allows. For the current state of the
code, see [Status and roadmap](status.md).

## The project

The runtime lets an operator host other people's code on their own servers. Tenants deploy TypeScript/JavaScript,
run in Deno V8 isolates, or WebAssembly components, run by wasmtime, through a REST API or the `21ci` CLI. Each
service gets an HTTP route, schedules, a durable queue, its own KV, SQL and blob storage, environment variables,
sealed secrets and calls to other services. The whole platform is one binary (`rt`) in one container with one data
volume, and it needs no external database or message broker.

The two runtimes have different jobs:

- **Deno** runs simple services, including services written by AI assistants: npm and jsr packages that only need
  `fetch`, WebCrypto and streams (resolved through a CDN at build time), calls to outside APIs, and
  the operator's SDKs (`std:sdk`, run by the runtime). Packages that need Node's server sockets or filesystem do not run.
- **WASM** runs efficient, high-load, stateless microservices: low per-request overhead, strong isolation and a
  small memory footprint.

### Goals

| Goal | What it means in practice |
|---|---|
| Run untrusted code inside the operator's infrastructure | Fresh isolate or instance per task, per-task limits, an SSRF guard on all outbound traffic, sealed secrets |
| High density and throughput per MB of memory | One process and one memory budget for all tenants, a Go-style scheduler, tiered caches; tunable with profiles |
| One binary, no required dependencies | Local disk storage, SQLite, in-process queues and schedules; configuration in one `rt.toml` |
| Storage built in | KV, SQL and blob storage scoped to each service |
| Extensible by the operator | SDKs the runtime runs for the operator, which tenants call by name and which update without redeploying tenants, and bindings that call the operator's platform with credentials guests never see |
| Predictable limits | Plans with quotas and limit maxima per tenant, limits per service |
| A debuggable, testable core | Scheduler traces, per-node and per-service stats, a deterministic scheduler simulation in the test suite |

### Sizing and performance

What a node needs and what a service costs is documented as a method, not as a leaderboard:
[Resource requirements](../guides/resources.md) explains how to size a node and how to measure the per-service cost
on your own hardware, and [Performance characteristics](../concepts/performance.md) explains what each phase of a
request costs.

### Built on

The runtime is written in Rust. It embeds V8 through `deno_core` (with `deno_ast` for TypeScript), runs WebAssembly
with wasmtime and the component model, stores data in SQLite, and serves HTTP with tokio, hyper and axum. The control
API is described by OpenAPI 3.1, which you can browse in the built-in Scalar UI.

## Editions

The runtime has an open core and paid editions. Paid modules live outside the core and plug into it through
extension points, so the open core is a complete runtime on its own.

| Capability | Open core | Paid |
|---|---|---|
| The runtime: Deno and WASM executors, triggers, storage, scheduler, control API, `21ci` CLI | :material-check: | :material-check: |
| Supervisor/executor process split and the sandboxed V8 build <span class="rt-badge rt-badge--planned">planned</span> | :material-check: | :material-check: |
| Executor sharding: several executor processes per node, tenant groups pinned to them | | :material-check: |
| KMS or Vault secrets backend | | :material-check: |
| Durable, exportable audit log | Events API and logs | :material-check: |
| SSO, OIDC and RBAC for the control API | | :material-check: |
| Web console and fleet management | | :material-check: |

The process split and the sandboxed V8 build are part of the open core by design but are not in this release; see
[Status and roadmap](status.md#not-in-this-release).

## License

The runtime is licensed under the **Apache License 2.0** (SPDX identifier `Apache-2.0`). You can use, modify and
redistribute it, including in commercial products, under the terms of that license.

- The license text is in [LICENSE](https://github.com/21-ci/runtime/blob/main/LICENSE); third-party notices are in
  [NOTICE](https://github.com/21-ci/runtime/blob/main/NOTICE).
- The paid capabilities above are separate modules; they are not part of the repository.

## More about the project

<div class="grid cards" markdown>

-   :material-map-marker-path:{ .lg .middle } **Status and roadmap**

    ---

    What works in this release, what is deferred, known limitations and the non-goals for v1.

    [:octicons-arrow-right-24: Status and roadmap](status.md)

-   :material-history:{ .lg .middle } **Changelog**

    ---

    What changed, by date: features, fixes, security and performance work.

    [:octicons-arrow-right-24: Changelog](changelog.md)

-   :material-frequently-asked-questions:{ .lg .middle } **FAQ**

    ---

    Short answers about behavior, limits, scaling, storage, operations and troubleshooting.

    [:octicons-arrow-right-24: FAQ](../faq.md)

</div>
