---
title: Self-hosted serverless runtime for TypeScript and WebAssembly
description: A self-hosted, multi-tenant serverless runtime for TypeScript (Deno isolates) and WebAssembly components, with built-in storage, queues, schedules and service calls.
hide:
  - navigation
  - toc
---

<div class="rt-hero" markdown>

# Serverless functions on your own servers

<p class="rt-hero__lead">
21 Cloud Functions Runtime is a self-hosted, multi-tenant runtime for TypeScript and WebAssembly.
Your tenants deploy code through one REST API or the <code>21ci</code> CLI, and every service gets an HTTP route,
schedules, a queue, KV, SQL and blob storage, secrets and calls to other services. One binary, one data volume, no
external dependencies.
</p>

[Get started](getting-started/index.md){ .md-button .md-button--primary }
[How it works](concepts/architecture.md){ .md-button }
[API reference](api.md){ .md-button }

</div>

## What it is for

The runtime lets an operator host other people's code inside their own infrastructure. You are the operator. Your
customers, your teams or their AI agents are **tenants**. A tenant writes a handler, uploads it and gets a URL; the
code runs next to your data under the limits and quotas you define, without a container or a virtual machine per
function.

That trade is the core design decision. Code runs as a V8 isolate or a WebAssembly component inside one long-lived
process, so a request does not boot an operating system or a container. In exchange the runtime only runs code that
fits those two sandboxes: web-standard JavaScript and WASI 0.2 components, not arbitrary Linux binaries.
[Isolation compared](concepts/isolation-compared.md) lays out what you give up and what you gain against containers,
microVMs and VPSes, and [How it works](concepts/architecture.md) walks through the machinery.

## Two runtimes, one API

<div class="grid cards" markdown>

-   :simple-deno:{ .lg .middle } **Deno: TypeScript and JavaScript**

    ---

    For quick services, often written by an AI, that glue outside APIs together. Standard Web APIs, `npm:` and `jsr:`
    packages resolved at build time, and platform modules such as `std:kv` and `std:sql`. Each request runs in a fresh
    V8 isolate by default.

    [:octicons-arrow-right-24: Write a Deno service](guides/deno.md)

-   :simple-webassembly:{ .lg .middle } **WASM: WebAssembly components**

    ---

    For low-latency, high-load, stateless microservices that need strong isolation and a small memory footprint.
    Upload a WASI Preview 2 component (Rust, JavaScript through `jco componentize`, Go with TinyGo, Python, C) that
    exports `wasi:http/incoming-handler`.

    [:octicons-arrow-right-24: Write a WASM service](guides/wasm.md)

</div>

=== "Deno"

    ```ts title="main.ts"
    export default async function (req: Request): Promise<Response> {
      const name = new URL(req.url).searchParams.get("name") ?? "World";
      return Response.json({ message: `Hello, ${name}!` });
    }
    ```

    ```console
    $ 21ci push main.ts --service hello --create --wait
    $ curl 'http://127.0.0.1:8080/s/hello/?name=Ada'
    {"message":"Hello, Ada!"}
    ```

=== "WASM (Rust)"

    ```console
    $ 21ci init hello-rs --template wasm-rust && cd hello-rs
    $ ./build.sh
    $ 21ci push target/wasm32-wasip2/release/hello_rs_wasm.wasm --service hello-rs --create --wait
    $ curl 'http://127.0.0.1:8080/s/hello-rs/?name=Ferris'
    ```

| | Deno | WASM |
|---|---|---|
| You write | TypeScript or JavaScript | Rust, JavaScript, Go (TinyGo), Python, C: anything that builds a WASI P2 component |
| Best for | Webhooks, bots, API glue, AI-written services | Hot paths, gateways, stateless high-load microservices |
| Dependencies | `npm:`, `jsr:`, allowlisted `https:` imports, bundled at build time | Whatever you compile into the component |
| Platform APIs | `std:kv` `std:sql` `std:blob` `std:queue` `std:services` `std:sdk` (operator SDKs) | `std:host` WIT interfaces: log, kv, sql, blob, queue, call, sdk |
| Isolation per request | Fresh V8 isolate (warm reuse is opt-in) | Fresh component instance (reuse is opt-in) |

Not sure which one to pick? Read [Languages and frameworks](languages/index.md) and
[Deno or WASM?](faq.md#deno-or-wasm-which-should-i-use).

## What you get

<div class="grid cards" markdown>

-   :material-account-group:{ .lg .middle } **Multi-tenant by design**

    ---

    Tenants, scoped API keys, plans with quotas and limit maxima. A tenant cannot see or call another tenant's
    services unless you allow it. The [guarantees and non-guarantees](concepts/multi-tenancy.md) are spelled out.

    [:octicons-arrow-right-24: Tenants and plans](concepts/tenants-and-plans.md)

-   :material-lightning-bolt:{ .lg .middle } **Triggers built in**

    ---

    HTTP routes and custom domains, interval and cron schedules, a durable queue with retries and a dead-letter
    queue, and in-process service-to-service calls.

    [:octicons-arrow-right-24: Triggers](concepts/triggers.md)

-   :material-database:{ .lg .middle } **Storage per service**

    ---

    Key/value, SQLite and streamed blobs scoped to each service, plus environment variables and AES-256-GCM sealed
    secrets. No database to run next to it.

    [:octicons-arrow-right-24: KV, SQL and blobs](guides/sub-services.md)

-   :material-timer-sand:{ .lg .middle } **Queues instead of 429s**

    ---

    A Go-style scheduler runs tasks across your cores. When a service is busy, requests wait for a slot instead of
    being rejected, and per-service concurrency adapts to load.

    [:octicons-arrow-right-24: Scheduler and autoscaling](concepts/scheduler.md)

-   :material-shield-lock:{ .lg .middle } **Guarded by default**

    ---

    Per-task CPU, memory and wall-clock limits, an SSRF guard on every outbound request, write-only secrets and
    hashed API keys.

    [:octicons-arrow-right-24: Security model](concepts/security.md)

-   :material-puzzle:{ .lg .middle } **Extensible by the provider**

    ---

    Publish SDKs the runtime runs for you, which tenants call by name and which you update or roll back without
    redeploying them, and bindings that call your platform with credentials the guest code never sees.

    [:octicons-arrow-right-24: Operator SDKs and bindings](guides/operator-sdks.md)

-   :material-chart-line:{ .lg .middle } **Observable**

    ---

    Live log streaming over SSE, per-service stats with latency percentiles, deploy events, Prometheus metrics and
    `server-timing` on every response.

    [:octicons-arrow-right-24: Observability](concepts/observability.md)

-   :material-tune:{ .lg .middle } **Tunable profiles**

    ---

    `max_density`, `balanced` or `min_latency`: one switch changes time slices, warm pools, eager builds and memory
    watermarks. Every knob is also an `rt.toml` key.

    [:octicons-arrow-right-24: Performance tuning](guides/performance.md)

-   :material-server:{ .lg .middle } **You run it**

    ---

    One container or one systemd unit on your own hardware. Requirements, backups, upgrades, monitoring and key
    rotation are documented for single servers and for company deployments.

    [:octicons-arrow-right-24: Self-hosting](guides/self-hosting.md)

</div>

## What it is not (yet)

The runtime is a single node: there is no clustering or high availability in this release, no built-in TLS (put a
reverse proxy in front), and no web console. The V8 sandbox is not available yet and there is no seccomp filter, so
the per-task isolate is the main isolation boundary. Guests cannot run native binaries, open raw sockets or use
`node:` built-ins; WebSockets are held by the node, not by guests. [Status and roadmap](about/status.md) lists everything that is deferred.

## Where to go next

<div class="grid cards" markdown>

-   :material-rocket-launch:{ .lg .middle } **Get started**

    ---

    Build and start a node with Docker, create a tenant and deploy your first service.

    [:octicons-arrow-right-24: Get started](getting-started/index.md)

-   :material-school:{ .lg .middle } **Learn how it works**

    ---

    How a request travels through the node, how isolates and components are contained, what happens when
    something fails, and what costs what.

    [:octicons-arrow-right-24: Concepts](concepts/index.md)

-   :material-wrench:{ .lg .middle } **Do a task**

    ---

    Get a tenant key, deploy, roll back, configure, read logs, use KV and SQL, tune a service.

    [:octicons-arrow-right-24: Guides](guides/index.md)

-   :material-code-braces:{ .lg .middle } **Pick a language**

    ---

    Which frameworks and languages work on each runtime, with working skeletons and honest limitations.

    [:octicons-arrow-right-24: Languages and frameworks](languages/index.md)

-   :material-book-open-variant:{ .lg .middle } **Follow a tutorial**

    ---

    Five complete sample apps: a Telegram bot, event sign-ups, a live queue, currency rates and a WASM edge gateway.

    [:octicons-arrow-right-24: Tutorials](tutorials/index.md)

-   :material-api:{ .lg .middle } **Look something up**

    ---

    Control API, CLI, every configuration key, error reasons, limits and metrics.

    [:octicons-arrow-right-24: Reference](reference/index.md)

</div>
