---
title: Guides
description: Task-oriented guides for using a node, building services, picking languages and frameworks, running your own node and building on top of it as a provider.
---

# Guides

Guides show how to get one job done end to end: what the runtime does under the hood, then the commands and code you
need. They assume you have a node and a tenant key; if not, start with [Get started](../getting-started/index.md) or
[Self-hosting](self-hosting.md).

## Using a node

For developers (tenants) who deploy and look after services.

<div class="grid cards" markdown>

-   :material-key:{ .lg .middle } **Get access**

    ---

    How the operator creates a tenant and a key, how you configure `21ci`, scopes, your own tokens, expiry and rotation.

    [:octicons-arrow-right-24: Get access](access.md)

-   :material-folder-multiple:{ .lg .middle } **Projects and environments**

    ---

    Group services into projects, give them `production`, `staging` or preview environments, and deploy into them.

    [:octicons-arrow-right-24: Projects and environments](projects.md)

-   :material-rocket-launch:{ .lg .middle } **Deploy and roll back**

    ---

    Create a service, deploy a file, a directory or a component, activate, roll back, rebuild and delete.

    [:octicons-arrow-right-24: Deploy and roll back](deploy.md)

-   :material-tune:{ .lg .middle } **Configure a service**

    ---

    Limits, access, environments and allowed callers, env vars and secrets, scaling and what takes effect when.

    [:octicons-arrow-right-24: Configure a service](configure.md)

-   :material-text-search:{ .lg .middle } **Logs, stats and usage**

    ---

    Tail and follow logs, read build logs, stats, events and usage, and debug a request by its task id.

    [:octicons-arrow-right-24: Logs, stats and usage](logs-and-usage.md)

-   :material-robot:{ .lg .middle } **AI assistants (MCP)**

    ---

    Connect Claude Code, Cursor or Claude Desktop to a node and let them deploy and debug with a token made for them.

    [:octicons-arrow-right-24: AI assistants (MCP)](../mcp.md)

</div>

## Building services

<div class="grid cards" markdown>

-   :material-language-typescript:{ .lg .middle } **Deno services**

    ---

    TypeScript and JavaScript handlers: Form A and Form B, request and response, env and secrets, outbound `fetch`.

    [:octicons-arrow-right-24: Deno services](deno.md)

-   :material-cube-outline:{ .lg .middle } **WASM services**

    ---

    WASI P2 components exporting `wasi:http/incoming-handler`: the contract, linked interfaces, lifecycle and limits.

    [:octicons-arrow-right-24: WASM services](wasm.md)

-   :material-database:{ .lg .middle } **KV, SQL, blobs and other sub-services**

    ---

    Every built-in sub-service with a Deno and a Rust example: KV with `incr` and TTL, SQL, blobs, queues, schedules,
    calls and bindings.

    [:octicons-arrow-right-24: Sub-services](sub-services.md)

-   :material-swap-horizontal:{ .lg .middle } **Service calls and queues**

    ---

    Call another service in-process, control who may call whom, and hand work to a durable queue.

    [:octicons-arrow-right-24: Calls and queues](calls-and-queues.md)

-   :material-clock-outline:{ .lg .middle } **Schedules**

    ---

    Interval and cron schedules, and exactly what happens when a run is slow or the node was down.

    [:octicons-arrow-right-24: Schedules](schedules.md)

-   :material-lan-connect:{ .lg .middle } **WebSockets and durable objects**

    ---

    Accept WebSockets, keep per-room state in durable objects with storage and alarms, and move over from
    Cloudflare Durable Objects.

    [:octicons-arrow-right-24: WebSockets and durable objects](websockets.md)

-   :material-web:{ .lg .middle } **Custom domains**

    ---

    Path routes, wildcard subdomains and your own host names, with DNS TXT verification.

    [:octicons-arrow-right-24: Custom domains](custom-domains.md)

-   :material-speedometer:{ .lg .middle } **Optimize your service**

    ---

    Cold versus warm, reuse-safe code, cheap top levels, small bundles, batching, streaming and limits.

    [:octicons-arrow-right-24: Optimize your service](optimize.md)

</div>

## Languages and frameworks

Which languages and frameworks work on each runtime, with requirements, limitations and working skeletons:
[overview](../languages/index.md), [Deno](../languages/deno.md), [WASM](../languages/wasm.md).

## Running a node

For operators who self-host the runtime.

<div class="grid cards" markdown>

-   :material-server:{ .lg .middle } **Self-hosting**

    ---

    One Linux machine: Docker Compose, a systemd unit, or a small VPS with automatic TLS.

    [:octicons-arrow-right-24: Self-hosting](self-hosting.md)

-   :material-server-network:{ .lg .middle } **Production deployment**

    ---

    A company runbook: proxy and TLS, backups, monitoring, upgrades, key rotation, capacity and incidents.

    [:octicons-arrow-right-24: Production](production.md)

-   :material-memory:{ .lg .middle } **Resource requirements**

    ---

    CPU, RAM and disk for a node, what each service and sub-runtime costs, and how to measure it.

    [:octicons-arrow-right-24: Resource requirements](resources.md)

-   :material-gauge:{ .lg .middle } **Performance tuning**

    ---

    Profiles, warm pools, caching and scheduler settings.

    [:octicons-arrow-right-24: Performance tuning](performance.md)

</div>

## For providers

Companies that build a product on top of a node can give tenants an SDK that the runtime runs for them, with controlled
access to their own API and without handing out credentials: [Operator SDKs and bindings](operator-sdks.md). Tenants:
[Use operator SDKs](use-sdks.md).

## Which guide do I need?

| You want to… | Read |
|---|---|
| Get a key for a node and set up the CLI | [Get access](access.md) |
| Give CI or a colleague a narrower token | [Get access](access.md#create-your-own-tokens) |
| Keep apps, staging and production apart | [Projects and environments](projects.md) |
| Let an AI assistant deploy and debug | [AI assistants (MCP)](../mcp.md) |
| Deploy, update or roll back a service | [Deploy and roll back](deploy.md) |
| Change limits, env vars, secrets or who may call a service | [Configure a service](configure.md) |
| See what a service is doing | [Logs, stats and usage](logs-and-usage.md) |
| Deploy a TypeScript service that calls an outside API | [Deno services](deno.md) |
| Serve a hot, stateless endpoint with low overhead | [WASM services](wasm.md) |
| Store data | [Sub-services](sub-services.md) |
| Use Hono, drizzle, Go, Python or C | [Languages and frameworks](../languages/index.md) |
| Make a service cheaper or faster | [Optimize your service](optimize.md) |
| Split an app into services that talk to each other | [Service calls and queues](calls-and-queues.md#service-to-service-calls) |
| Process work in the background, with retries | [Service calls and queues](calls-and-queues.md#queues) |
| Run a job every hour, or at 09:00 on weekdays | [Schedules](schedules.md) |
| Build a chat, a live dashboard or a multiplayer room | [WebSockets and durable objects](websockets.md) |
| Move a Cloudflare Durable Object | [WebSockets and durable objects](websockets.md#migrating-from-cloudflare) |
| Serve a service at `api.example.com` | [Custom domains](custom-domains.md) |
| Run a node on one machine | [Self-hosting](self-hosting.md) |
| Run a node for a company | [Production deployment](production.md) |
| Size a node | [Resource requirements](resources.md) |
| Let tenants use your platform API safely | [Operator SDKs and bindings](operator-sdks.md) |

## Learn by building

The [tutorials](../tutorials/index.md) build complete apps from the runtime's `sample/apps/` directory: a Telegram
bot, an event sign-up service, a live queue, a currency-rate poller and a WASM edge gateway. They use the features
these guides explain, in context.

For background on how things work, see [Concepts](../concepts/index.md); for exact fields, limits and error codes, see
the [Reference](../reference/index.md).
