---
title: Reference
description: Look-up material for the control API, the 21ci CLI, rt.toml, error reasons, limits, metrics and the guest APIs.
---

# Reference

Exact, complete look-up pages. The control API, CLI, configuration and guest code pages are the guides in the
repository's `docs/` folder, so the site and the source tree describe the same release.

## APIs

<div class="grid cards" markdown>

-   :material-api:{ .lg .middle } **Control and invoke API**

    ---

    Authentication, scopes, conventions, errors, `Retry-After`, pagination, idempotency and every route.

    [:octicons-arrow-right-24: API overview](../api.md)

-   :material-application-brackets:{ .lg .middle } **API explorer**

    ---

    Interactive OpenAPI 3.1 reference of all control operations and schemas.

    [:octicons-arrow-right-24: API explorer](api-explorer.md)

-   :material-language-typescript:{ .lg .middle } **Guest API (`std:*`)**

    ---

    Handler forms, `ctx`, and every function of `std:kv`, `std:sql`, `std:blob`, `std:queue`, `std:services`,
    `std:platform`.

    [:octicons-arrow-right-24: Guest API](guest-api.md)

-   :simple-webassembly:{ .lg .middle } **WIT (`std:host`)**

    ---

    The WebAssembly interfaces a component imports for logs, storage, queues, calls and bindings.

    [:octicons-arrow-right-24: WIT reference](wit.md)

</div>

## Tools and configuration

<div class="grid cards" markdown>

-   :material-console:{ .lg .middle } **CLI (`21ci`)**

    ---

    Every command with example output, contexts, CI usage and exit codes.

    [:octicons-arrow-right-24: CLI reference](../cli.md)

-   :material-file-cog:{ .lg .middle } **Configuration (`rt.toml`)**

    ---

    Every key with type, default, profile defaults and its `RT__SECTION__KEY` override.

    [:octicons-arrow-right-24: Configuration reference](../config.md)

</div>

## Behavior

<div class="grid cards" markdown>

-   :material-alert-circle-outline:{ .lg .middle } **Error reasons**

    ---

    Control API `error.code` values, invoke `x-rt-reason` values and SDK error codes.

    [:octicons-arrow-right-24: Error reasons](./errors.md)

-   :material-gauge:{ .lg .middle } **Limits and quotas**

    ---

    Per-task limits, plan quotas, storage, body and ingress limits, and who can change them.

    [:octicons-arrow-right-24: Limits and quotas](limits.md)

-   :material-chart-box-outline:{ .lg .middle } **Metrics**

    ---

    Every Prometheus series the node exports, plus health endpoints and example queries.

    [:octicons-arrow-right-24: Metrics](metrics.md)

</div>
