---
title: Use operator SDKs
description: Call the SDKs your node's operator publishes from Deno and WASM services, get their TypeScript types, handle their errors, and see which release your grants select; plus typed RPC proxies for your own services.
---

# Use operator SDKs

The operator of your node can publish **SDKs**: their own code for things like billing, accounts or AI features. An SDK
runs in the runtime, not in your service. Your service imports its **contract** (names and types) and calls it; the
operator can publish a new release, roll it back or remove it, and your service sees the change on its next call,
without a rebuild or a redeploy.

## What you can use

```console
$ 21ci sdk ls
NAME  VERSION  CHANNEL  REQUIRES  MEMBERS                                         REASON
acme  1.1.0    stable   ^1        getPlatformInfo,listIntegrations,callService,audit  -
```

`21ci sdk ls` (`GET /v1/sdks/effective`, MCP `list_sdks`) lists the SDKs your grants allow and the release a call runs
now. Grants can differ per project and environment: `21ci sdk ls --project shop --environment staging`. An SDK that is
not granted to you does not appear at all.

## Call an SDK (Deno)

```ts
import { acme, SdkError } from "std:sdk";

export default async function (req: Request): Promise<Response> {
  try {
    const info = await acme.getPlatformInfo();
    const receipt = await acme.audit.ship({ source: "web", events: [] });   // nested methods: dotted names
    return Response.json({ info, receipt });
  } catch (e) {
    if (e instanceof SdkError) return Response.json({ error: e.code }, { status: 502 });
    throw e;
  }
}
```

Three ways to import, all the same SDK:

| Import | Use |
|---|---|
| `import { acme } from "std:sdk"` | One named export per SDK published on the node |
| `import acme, { getPlatformInfo } from "sdk:acme"` | The SDK as the default export, plus its top-level methods and namespaces |
| `import { sdk } from "std:sdk"` then `sdk("acme")` | A name chosen at run time (from an env var, for example) |

How it works:

- **No SDK code in your service.** At build time, `std:sdk` and `sdk:acme` become small generated modules over the
  runtime's `sdk(name)` proxy. Importing an SDK that is not published on the node fails the deploy (`422`).
- **Each call is a call.** `await acme.invoices.create(arg)` sends `arg` as JSON (at most 256 KiB) and returns the JSON
  result. It counts as a service call: your remaining time and the call depth limit (15) apply.
- **The release is chosen per call** by your grants: the operator's `stable` channel by default, perhaps `canary` in a
  staging environment, pinned to a major version. You do not pick versions in code.
- **Your identity is stamped by the runtime.** The SDK sees your tenant, project, environment and service as the runtime
  knows them; nothing your code sends can change that.

## Errors

Every failure throws `SdkError` (it is also a `ServiceError`), with `code` and `message`:

| `code` | Meaning |
|---|---|
| `sdk_not_found` | No SDK of that name on the node |
| `sdk_not_granted` | Your grants do not allow it (here) |
| `sdk_unavailable` | The operator removed it |
| `sdk_version_unavailable` | No release satisfies your grants |
| `method_not_found` | The release has no such method |
| `validation_failed` | The argument is not JSON or is larger than 256 KiB |
| `call_depth`, `timeout` | The call limits |
| anything else | The SDK's own code (for example `out_of_stock`) |

## Types for your editor

```bash
21ci sdk types acme -d types        # writes types/acme.d.ts
```

The file declares `module "sdk:acme"` (with the SDK's interfaces and an `Acme` interface of its methods) and the
`std:sdk` export `acme`. Include it in your `tsconfig.json` (or `deno.json` `compilerOptions`) and both import forms are
typed:

```json title="tsconfig.json (excerpt)"
{ "include": ["src", "types"] }
```

Download it again after the operator publishes a release with new methods. The types are for your editor and `tsc`
only: they never change what your service runs.

## Call an SDK from WASM

`std:host/sdk` has one function:

```rust
use bindings::std::host::sdk;

match sdk::invoke("acme", "getPlatformInfo", "null") {
    Ok(json) => { /* the JSON text of the result */ }
    Err(e) => { /* e.code: sdk_not_granted, sdk_unavailable, … or the SDK's own */ }
}
```

The `args-json` argument and the result are JSON text. See the [WIT reference](../reference/wit.md) and the
[edge gateway tutorial](../tutorials/edge-gateway.md), which ships audit events through an SDK from a Rust component.

## Typed calls to your own services

The same contract mechanism works for your own RPC services ([Service calls](calls-and-queues.md)):

```bash
21ci types billing -d types       # types/billing.d.ts: an interface BillingRpc from billing's `rpc` export
```

```ts
import { services } from "std:services";
import type { BillingRpc } from "../types/billing.d.ts";

const billing = services.bind("billing") as unknown as BillingRpc;
const invoice = await billing.createInvoice({ amount: 100 });
```

## See also

- [Operator SDKs and bindings](operator-sdks.md): the operator's side (publishing, grants, bindings).
- [Guest API](../reference/guest-api.md#operator-sdks): `std:sdk`.
- [CLI reference](../cli.md#sdk): `21ci sdk ls`, `21ci sdk types`, `21ci types`.
