# Operator SDK and binding

How an **operator** (you, running the node) gives tenants an SDK for your own API, and changes it without touching their
services (and).

| File | Role |
|---|---|
| `acme-sdk/mod.ts` | the SDK: `defineSdk({ methods })` from `std:sdk/server`; it runs as a service of the system tenant |
| `rt.toml` | `[[binding]]`: what the SDK's `platform.fetch("acme", …)` may reach, with your credentials |
| `mock-platform-api/server.js` | stand-in for your platform API (Node, no dependencies) |

## How the pieces fit

```
tenant service                         runtime                              system tenant (sys)
import { acme } from "std:sdk"   ──>   grants: node → plan → tenant →  ──>  acme 1.1.0 (stable)   ── platform.fetch("acme") ──> your API
await acme.getPlatformInfo()           project → environment                ctx.caller = the tenant    credentials injected,
(a shim built from the contract,       pick the release, stamp the caller                              x-rt-tenant = the caller
 no SDK code)
```

- **Contract at build, bind at run time.** A tenant build sees only the contract (the method names and types); every call runs
  in the release the tenant's grants select. Publishing 1.1.0, rolling back to 1.0.0 or removing the SDK changes a row in the
  control plane: no tenant service is rebuilt or redeployed.
- **Credentials live in the binding.** `inject_headers = { authorization = "env:ACME_INTERNAL_TOKEN" }` reads the token from the
  node's environment; the value never reaches guest memory. Bindings are reachable only from SDK services: a tenant service gets
  `403 binding_not_allowed`.
- **The SDK authorizes by the caller.** `ctx.caller` (tenant, plan, project, environment, service, SDK release) is stamped by the
  runtime; nothing in the request can change it. `defineSdk` refuses a method that never reads it, and the binding forwards the
  *calling* tenant (`x-rt-tenant`), whose plan `allowed_plans` checks.
- **Grants decide who gets what.** `21ci sdk grants set node acme --allow` gives every tenant the `stable` channel, pinned to the
  stable major (`^1`): minors roll out on the channel, a new major is a grant change. A lower scope can only narrow: deny a tenant,
  move a staging environment to `canary`, pin a version.

## Try it locally

1. Start the mock API with the same token the node will inject:

   ```bash
   ACME_INTERNAL_TOKEN=dev-internal-token node sample/operator-sdk/mock-platform-api/server.js 3000
   ```

2. Configure the node: merge [`rt.toml`](rt.toml) into its config and start it with the same `ACME_INTERNAL_TOKEN` in its
   environment; make sure the `pro` / `business` plans exist (`[plans.pro]`). `SIGHUP` reloads plans and bindings.

3. Publish and grant the SDK (root token):

   ```bash
   21ci sdk publish acme 1.0.0 sample/operator-sdk/acme-sdk
   21ci sdk grants set node acme --allow
   21ci sdk ls
   ```

4. Deploy and call a tenant service that uses it ([`../deno/platform-sdk`](../deno/platform-sdk/README.md)):

   ```bash
   cd sample/deno/platform-sdk
   ../../scripts/deploy.sh . platform-sdk
   ../../scripts/invoke.sh platform-sdk /info
   ```

5. Update it without redeploying the tenant: change the SDK (e.g. `INFO_WITH_INTEGRATIONS = true`), then

   ```bash
   21ci sdk publish acme 1.1.0 sample/operator-sdk/acme-sdk --channel stable
   ../../scripts/invoke.sh platform-sdk /info     # "sdk": "1.1.0", with the integrations
   21ci sdk rollback acme                         # stable back to 1.0.0
   ```

## Writing your own SDK

- `export default defineSdk({ methods: { … } })`; nest objects for namespaces (`invoices.create`). Each method gets
  `(args, ctx)`: `args` is the caller's JSON argument (256 KiB at most), the result is JSON too.
- Authorize every method by `ctx.caller` (or give `authorize(caller, method, args)`), and key any data you keep by
  `ctx.caller.tenant`: the SDK's own storage is shared by every tenant that calls it.
- Reach your API only through `platform.fetch(<binding>, …)`; keep secrets in the binding or in the SDK service's secrets.
- Annotate parameter and return types: the published contract (`21ci sdk types acme`) is generated from them.

## See also

- `sample/deno/platform-sdk/`: a tenant service using this SDK
- `sample/apps/edge-gateway/`: a WASM gateway and a Deno service shipping audit events through it (`sdk.invoke`), and a smoke
  that updates the SDK under them
- `docs/api.md` (Operator SDKs), `docs/handlers.md`, `SECURITY.md`
