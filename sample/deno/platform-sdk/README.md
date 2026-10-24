# platform-sdk

A tenant service that uses the **operator SDK** `acme`: a runtime service the operator publishes and grants
.

## Shows

- `import { acme } from "std:sdk"` and `import { listIntegrations } from "sdk:acme"`: at build time both become shims
  generated from the SDK's published contract. No SDK code is in this service.
- Each call (`await acme.getPlatformInfo()`) runs in the runtime, in the SDK release this service's grants select. A new
  release, a rollback or a removal reaches the service on its next call, without a rebuild or a redeploy.
- Failures are typed: `SdkError` with `code` `sdk_not_granted`, `sdk_unavailable` (removed), `sdk_not_found`, or the SDK's own
  code.
- `platform.fetch` from a tenant service is refused (`403 binding_not_allowed`): operator bindings belong to SDK services, which
  forward the calling tenant.

## Setup (operator side)

See [`../../operator-sdk/`](../../operator-sdk/README.md): the `[[binding]] acme` block in the node's `rt.toml`, the mock API,
then publish and grant the SDK:

```bash
21ci sdk publish acme 1.0.0 sample/operator-sdk/acme-sdk     # root token
21ci sdk grants set node acme --allow                        # pinned to ^1
```

## Types for your editor

```bash
21ci sdk types acme -d types     # types/acme.d.ts: declare module "sdk:acme" and the std:sdk export
21ci sdk ls                      # the SDKs your grants allow, and the release each runs
```

## Deploy and invoke

```bash
cd sample/deno/platform-sdk
../../scripts/deploy.sh . platform-sdk
S=../../scripts/invoke.sh
$S platform-sdk /info
$S platform-sdk /integrations
$S platform-sdk /deploy POST
$S platform-sdk /raw
```

## Expected responses

```json
// /info
{ "platform": "acme", "version": "1.0.0", "features": ["storage", "api", "webhooks"], "tenant": "ten_01J…", "sdk": "1.0.0" }

// /integrations
{ "integrations": ["slack", "github", "stripe", "sendgrid"] }

// /deploy
{ "service": "my-integration", "action": "executed", "input": { "action": "deploy" }, "tenant": "ten_01J…" }

// /raw  (platform.fetch from a tenant service)
{ "status": 403, "reason": "binding_not_allowed" }
```

`tenant` is the calling tenant: the SDK's binding forwards it in the `x-rt-tenant` header the runtime sets. `sdk` is the release
that answered; after `21ci sdk publish acme 1.1.0 … --channel stable` the next `/info` says `1.1.0`.
