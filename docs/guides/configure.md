---
title: Configure a service
description: Change a deployed service without a new version - limits, access, ingress rules, env variables, secrets, the environment label, allowed callers and scaling knobs - and see which changes apply at once.
---

# Configure a service

A service is two things: **code**, which arrives as immutable versions ([Deploy and roll back](deploy.md)), and
**settings**, which you change in place. This guide covers the settings. Almost all of them apply without a deploy and
survive new versions and rollbacks.

You need a key with `services:write` to change config blocks, `env:write` for env variables and `secrets:write` for
secrets ([Get access](access.md)). Examples use `$RT_API`, `$RT_KEY` and a service `hello` (id in `$SVC`).

## What needs a new version and what does not

| You change | How | Takes effect |
|---|---|---|
| Code, `rt.json`, dependency specifiers | New version: `21ci push` | When the version is built and live |
| `limits`, `scaling`, `triggers`, `ingress`, `egress`, `call`, `deno`, `wasm`, `access`, `environment`, `profile`, `name` | `PATCH /v1/services/{s}` or `21ci service update` | Before the API answers; tasks that start afterwards use it |
| Env variables | `PUT …/env` or `21ci env set` | The next task |
| Secrets | `PUT …/secrets/{NAME}` or `21ci secret set` | The next task |
| Custom hosts, schedules | `21ci host …`, `21ci schedule …` | At once (a tenant's host claim is active once DNS verifies it) |
| Slug and runtime | Cannot be changed | Create a new service |
| The plan's quotas and maxima | The operator | On the next check; the plan wins over a service's own value |

The reason config applies immediately: every control change refreshes the node's routing table and service table
before the response is sent, so a request made right after `PATCH` returns already sees it. Tasks that are already running
keep what they started with.

!!! warning "Env and secrets are not versioned"
    A rollback moves the code pointer only. The service keeps its current env variables, secrets, limits and other
    settings. If version 7 needs a new variable, set it before activating 7, and remember that version 6 will see it
    too after a rollback.

## Read the current configuration

=== "21ci"

    ```console
    $ 21ci service get hello
    ID:               svc_01M3XM1AMYAKX6EREP6J536ATG
    Slug:             hello
    Runtime:          deno
    Name:             hello
    Profile:          -
    Project:          default
    Environment:      production
    Allowed callers:  (services of the same project in the same environment)
    Live:             2
    Access:           public
    Reuse isolates:   default (node config, then profile)
    Limits:           cpu_ms=500
    Scaling:          max_instances=16
    Instances:        min 0 (default), max 16 (service)
    Triggers:         off: cron
    Build:            ready
    Tier:             T1
    ETag:             "4"
    …
    ```

    `Limits`, `Scaling`, `Ingress` and `Egress` list only the fields you set. `--json` prints the whole service,
    including the resolved values.

=== "curl"

    ```bash
    curl -sS -H "Authorization: Bearer $RT_KEY" "$RT_API/v1/services/$SVC" | jq '{etag, access, limits, scaling, triggers, ingress, egress, call}'
    ```

`effective` in the JSON shows every resolved value and where it came from:

```bash
21ci service get hello --json | jq '.effective | {cpu_ms, wall_ms, memory_mb, compile}'
```

```json
{
  "cpu_ms":    { "value": 500,   "source": "service" },
  "wall_ms":   { "value": 30000, "source": "default" },
  "memory_mb": { "value": 128,   "source": "default" },
  "compile":   { "value": "lazy", "source": "profile:balanced" }
}
```

`source` is `service` (you set it), `config` (the node's `rt.toml`), `profile:<name>`, `plan` (the plan capped it) or
`default`. When a service behaves differently from what you set, read this first.

## Change a block: `PATCH` replaces the whole block

The config is a set of **blocks**: `limits`, `scaling`, `triggers`, `ingress`, `egress`, `call`, `deno`, `wasm`.
`PATCH` replaces each block you send in full and leaves the blocks you omit alone. A field missing from a block you sent
goes back to its default.

=== "21ci"

    ```bash
    21ci service update hello --set limits.cpu_ms=500 --set triggers.cron=false
    21ci service update hello --unset limits.cpu_ms
    21ci service update hello --set 'ingress.allowed_methods=["GET","POST"]' --set ingress.rate_limit.rps=20
    ```

    `--set BLOCK.KEY=VALUE` changes one field and `--unset BLOCK.KEY` returns it to its default. The CLI reads the service,
    merges your fields into each block it touches and sends the result with `If-Match`, so it never drops the other
    fields, and a concurrent change fails with `412 precondition_failed`. Values are JSON when they parse as JSON
    (`500`, `false`, `["GET"]`), otherwise strings. Unknown blocks are refused locally and unknown fields come back as
    `422 validation_failed`.

=== "curl"

    ```bash
    # read, change one field, write the whole block back
    CUR=$(curl -sS -H "Authorization: Bearer $RT_KEY" "$RT_API/v1/services/$SVC")
    ETAG=$(jq -r .etag <<<"$CUR")
    LIMITS=$(jq -c '.limits + {cpu_ms: 500}' <<<"$CUR")
    curl -sS -X PATCH -H "Authorization: Bearer $RT_KEY" -H 'Content-Type: application/json' -H "If-Match: $ETAG" \
      -d "{\"limits\": $LIMITS}" "$RT_API/v1/services/$SVC"
    ```

!!! danger "The trap"
    If `limits` is `{"cpu_ms": 500, "memory_mb": 64}` and you send `{"limits": {"wall_ms": 10000}}`, the result is
    `{"wall_ms": 10000}`: the CPU and memory limits are gone. This is deliberate, so a block is always exactly what you
    sent, but it means `curl` users must send the whole block. `21ci service update --set` does the merge for you.

`If-Match` is optional but worth sending in scripts: two people editing the same service otherwise overwrite each other.

## Limits and plan maxima

The `limits` block holds four per-task limits:

| Field | Meaning | Default for an HTTP task | When exceeded |
|---|---|---|---|
| `wall_ms` | Wall-clock time from start to finish | 30,000 | `504 timeout` |
| `cpu_ms` | CPU time the task uses; waiting for network or storage does not count | 1,000 | `502 cpu_limit` |
| `memory_mb` | Heap (Deno) or linear memory (WASM) | 128 | `502 oom` |
| `max_body_bytes` | Request body size | 10 MB | `413 too_large` |

An explicit `wall_ms` or `cpu_ms` applies to **every** trigger of the service. Without one, schedules and queue
batches keep their own, larger defaults (300 s and 10 s), so setting `cpu_ms=200` for the HTTP path also caps your cron
jobs. The plan's `limits_max` caps everything:

```console
$ 21ci service update hello --set limits.cpu_ms=5000
error: limit_exceeds_plan: `limits.cpu_ms` 5000 exceeds the plan maximum 1000 (HTTP 422)
  {
    "field": "cpu_ms",
    "requested": 5000,
    "max": 1000
  }
```

Values are refused, never clamped silently. If the operator later lowers the plan below a value you set, the plan wins at
runtime and `effective` shows the source `plan`. Lower limits are also a safety net: a runaway loop ends at `cpu_ms`
instead of occupying a processor. Pick them from [Logs, stats and usage](logs-and-usage.md) (the `exec` p99) and see
[Optimize a service](optimize.md).

## HTTP access and ingress

### Access modes

`access` decides who may call the service over HTTP:

| `access` | Public listener | Internal listener |
|---|---|---|
| `public` (default) | Anyone | Yes |
| `key` | A tenant key of the same tenant with `invoke` as `Authorization: Bearer` or `x-rt-key`, or the root token as `Authorization: Bearer`; otherwise `401` | Yes, no key needed |
| `internal` | `404 not_found`, as if it did not exist | Yes |

`access` governs HTTP only. Calls between services have their own rule ([below](#environments-and-allowed-callers)),
and schedules and queue messages never go through a listener.

```bash
21ci service update reports --access key
21ci key create --tenant ten_01M3XM135SCP9EP41ZZXAQ5CCZ --name reports-client --scopes invoke --project prj_01M4Q2GB5APYBGEPYPBX1X4ZYJ
21ci invoke reports /daily --key "$INVOKE_KEY"          # or RT_INVOKE_KEY
curl -H "Authorization: Bearer $INVOKE_KEY" "$RT_INVOKE/s/reports/daily"
```

The invoke key is removed before your handler runs, so your own `Authorization` header is unaffected; if a client must
send both, it puts the invoke key in `x-rt-key`. Ten wrong keys per minute from one client end in `429`.

### Ingress rules

On the public listener, after the access check, the `ingress` block narrows traffic further:

```bash
21ci service update hook \
  --set 'ingress.allowed_methods=["POST"]' \
  --set 'ingress.allow_paths=["/webhook","/api/*"]' \
  --set 'ingress.ip_allow=["203.0.113.0/24"]' \
  --set ingress.rate_limit.rps=20 --set ingress.rate_limit.burst=40
```

| Rule | Refusal |
|---|---|
| `allowed_methods` | `405 method_not_allowed` |
| `allow_paths` (exact, or a prefix with a trailing `*`; matched against the path your handler sees) | `403 forbidden` |
| `ip_allow` (IPv4 and IPv6 CIDRs) | `403 forbidden` |
| `rate_limit` (`rps`, `burst`; a per-service token bucket) | `429 rate_limited` with `Retry-After` |

Empty lists and `rps: 0` mean no rule. `ip_allow` uses the client address the node resolved; behind a reverse proxy
that is correct only when the operator listed the proxy in `ingress.trusted_proxies`. The internal listener skips ingress
rules. See [Triggers](../concepts/triggers.md).

### Trigger switches

`triggers` has booleans `http`, `interval`, `cron`, `queue` and `call`, all on. Turning one off stops that kind of task
for this service (`http` off makes the service `404` over HTTP). The tenant's plan must also allow a trigger
(`allowed_triggers`; `cron` is off on `free`), and both must say yes, else `403 trigger_not_allowed`.

```bash
21ci service update hello --set triggers.cron=false --set triggers.queue=false
```

### Outbound requests

`egress.allowed_hosts` (non-empty means allow-list) and `egress.denied_hosts` shape what the service's `fetch` may reach.
Entries match the host exactly or as a dot-suffix (`telegram.org` also covers `api.telegram.org`). The node's own guard
(private networks, SMTP ports) and the plan's `egress_default` apply on top. `allowed_hosts` needs a JSON array:

```bash
21ci service update bot --set 'egress.allowed_hosts=["api.telegram.org"]'
```

## Env variables and secrets

Both are per-service key/value pairs that your code reads the same way. They differ in who can read them back.

| | Env variables | Secrets |
|---|---|---|
| Scope to write | `env:write` | `secrets:write` |
| Readable through the API | Yes (`21ci env get`, needs `services:read`) | Never. `list` shows names and update times |
| Stored | As plain text in the node's database | Sealed with AES-256-GCM, bound to tenant, service and name |
| Write semantics | `PUT` **replaces the whole map** | One secret per call |
| For | Settings: feature flags, URLs, log level | API tokens, passwords, signing keys |

Names match `[A-Z_][A-Z0-9_]*` and must not start with `RT_`. At most 100 variables and 100 secrets per service, values up
to 32 KB.

=== "21ci"

    ```bash
    21ci env set hello GREETING=Hi LOG_LEVEL=debug       # reads the map, merges, writes it back
    21ci env get hello
    21ci env unset hello LOG_LEVEL

    printf %s "$STRIPE_KEY" | 21ci secret set hello STRIPE_KEY      # value from stdin, never from argv
    21ci secret set hello TLS_CERT --from-file cert.pem
    21ci secret list hello
    21ci secret delete hello STRIPE_KEY
    ```

    `env set` and `env unset` read the map and write it back, which is not atomic: two people editing at the same time can
    overwrite each other. From stdin, one trailing newline is dropped; `--from-file` sends the file verbatim.

=== "curl"

    ```bash
    # env: the PUT body is the complete map
    curl -sS -X PUT -H "Authorization: Bearer $RT_KEY" -H 'Content-Type: application/json' \
      -d '{"GREETING":"Hi","LOG_LEVEL":"debug"}' "$RT_API/v1/services/$SVC/env"

    # secret: build the body from stdin so the value stays out of the process list
    jq -n --arg v "$STRIPE_KEY" '{value: $v}' | curl -sS -X PUT -H "Authorization: Bearer $RT_KEY" \
      -H 'Content-Type: application/json' -d @- "$RT_API/v1/services/$SVC/secrets/STRIPE_KEY"      # 204

    curl -sS -H "Authorization: Bearer $RT_KEY" "$RT_API/v1/services/$SVC/secrets"                  # names only
    curl -sS -X DELETE -H "Authorization: Bearer $RT_KEY" "$RT_API/v1/services/$SVC/secrets/STRIPE_KEY"
    ```

In the handler, both appear together:

```ts
const greeting = Deno.env.get("GREETING") ?? "Hello";      // env variable
const stripeKey = Deno.env.get("STRIPE_KEY");              // secret
```

In a WASM component they are the WASI environment (`std::env::var("STRIPE_KEY")` in Rust). Changes apply to the next
task. A warm isolate or instance that holds the old values is replaced, so a rotated secret takes effect without a
deploy. Do not use the same name for a variable and a secret. Secret values never appear in logs or events (events record
names only), but your own code can log them: do not.

## Environments and allowed callers

Services of one tenant can call each other with `std:services`. Two settings decide who may call whom.

**`environment`** is a label on the service, `production` unless you set another (`staging`, `pr-42`, `^[a-z][a-z0-9-]{0,31}$`).
It is used for these call decisions only. It does not change routing, limits or storage. Changing it needs the root token
or a key with no project or environment restriction, because it changes who may call whom (the change is recorded as a
`service.environment_changed` event).

**`call.allowed_callers`** lists who may call the service. Each entry is one of:

| Entry | Admits |
|---|---|
| `checkout` (a slug) | The service holding that slug **at call time** |
| `svc_01M3XM1AMYAKX6EREP6J536ATG` (an id) | Exactly that service |
| `env:staging` | Every service of the same project whose environment is `staging` |

An **empty list** (the default) admits services of the same project in the same environment. A **non-empty list** admits only
the listed callers: it replaces the same-environment rule, so add `env:production` if production services should keep
calling. Prefer ids for sensitive targets, because a slug entry follows the slug if the service is deleted and the slug is
reused. A service of another tenant is admitted only by its id and only when its plan allows cross-tenant calls. A service
may always call itself.

### Worked example: production and staging callers

`pricing` quotes prices and should be reachable by other services only. `storefront` calls it. Later a staging copy of
`storefront` needs to call the same `pricing`.

```ts title="pricing/main.ts"
export default function (req: Request): Response {
  const sku = new URL(req.url).searchParams.get("sku") ?? "";
  return Response.json({ sku, cents: sku === "42" ? 1999 : 500 });
}
```

```ts title="storefront/main.ts"
import { call } from "std:services";

export default async function (): Promise<Response> {
  try {
    const res = await call("pricing", "http://pricing/quote?sku=42");
    return Response.json({ price: await res.json() });
  } catch (e) {
    // call_not_allowed, not_found, forbidden, ...: refused before pricing ran
    return Response.json({ error: (e as { code?: string }).code }, { status: 502 });
  }
}
```

**1. Both in `production`.** An empty caller list admits same-environment callers.

```bash
21ci service create pricing --access internal
21ci push pricing/main.ts --service pricing --wait
21ci push storefront/main.ts --service storefront --create --wait
curl -sS "$RT_INVOKE/s/storefront/"        # {"price":{"sku":"42","cents":1999}}
```

**2. A staging copy.** Same code, different environment, empty list on `pricing`: refused.

```bash
21ci service create storefront-staging --env staging
21ci push storefront/main.ts --service storefront-staging --wait
curl -sS "$RT_INVOKE/s/storefront-staging/"   # {"error":"call_not_allowed"}
```

**3. Admit exactly who you want.** The production storefront by id, plus everything in `staging`.

=== "21ci"

    ```bash
    SF=$(21ci service get storefront --json | jq -r .id)
    21ci service update pricing --allowed-caller "$SF" --allowed-caller env:staging
    curl -sS "$RT_INVOKE/s/storefront-staging/"   # {"price":{"sku":"42","cents":1999}}
    ```

=== "curl"

    ```bash
    SF=$(curl -sS -H "Authorization: Bearer $RT_KEY" "$RT_API/v1/services?slug=storefront" | jq -r '.items[0].id')
    PRICING=$(curl -sS -H "Authorization: Bearer $RT_KEY" "$RT_API/v1/services?slug=pricing" | jq -r '.items[0].id')
    curl -sS -X PATCH -H "Authorization: Bearer $RT_KEY" -H 'Content-Type: application/json' \
      -d "{\"call\":{\"allowed_callers\":[\"$SF\",\"env:staging\"]}}" "$RT_API/v1/services/$PRICING"
    ```

    `PATCH` replaces the whole `call` block.

Now `storefront` and every `staging` service are admitted. Any other `production` service is refused, because the list no
longer includes the same-environment rule. To go back to the default, clear the list:

```bash
21ci service update pricing --no-allowed-callers
```

Calls, nesting limits and `std:queue` enqueues between services are covered in [Calls and queues](calls-and-queues.md).

The environments a service may use are listed by its **project**: `--env staging` works only when the service's project
has a `staging` environment (`21ci project update <project> --add-env staging`), otherwise the update is
`422 validation_failed` listing the allowed ones. See [Projects and environments](projects.md).

## Schedules, queues and domains

These have their own guides. One command each to start:

- **Schedules:** `21ci schedule add hello --every 5m` or `--cron "0 3 * * *" --tz Europe/Berlin`
  ([Schedules](schedules.md)). The plan's `min_schedule_seconds` and `max_schedules` apply, and `cron` must be allowed.
- **Queues:** `21ci queue send hello --body '{"job":1}'`, `21ci queue dlq hello --redrive`
  ([Calls and queues](calls-and-queues.md)).
- **Custom domains:** `21ci host attach hello shop.example.com`, then publish the DNS record it prints
  ([Custom domains](custom-domains.md)).

## Scaling and warm instances

| Setting | What it does | Trade-off |
|---|---|---|
| `scaling.min_instances` (`--min-instances`) | Instances kept warm: the service never scales to zero and its concurrency limit never drops below it (default 0) | No cold start after idle, but the warm instances hold memory all the time |
| `scaling.max_instances` (`--max-instances`) | Most instances running at once (default: the plan's `max_instances`, else the profile ceiling) | Protects a downstream system, but requests beyond it wait in the queue |
| `scaling.max_queued` | Length of the service's queue; beyond it `503 overloaded` with `Retry-After` | A short queue fails fast; a long one hides overload as latency |
| `scaling.keep_resident` | Accepted by the API; not active in this release | None yet |
| `deno.reuse_isolates` (`--reuse-isolates`) | Keep an isolate with the evaluated module for the next request of the same version | Faster; module-level variables now persist between requests, so never keep per-user data there |
| `wasm.reuse_instances` (`--reuse-instances`) | The same for WASM instances | The same |
| `profile` (`--profile`) | `max_density`, `balanced` or `min_latency`: a set of defaults for compile mode, reuse and pooling | Density versus latency; unset means the node's profile |

```bash
21ci service update api --reuse-isolates true --min-instances 1 --max-instances 16 --set scaling.max_queued=200
```

By default every request runs in a fresh isolate, which is why `reuse_isolates` is a decision about correctness as well as
speed: code written for isolation can break when state survives. The node adapts the concurrency limit on its own from
observed load and latency; `min_instances` and `max_instances` only bound it, and both are capped by the plan's
`max_instances` (a larger value is clamped, and `service get` shows `plan` as its source). See [Scheduler and autoscaling](../concepts/scheduler.md), [Execution model](../concepts/execution.md) and
[Optimize a service](optimize.md).

## How plan limits reach your service

The plan is set by the operator and cannot be changed through a service's config; you see its effect in three places:

| Plan key | What you see when you hit it |
|---|---|
| `limits_max` | `422 limit_exceeds_plan` on `PATCH`; the source `plan` in `effective` |
| `max_services`, `max_schedules` | `403 quota_exceeded` on create |
| `min_schedule_seconds` | `422 schedule_too_frequent` |
| `allowed_triggers` | `403 trigger_not_allowed` |
| `rps`, `invocations_per_day` | `429 rate_limited` / `quota_exceeded` with `Retry-After` on invoke |
| `max_concurrency` (tenant-wide) | Requests queue, then `503 queue_timeout` |
| `max_instances` (per service) | `scaling.min_instances` / `max_instances` above it are clamped; `Instances: … (plan)` in `service get` |
| `storage_mb`, `egress_mb_per_day` | `storage_quota` / `egress_quota` errors inside your code |
| `builds_per_hour` | `429 build_quota` on upload |
| `custom_host_suffixes` | `403 host_not_allowed` when claiming a host |

`21ci whoami --json | jq .plan` shows your plan with the tenant's overrides applied. The resolution order for each knob is:

```text
plan maximum  ≥  service explicit  >  node config (rt.toml)  >  profile  >  built-in default
```

Details are in [Tenants, plans and keys](../concepts/tenants-and-plans.md) and
[Services, versions and builds](../concepts/services-and-deploys.md).

## Troubleshooting

| Symptom | Cause | Fix |
|---|---|---|
| A setting disappeared after a `PATCH` | You sent a block without its other fields | Send the whole block, or use `21ci service update --set` |
| `422 validation_failed` on `PATCH` | Unknown field or block, or a bad value | The message names the field; blocks are listed above |
| `412 precondition_failed` | Somebody changed the service since you read it | Read it again and repeat |
| `422 limit_exceeds_plan` | The value is above the plan maximum | Use `details.max` or ask for a bigger plan |
| `403 insufficient_scope` on `--env` | The key has a project or environment restriction | Use an unrestricted key or the operator |
| `403 forbidden` / `405` / `429` on a public URL | An `ingress` rule | `21ci service get` and read `Ingress` |
| `404` on a service that has a live version | `triggers.http` is off, or `access` is `internal` | Check `Triggers` and `Access` |
| `call_not_allowed` in `std:services` | The caller is not admitted by `call.allowed_callers` or its environment differs | [Worked example](#worked-example-production-and-staging-callers) |
| New env value not seen | The key lacks `env:write` (the PUT failed) or you set a secret with the same name | `21ci env get`, `21ci secret list` |

## See also

- [Deploy and roll back](deploy.md): the code side of a service.
- [Logs, stats and usage](logs-and-usage.md): measure before and after a change.
- [Services, versions and builds](../concepts/services-and-deploys.md): the service object and the config blocks.
- [Calls and queues](calls-and-queues.md), [Schedules](schedules.md) and [Custom domains](custom-domains.md).
- [`21ci service`](../cli.md#service), [`env`](../cli.md#env) and [`secret`](../cli.md#secret).
