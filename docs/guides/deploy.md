---
title: Deploy and roll back
description: Create a service and ship code to it as an immutable version from one file, a project directory, an archive or a WASM component, then check the build, call it, roll back and delete it.
---

# Deploy and roll back

This guide is the whole deploy loop for a tenant developer, with `21ci` and `curl` side by side. It assumes you have a
tenant key (see [Get access](access.md)) with `services:read`, `services:write` and `deploy`, plus `logs:read` to read
build logs. Set the environment once:

```bash
export RT_API=http://127.0.0.1:8081      # control listener
export RT_INVOKE=http://127.0.0.1:8080   # public listener
export RT_KEY=rtk_01M3XM1AG5HN6WY9S6JQ550JQV_dtok…   # your tenant key
```

`21ci` reads the same variables, so no `21ci config` step is needed. Contexts are covered in [Get access](access.md).

## How a deploy works

```mermaid
flowchart LR
  U["Upload<br/>POST …/versions"] --> V["Version N<br/>pending or building"]
  V --> R["ready"]
  V -.-> F["failed"]
  R --> L["live pointer<br/>?activate=1 or PUT …/live"]
```

- A **service** has a fixed slug and a fixed runtime (`deno` or `wasm`). Everything else about it can change later
  ([Configure a service](configure.md)).
- Every upload creates a new, immutable, numbered **version**. A fix is always a new version; nothing is edited in
  place. This is what makes rollback a pointer move instead of a rebuild.
- Exactly one version is **live**. New requests run the live version; requests already running finish on the version
  they started with.
- The node validates the upload without network access, stores it, and compiles it on its own (a build). By default the
  build happens on the first request, so a deploy that returns `201` has not proven that the code builds. Use `--wait`
  to find out at deploy time ([Builds and failures](#builds-and-failures)).

## Project, environment, service

A service lives in a **project** and in one of the project's **environments**. Every tenant starts with the project
`default` and its environment `production`, so the examples below need neither. When you ship more than one app, or
stages of one app, set them up first:

```bash
21ci project create web --env production,staging      # once (needs a key with no project or environment restriction)
21ci push ./src --service api-staging --create --project web --env staging --wait
21ci push ./src --service api --create --project web --env production --wait
```

The project of a service is fixed at creation; the environment must be one of the project's, and it decides which
services may call each other. See [Projects and environments](projects.md).

## Deploy one file

Create `main.ts`:

```ts title="main.ts"
export default async function (req: Request): Promise<Response> {
  const name = new URL(req.url).searchParams.get("name") ?? "World";
  return Response.json({ message: `Hello, ${name}!` });
}
```

=== "21ci"

    ```console
    $ 21ci push main.ts --service hello --create --wait
    Service:  svc_01M3XM1AMYAKX6EREP6J536ATG (slug: hello, runtime: deno, created)
    Upload:   application/typescript, 1287 bytes
    Version:  1 (build_state: building, sha256:4128185b…)
    Building: ready
    Live:     yes
    Invoke:   http://127.0.0.1:8080/s/hello/
    ```

    | Flag | Effect |
    |---|---|
    | `--service SLUG` | Target service. Default: the file or directory name turned into a slug |
    | `--create` | Create the service if it does not exist. Without it a missing service is an error |
    | `--project P`, `--env ENV` | With `--create`: the project (default `default`) and environment (default `production`) of the new service |
    | `--wait` | Build now and wait until the version is `ready`. Exit code 1 if it fails or after `--wait-timeout` (default 120 s) |
    | `--compile lazy\|eager` | When the build runs. `--wait` implies `eager` |
    | `--no-activate` | Upload but keep the current live version |
    | `--runtime deno\|wasm` | Runtime of a created service. Implied by the artifact (`.wasm` is `wasm`) |

    Push again without `--create` for the next version: `21ci push main.ts --service hello`.

=== "curl"

    ```bash
    # 1. create the service (once)
    SVC=$(curl -sS -H "Authorization: Bearer $RT_KEY" -H 'Content-Type: application/json' \
      -d '{"slug":"hello","runtime":"deno"}' "$RT_API/v1/services" | jq -r .id)

    # 2. upload version 1, make it live, build it now
    curl -sS -H "Authorization: Bearer $RT_KEY" -H 'Content-Type: application/typescript' \
      --data-binary @main.ts "$RT_API/v1/services/$SVC/versions?activate=1&compile=eager"
    ```

    ```json
    {"version":1,"source_digest":"sha256:…","lock_digest":null,"build_state":"building","failure":null,"created_at":…}
    ```

    `?activate=1` makes the new version live as part of the upload. `?compile=eager` starts the build now and answers
    `202`; `lazy` answers `201` and builds on the first request. Without `compile`, the service's profile decides
    (`lazy` in `balanced`).

    The upload does not wait for the build. Poll the version until it leaves `building`:

    ```bash
    until [ "$(curl -sS -H "Authorization: Bearer $RT_KEY" "$RT_API/v1/services/$SVC/versions/1" | jq -r .build_state)" != building ]; do sleep 1; done
    ```

Call it:

=== "21ci"

    ```bash
    21ci invoke hello '/?name=Ada'
    ```

=== "curl"

    ```bash
    curl -i "$RT_INVOKE/s/hello/?name=Ada"
    ```

The response carries `x-rt-task-id`, `x-rt-version` and `server-timing`. [Logs, stats and usage](logs-and-usage.md)
explains what to do with them.

!!! tip "Create the service first when you need settings"
    `push --create` makes a plain service (in `--project` / `--env` when given). To set an access mode, instance bounds
    or limits at creation, run
    `21ci service create` first (or add the blocks to the `POST /v1/services` body), then push. See
    [Configure a service](configure.md).

## What you can upload

| What | `21ci push` | `Content-Type` for curl | Runtime |
|---|---|---|---|
| One TypeScript file | `main.ts` | `application/typescript` | deno |
| One JavaScript file | `main.js`, `main.mjs` | `text/javascript` | deno |
| A project directory | `shop/` | `application/json` (a bundle, built for you) | deno |
| A zip archive | `app.zip` | `application/zip` | deno |
| A tar archive | `app.tar` | `application/x-tar` (`application/gzip` for `.tar.gz`) | deno |
| A WASI P2 component | `app.wasm` | `application/wasm` | wasm |

Limits at upload: 1 MB for a single file, 1,000 files and 1 MB per file and 10 MB in total for a bundle or archive,
50 MB for a component. Bigger is `413 too_large`. The node checks the upload synchronously and without network access;
a failed check creates no version and a source error comes back with `file`, `line`, `col` and `message`:

```console
$ 21ci push broken.ts --service hello
error: validation_failed: the upload failed validation (HTTP 422)
  {
    "diagnostics": [
      { "file": "broken.ts", "line": 1, "col": 17, "message": "Expression expected" }
    ]
  }
```

Deno builds strip types and do not type-check. Run `deno check` or `tsc` in your own CI if you want type errors to fail
a build.

## Deploy a project directory

A service can have many files. Point `push` at the directory:

```text
shop/
  rt.json
  main.ts
  lib/
    prices.ts
    tax.ts
```

```bash
21ci push shop --service shop --create --wait
```

`21ci` reads the directory and uploads one JSON bundle `{"entry", "files", "imports"?}`:

- It includes the `.ts .tsx .mts .js .jsx .mjs .json` files below the directory, sorted by path.
- It leaves out dot files and dot directories, `node_modules`, symbolic links and `rt.json` itself (its contents go into
  `entry` and `imports`).
- Sources must be UTF-8 text. Images, fonts and `.wasm` files are not part of a bundle. Put data in a `.json` or `.ts`
  module, or keep it in the service's [built-in storage](sub-services.md).
- **Entry point.** `rt.json`'s `entry` if given; otherwise the first of `main.ts`, `main.js`, `index.ts`, `index.js`,
  `mod.ts`. No match is an error that asks you to add `rt.json`. (An archive or bundle that reaches the node without an
  `entry` falls back to `main.ts`, `main.tsx`, `main.js`, `main.jsx`, `main.mjs`.)
- A directory with exactly one source file and no `rt.json` is uploaded as that single file.

`rt.json` has two optional keys:

```json title="shop/rt.json"
{
  "entry": "main.ts",
  "imports": { "zod": "npm:zod@3" }
}
```

=== "Zip or tar with curl"

    ```bash
    zip -qr app.zip main.ts lib rt.json
    curl -sS -H "Authorization: Bearer $RT_KEY" -H 'Content-Type: application/zip' \
      --data-binary @app.zip "$RT_API/v1/services/$SVC/versions?activate=1&compile=eager"

    # or a tar archive
    tar -cf app.tar main.ts lib rt.json
    curl -sS -H "Authorization: Bearer $RT_KEY" -H 'Content-Type: application/x-tar' \
      --data-binary @app.tar "$RT_API/v1/services/$SVC/versions?activate=1&compile=eager"
    ```

    `21ci push app.zip` and `21ci push app.tar` do the same. The node reads archives in memory and never extracts them to
    disk. It refuses entries with `..`, absolute paths or links, and archives that inflate more than 100 times.

=== "JSON bundle with curl"

    ```bash
    curl -sS -H "Authorization: Bearer $RT_KEY" -H 'Content-Type: application/json' \
      "$RT_API/v1/services/$SVC/versions?activate=1&compile=eager" -d '{
        "entry": "main.ts",
        "files": {
          "main.ts": "import { tax } from \"./lib/tax.ts\";\nexport default () => new Response(String(tax(100)));\n",
          "lib/tax.ts": "export const tax = (n: number) => n * 0.2;\n"
        }
      }'
    ```

## Dependencies: npm, jsr and `rt.lock`

Deno services can import more than their own files. The node resolves every import **at build time**, bundles the result
and never touches the network when your handler runs.

| Specifier | Resolves to |
|---|---|
| `./x.ts`, `../lib/y.js` | Files of your upload |
| `npm:pkg@range` | The package through the operator's npm CDN (default `https://esm.sh`) |
| `jsr:@scope/pkg@range` | The package through the same CDN's JSR path |
| `https://host/…` | Fetched only if the host is on the node's build allow-list (default `esm.sh`, `jsr.io`, `deno.land`, `cdn.jsdelivr.net`, `unpkg.com`) |
| `zod` (a bare name) | Only through the `imports` map of `rt.json` |
| `std:kv`, `std:sql`, `std:blob`, `std:queue`, `std:services`, `std:platform` | Built-in runtime modules |
| `node:…`, `file:…` | Refused at upload |

```ts title="main.ts"
import { z } from "zod";          // mapped by rt.json to npm:zod@3

const Todo = z.object({ title: z.string().min(1) });

export default async function (req: Request): Promise<Response> {
  const parsed = Todo.safeParse(await req.json());
  return parsed.success
    ? Response.json(parsed.data, { status: 201 })
    : Response.json({ issues: parsed.error.issues }, { status: 400 });
}
```

**What `rt.lock` pins, and when.** The first successful build of a version records every remote module it fetched, with
its digest, in an `rt.lock` (the version's `lock_digest`) and stores the fetched modules next to the source. Rebuilding
that version (`21ci rebuild`, or the automatic rebuild after a runtime upgrade) uses the lock and the stored modules,
so it reproduces the same code even if the CDN has changed or is down; if a locked URL has to be fetched again and its
content differs, the build fails with `lock_mismatch`. You never write or upload `rt.lock`.

Treat **every new upload as a fresh resolution**. A range such as `npm:zod@3` is resolved by the CDN when the new version
builds, so two deploys of the same source can pick different releases. Pin exact versions (`npm:zod@3.23.8`) when you
need the same code across deploys. If a dependency is missing or unreachable, the build fails with `resolve` or `fetch`;
`--wait` shows that at deploy time instead of on the first request.

Node.js built-ins are not available (no `fs`, `net`, `child_process`). Packages built for browsers or web-standard
runtimes usually work. [Deno services](deno.md) and the [Deno language page](../languages/deno.md) list what works.

## Deploy a WASM component

A `wasm` service is a WASI Preview 2 component that exports `wasi:http/incoming-handler`. In Rust:

```bash
rustup target add wasm32-wasip2
cargo build --release --target wasm32-wasip2
```

=== "21ci"

    ```bash
    21ci push target/wasm32-wasip2/release/my_service.wasm --service my-service --create --wait
    21ci invoke my-service /
    ```

=== "curl"

    ```bash
    SVC=$(curl -sS -H "Authorization: Bearer $RT_KEY" -H 'Content-Type: application/json' \
      -d '{"slug":"my-service","runtime":"wasm"}' "$RT_API/v1/services" | jq -r .id)
    curl -sS -H "Authorization: Bearer $RT_KEY" -H 'Content-Type: application/wasm' \
      --data-binary @target/wasm32-wasip2/release/my_service.wasm \
      "$RT_API/v1/services/$SVC/versions?activate=1&compile=eager"
    ```

The upload check requires a component (not a core module) that validates without threads or shared memory and exports
`wasi:http/incoming-handler`. Anything else is `422 invalid_artifact`. The component may import only interfaces the
runtime links: `wasi:http`, `wasi:io`, `wasi:clocks`, `wasi:random`, `wasi:cli` (stdin is empty, `exit` ends the task,
`terminal-*` reports no terminal), `wasi:filesystem` (nothing to open) and `wasi:sockets` (every address denied), all
`0.2.x`, and the `std:host` interfaces. There is no dependency resolution: your toolchain compiles everything into the component. The
node's build step compiles it to native code (120 s limit by default), so build large components eagerly (`--wait`).

A `deno` service cannot receive a component, and the reverse. The runtime is fixed when the service is created. See
[WASM services](wasm.md) and the [WASM language page](../languages/wasm.md).

## Start from a template

`21ci init` writes a working project from the repository samples (embedded in the binary):

```console
$ 21ci init hello --template deno
Created hello (template deno)
  README.md
  main.ts
Next: cd hello && 21ci push . --service hello --create
```

| `--template` | Scaffolds | Runtime |
|---|---|---|
| `deno` (default) | A handler that echoes request details | deno |
| `deno-kv` | A counter in the per-service key/value store | deno |
| `wasm-rust` | A Rust crate for `wasm32-wasip2` with a `build.sh` | wasm |
| `wasm-js` | A JavaScript handler that `jco componentize` turns into a component (`build.sh` needs Node.js 20+) | wasm |

A non-empty target directory is refused unless you pass `--force`.

## Versions, activation and rollback

```bash
21ci versions hello             # all retained versions; * marks the live one
21ci versions hello 2           # one version: build state, digests, failure
21ci rollback hello             # live pointer to the previous healthy version
21ci rollback hello --to 5      # any retained version, also forward
21ci source hello 2 --out v2.ts # the exact bytes that were uploaded
```

```console
$ 21ci versions hello
VERSION  BUILD    LIVE  DIGEST               CREATED
2        ready    *     sha256:e5fdfbd20a63  2026-10-02T06:15:26Z
1        ready    -     sha256:4128185bbcac  2026-10-02T06:15:14Z

$ 21ci rollback hello
Service hello (svc_01M3XM1AMYAKX6EREP6J536ATG) is now live at version 1 (was 2).
```

The same with curl. `PUT …/live` takes the service `etag` as `If-Match`, so a change made by somebody else in between
fails with `412 precondition_failed` instead of being overwritten:

```bash
curl -sS -H "Authorization: Bearer $RT_KEY" "$RT_API/v1/services/$SVC/versions"
ETAG=$(curl -sS -H "Authorization: Bearer $RT_KEY" "$RT_API/v1/services/$SVC" | jq -r .etag)
curl -sS -X PUT -H "Authorization: Bearer $RT_KEY" -H 'Content-Type: application/json' -H "If-Match: $ETAG" \
  -d '{"version":1}' "$RT_API/v1/services/$SVC/live"
```

How it behaves:

- **A rollback only moves the live pointer.** Nothing is uploaded or rebuilt by the rollback itself; if the version's
  compiled code was evicted from the node's cache, the first request rebuilds it from the stored source. Tasks already
  running finish on the version they started with.
- **Retention.** The node keeps the newest `deploy.retain_versions` versions (10 by default; the operator sets it) plus
  the live one and deletes older ones on a later upload. You can only roll back to a retained version.
- **A failed version cannot go live** (`409 version_not_ready`). A `pending` one can; it builds on its first request.
- **Without `--to`**, `rollback` picks the newest version older than the live one that did not fail to build.
- Every deploy, live change and rollback is recorded: `21ci events hello`.

### Move traffic only after the build succeeded

`push` activates by default. To keep serving the old version until the new one is built:

```bash
21ci push ./shop --service shop --no-activate --wait    # prints the new version number, e.g. 7
21ci rollback shop --to 7                               # PUT …/live {"version": 7}
```

With curl, upload without `activate`, wait for `build_state` to become `ready`, then `PUT …/live`. This guarantees that
the build is ready when traffic moves. It does not test your code: a non-live version cannot be called.

## Builds and failures

| `build_state` | Meaning |
|---|---|
| `pending` | Uploaded, build not started (lazy mode: it starts on the first request that needs it) |
| `building` | A build processor is compiling it |
| `ready` | Compiled and cached; requests run it directly |
| `failed` | The build failed; `failure` says why |

**Lazy or eager.** In `max_density` and `balanced` the default is `lazy`; in `min_latency` it is `eager`. With a lazy
build the first request pays for compiling and waits for it only as long as the service's queue timeout (5 s in
`balanced`), then ends with `503 queue_timeout` while the build carries on. For dependency-heavy bundles and large
components build eagerly: `--wait`, `--compile eager`, or `?compile=eager`.

When a version fails:

=== "21ci"

    ```bash
    21ci versions hello 7             # failure: reason, message, diagnostics
    21ci build-logs hello 7           # the whole build log; ends when the build is ready or failed
    21ci logs hello --kind build      # build records of every version
    21ci rebuild hello 7              # clear a failed build and build again
    ```

=== "curl"

    ```bash
    curl -sS  -H "Authorization: Bearer $RT_KEY" "$RT_API/v1/services/$SVC/versions/7"
    curl -sSN -H "Authorization: Bearer $RT_KEY" "$RT_API/v1/services/$SVC/versions/7/build/logs"   # SSE
    curl -sS -X POST -H "Authorization: Bearer $RT_KEY" "$RT_API/v1/services/$SVC/versions/7/rebuild"
    ```

`failure.reason` is one of:

| Reason | Cause | What to do |
|---|---|---|
| `syntax` | A file does not parse | Fix the file named in `diagnostics` and push again |
| `resolve` | An import cannot be resolved (missing file, unknown bare name, bad specifier) | Fix the path or add the name to `rt.json` `imports` |
| `fetch` | A dependency could not be downloaded | Check the specifier; `rebuild` after a CDN outage |
| `lock_mismatch` | A locked module changed upstream | Push a new version, which resolves afresh |
| `timeout` | The build exceeded `build.timeout` (60 s for Deno, `build.wasm_timeout` 120 s for WASM) | Reduce dependencies, or ask the operator |
| `oom` | The build exceeded `build.memory_mb` | Same |
| `too_large` | One module (over 10 MB) or all remote modules together (`build.max_deps_mb`, 50 MB) are too large | Drop heavy packages |
| `internal` | Node-side error | `rebuild`; report it to the operator if it repeats |

The node remembers a failure so that it does not rebuild on every request. Tasks that were waiting for the failed build
end with `502` and `x-rt-reason: build_failed`. **Auto-rollback** is on by default (`deploy.auto_rollback`): if the
failed version is live and an older version is `ready`, `live` moves back to it and a `rollback.auto` event is recorded.

## Call your service

| Form | Example | The handler sees |
|---|---|---|
| Path prefix | `http://127.0.0.1:8080/s/hello/x?y=1` | `/x?y=1`, prefix in `x-rt-base-path` |
| Base domain | `https://hello.fn.example.com/x` (when the operator set `routing.base_domain`) | `/x` |
| Custom host | `https://bot.example.com/x` ([Custom domains](custom-domains.md)) | `/x` |

=== "21ci"

    ```bash
    21ci invoke hello '/x?name=Ada'
    21ci invoke hello /data -X POST -d '{"a":1}'
    21ci invoke reports /daily --key "$INVOKE_KEY"       # access=key services; or RT_INVOKE_KEY
    ```

=== "curl"

    ```bash
    curl -i "$RT_INVOKE/s/hello/x?name=Ada"
    curl -i -X POST -H 'Content-Type: application/json' -d '{"a":1}' "$RT_INVOKE/s/hello/data"
    curl -i -H "Authorization: Bearer $INVOKE_KEY" "$RT_INVOKE/s/reports/daily"
    ```

A service with `access: public` (the default) needs no credential. `access: key` needs a key with the `invoke` scope,
and `access: internal` is not reachable from the public listener. See [Configure a service](configure.md#access-modes).
Your control-plane key is never sent to the invoke listener by `21ci`.

## Delete a service

=== "21ci"

    ```bash
    21ci data export hello --out hello.db   # optional backup of its SQLite data (needs data:admin)
    21ci service delete hello               # add --keep-data to keep the KV, SQL and blob files
    ```

=== "curl"

    ```bash
    curl -sS -X DELETE -H "Authorization: Bearer $RT_KEY" "$RT_API/v1/services/$SVC"                 # 204
    curl -sS -X DELETE -H "Authorization: Bearer $RT_KEY" "$RT_API/v1/services/$SVC?keep_data=1"
    ```

Deleting is immediate and cannot be undone. It removes the service record with all its versions, env variables,
secrets, custom hosts and schedules, drops its routes, and, unless you keep it, deletes its KV, SQL and blob data. The slug becomes free for any service of the node. Daily usage counters stay in the tenant's usage report.
With `--keep-data` the data files stay on disk under the old service id: a new service with the same slug has a new id
and does not see them, so the flag is for operator-assisted recovery, not for "recreate and reattach".

## Deploy checklist

1. **Scopes.** The key holds `deploy` and `services:read`, plus `services:write` if the pipeline creates services.
2. **Service settings.** `access`, `environment`, `limits` and `triggers` are what you intend
   ([Configure a service](configure.md)). Plan limits cap `limits`; a value above the plan is `422 limit_exceeds_plan`.
3. **Env and secrets** exist on the service before the first request that needs them. They apply to the next task and do
   not need a new version.
4. **Dependencies are pinned** (`npm:pkg@1.2.3`) if you need reproducible deploys.
5. **Push with `--wait`.** The exit code tells you whether the build succeeded.
6. **Smoke test** through the invoke listener (`21ci invoke … --fail`) and read `x-rt-version` to confirm which version
   answered.
7. **Know the way back.** `21ci rollback <svc>` is one command; the previous version is retained.
8. **Watch the first minutes:** `21ci logs <svc> -f` and `21ci stats <svc>` ([Logs, stats and usage](logs-and-usage.md)).

## Deploy from CI

Everything `21ci` needs can come from the environment, and every command exits non-zero on failure (`1` for an API error
or a failed build, `2` for bad input, `3` for no connection, `4` when the node answered busy with a `Retry-After`; see
[exit codes](../cli.md#exit-codes-and-errors)). Give the pipeline its own key with only `deploy` and
`services:read` (add `services:write` if it creates services), limited to the service it ships and with an expiry
([Get access](access.md)). Store it as a CI secret, never in the repository.

The runner must reach the control API. The control listener has no network ACL of its own and is loopback-only by
default, so operators usually expose it through a VPN or an authenticated proxy. Use a self-hosted runner on that
network, or ask your operator what route exists.

=== "curl only"

    Needs only `curl`, `jq` and `zip`, which every runner has:

    ```yaml title=".github/workflows/deploy.yml"
    name: deploy
    on:
      push:
        branches: [main]

    jobs:
      deploy:
        runs-on: ubuntu-latest
        env:
          RT_API: ${{ vars.RT_API }}
          RT_KEY: ${{ secrets.RT_KEY }}
          SLUG: shop
        steps:
          - uses: actions/checkout@v4
          - name: Upload, build, then switch traffic
            run: |
              set -euo pipefail
              auth=(-H "Authorization: Bearer $RT_KEY")
              SVC=$(curl -fsS "${auth[@]}" "$RT_API/v1/services?slug=$SLUG" | jq -r '.items[0].id // empty')
              if [ -z "$SVC" ]; then
                SVC=$(curl -fsS "${auth[@]}" -H 'Content-Type: application/json' \
                  -d "{\"slug\":\"$SLUG\",\"runtime\":\"deno\"}" "$RT_API/v1/services" | jq -r .id)
              fi
              zip -qr app.zip main.ts lib rt.json
              VER=$(curl --fail-with-body -sS "${auth[@]}" -H 'Content-Type: application/zip' \
                --data-binary @app.zip "$RT_API/v1/services/$SVC/versions?compile=eager" | jq -r .version)
              for _ in $(seq 1 60); do
                STATE=$(curl -fsS "${auth[@]}" "$RT_API/v1/services/$SVC/versions/$VER" | jq -r .build_state)
                [ "$STATE" = ready ] && break
                if [ "$STATE" = failed ]; then
                  curl -fsS "${auth[@]}" "$RT_API/v1/services/$SVC/versions/$VER" | jq .failure
                  exit 1
                fi
                sleep 2
              done
              [ "$STATE" = ready ] || { echo "build did not finish"; exit 1; }
              curl --fail-with-body -sS -X PUT "${auth[@]}" -H 'Content-Type: application/json' \
                -d "{\"version\":$VER}" "$RT_API/v1/services/$SVC/live" > /dev/null
              echo "live: version $VER"
    ```

    The key needs `services:write`, and must not be restricted to some projects or environments, only for the create branch. Remove
    that branch (and the extra scope) once the service exists.

=== "21ci"

    The `21ci` binary must be on the runner. There are no published binaries yet; build it from the runtime
    repository (`make cli-install`, see [Install](../getting-started/install.md#install-the-cli)) or ask your operator for
    one.

    ```yaml title=".github/workflows/deploy.yml"
    jobs:
      deploy:
        runs-on: self-hosted
        env:
          RT_API: ${{ vars.RT_API }}
          RT_INVOKE: ${{ vars.RT_INVOKE }}
          RT_KEY: ${{ secrets.RT_KEY }}
          RT_RETRY: 3
        steps:
          - uses: actions/checkout@v4
          - run: 21ci doctor
          - run: 21ci push . --service shop --create --wait --wait-timeout 5m
          - run: 21ci invoke shop /healthz --fail --body-only
    ```

    `21ci doctor` fails the job early on a bad URL or token. `push --wait` fails it when the build fails. `RT_RETRY`
    rides out short `429`/`503` answers that carry a `Retry-After`. To undo a bad release from a job, run
    `21ci rollback shop`.

## Troubleshooting a deploy

| Symptom | Cause | Fix |
|---|---|---|
| `401 unauthorized` | Key missing, expired, revoked or mistyped | [Get access](access.md#what-401-and-403-mean) |
| `403 insufficient_scope` on push | The key lacks `deploy`, `services:read` (slug lookup) or `services:write` (`--create`) | New key with the missing scope |
| `404 not_found` for a service you created | The key's projects or environments do not include the service, or it belongs to another tenant | `21ci whoami` |
| `409 slug_taken` | Another service of the node uses the slug | Pick another slug |
| `422 validation_failed` with `diagnostics` | Syntax error, disallowed import or bad bundle | Fix the named file and line |
| `422 invalid_artifact` | The `.wasm` is a core module or lacks `wasi:http/incoming-handler` | Build a component for `wasm32-wasip2` |
| `422 limit_exceeds_plan` | A `limits` value is above the plan maximum | Lower it; `details` gives the maximum |
| `429 build_quota` | The plan's `builds_per_hour` is used up | Wait for `Retry-After` |
| `409 version_not_ready` | You tried to activate a failed version | Fix and push again, or `rebuild` |
| First request after deploy is slow or `503 queue_timeout` | Lazy build | Deploy with `--wait` |

## See also

- [Get access](access.md): keys, scopes and `21ci` setup.
- [Configure a service](configure.md): limits, access, env, secrets and callers.
- [Logs, stats and usage](logs-and-usage.md): check what the new version does.
- [Services, versions and builds](../concepts/services-and-deploys.md): the mechanisms behind builds, dependencies and rollbacks.
- [`21ci push` and friends](../cli.md#push): every flag.
