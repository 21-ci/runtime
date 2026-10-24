---
title: Your first service
description: Scaffold, deploy, call and roll back a Deno service and a WASM component with 21ci or curl, then read where to go next.
---

# Your first service

Now you act as the **developer**. You deploy a TypeScript service, call it, read its logs, ship a second version and roll
it back, then do the same with a WebAssembly component. This page is the fast path; each step links to the guide that goes
deeper.

You need the tenant key from [First run](first-run.md), either in the current `21ci` context or in `RT_KEY`:

```bash
export RT_API=http://127.0.0.1:8081 RT_INVOKE=http://127.0.0.1:8080 RT_KEY=rtk_…
21ci whoami      # confirms the key works; see Get access if it does not
```

## A Deno service

### Scaffold it

```console
$ 21ci init hello --template deno
Created hello (template deno)
  README.md
  main.ts
Next: cd hello && 21ci push . --service hello --create
```

`main.ts` exports a default function that takes a standard Web `Request` and returns a `Response`. The second argument,
`ctx`, carries the task id, the service and the trigger. An excerpt of the scaffold:

```ts title="hello/main.ts (excerpt)"
export default async function (req: Request, ctx: Ctx): Promise<Response> {
  const url = new URL(req.url);
  const name = url.searchParams.get("name") ?? "World";

  return Response.json({
    message: `Hello, ${name}!`,
    path: url.pathname,
    basePath: req.headers.get("x-rt-base-path"), // "/s/hello"
    taskId: ctx.taskId,
    trigger: ctx.trigger,                         // "http"
  });
}
```

The runtime strips the `/s/{slug}` prefix, so for `GET /s/hello/x?name=Ada` your handler sees `/x?name=Ada`.

### Deploy it

=== "21ci"

    ```console
    $ cd hello
    $ 21ci push . --service hello --create --wait
    Service:  svc_01M3XM1AMYAKX6EREP6J536ATG (slug: hello, runtime: deno, created)
    Upload:   application/typescript, 1287 bytes
    Version:  1 (build_state: building, sha256:4128185b…)
    Building: ready
    Live:     yes
    Invoke:   http://127.0.0.1:8080/s/hello/
    ```

    `--create` creates the service the first time. `--wait` builds the version now and waits until it is ready; without it
    the build happens on the first request.

=== "curl"

    ```bash
    SVC=$(curl -sS -H "Authorization: Bearer $RT_KEY" -H 'Content-Type: application/json' \
      -d '{"slug":"hello","runtime":"deno"}' "$RT_API/v1/services" | jq -r .id)

    curl -sS -H "Authorization: Bearer $RT_KEY" -H 'Content-Type: application/typescript' \
      --data-binary @main.ts "$RT_API/v1/services/$SVC/versions?activate=1&compile=eager"
    ```

    `?activate=1` makes the new version live; `compile=eager` starts the build now. The upload answers `202` and does
    not wait for the build; poll `GET /v1/services/$SVC/versions/1` until `build_state` is `ready`.

Directories, archives, import maps, npm packages and CI are in [Deploy and roll back](../guides/deploy.md).

### Call it

=== "21ci"

    ```console
    $ 21ci invoke hello '/x?name=Ada'
    -> GET http://127.0.0.1:8080/s/hello/x?name=Ada
    <- HTTP 200
       content-type: application/json
       server-timing: queue;dur=0.000, wait_runtime;dur=0.000, init;dur=5.932, exec;dur=4.720
       x-rt-task-id: task_0000000000000002
       x-rt-version: 1

    {
      "message": "Hello, Ada!",
      "path": "/x",
      "basePath": "/s/hello",
      …
    }
    ```

=== "curl"

    ```bash
    curl -i "$RT_INVOKE/s/hello/x?name=Ada"
    ```

Every response carries `x-rt-task-id` (find this request in the logs), `x-rt-version` (which version answered) and
`server-timing` (time spent queued, loading, initializing and executing). By default every request runs in a fresh
isolate, so module-level variables do not survive between requests; keep state in the
[built-in storage](../concepts/storage.md) instead.

### Read the logs

Everything the handler writes with `console.*` goes to the service log, tagged with the task id:

```bash
21ci logs hello -f          # follow (Server-Sent Events); Ctrl-C to stop
21ci logs hello --task task_0000000000000002
```

```console
2026-10-02T06:15:53.026Z info  v1 - build started (deno)
2026-10-02T06:15:53.052Z info  v1 - build finished in 25 ms
2026-10-02T06:15:55.560Z info  v1 task_000000000000000b hit /a
```

Filters, time ranges, build logs, stats and usage are in [Logs, stats and usage](../guides/logs-and-usage.md).

### Ship a new version, then roll back

Change the greeting in `main.ts` and push again. Each push creates a new immutable version and makes it live; requests
already running finish on the old one.

```console
$ 21ci push . --service hello --wait
$ 21ci versions hello
VERSION  BUILD    LIVE  DIGEST               CREATED
2        ready    *     sha256:e5fdfbd20a63  2026-10-02T06:15:26Z
1        ready    -     sha256:4128185bbcac  2026-10-02T06:15:14Z

$ 21ci rollback hello
Service hello (svc_01M3XM1AMYAKX6EREP6J536ATG) is now live at version 1 (was 2).
```

A rollback only moves the live pointer. With curl it is `PUT /v1/services/{id}/live` with `{"version": 1}`. If a lazily
built version fails to compile on its first request, the node moves the live pointer back to the previous healthy version
by itself and records a `rollback.auto` event.

### Add state and settings

The `deno-kv` template is a counter in the per-service key/value store, updated with the atomic `kv.incr`:

```console
$ 21ci init counter --template deno-kv && cd counter
$ 21ci push . --service counter --create --wait
$ 21ci invoke counter '/?key=visits&action=increment'
```

Environment variables and secrets apply to the next request, without a redeploy:

```bash
21ci env set hello GREETING=Hi
printf %s "$OPENAI_API_KEY" | 21ci secret set hello OPENAI_API_KEY
```

The handler reads both with `Deno.env.get("GREETING")` and `Deno.env.get("OPENAI_API_KEY")`. Limits, access modes and
the rest of the settings are in [Configure a service](../guides/configure.md); the storage modules are in
[Sub-services](../guides/sub-services.md).

## A WASM service

WASM services are WASI Preview 2 **components** that export `wasi:http/incoming-handler`. Rust builds them directly with the
`wasm32-wasip2` target (included in the repository's `rust-toolchain.toml`).

```console
$ 21ci init hello-rs --template wasm-rust && cd hello-rs
$ ./build.sh
Built target/wasm32-wasip2/release/hello_rs_wasm.wasm (… bytes)
```

Deploy and call it the same way. `21ci push` sees the `.wasm` file and creates a `wasm` service:

=== "21ci"

    ```console
    $ 21ci push target/wasm32-wasip2/release/hello_rs_wasm.wasm --service hello-rs --create --wait
    $ 21ci invoke hello-rs '/?name=Ferris'
    ```

=== "curl"

    ```bash
    SVC=$(curl -sS -H "Authorization: Bearer $RT_KEY" -H 'Content-Type: application/json' \
      -d '{"slug":"hello-rs","runtime":"wasm"}' "$RT_API/v1/services" | jq -r .id)
    curl -sS -H "Authorization: Bearer $RT_KEY" -H 'Content-Type: application/wasm' \
      --data-binary @target/wasm32-wasip2/release/hello_rs_wasm.wasm \
      "$RT_API/v1/services/$SVC/versions?activate=1&compile=eager"
    curl "$RT_INVOKE/s/hello-rs/?name=Ferris"
    ```

```json
{ "hitsInThisInstance": 1, "message": "Hello from Rust WASM component, Ferris!", "method": "GET", "path": "/", "unixSeconds": 1790881616 }
```

As with Deno, each task gets a fresh component instance by default, so `hitsInThisInstance` stays `1`.
`21ci init hello-js --template wasm-js` scaffolds a JavaScript handler that `jco componentize` turns into a component
(`./build.sh` needs Node.js 20 or newer). More in [WASM services](../guides/wasm.md) and the
[language guide](../languages/wasm.md).

## Next steps

- [Deploy and roll back](../guides/deploy.md): every upload format, dependencies, build failures, CI.
- [Configure a service](../guides/configure.md): limits, access, env, secrets, callers.
- [Logs, stats and usage](../guides/logs-and-usage.md): debug and measure what you deployed.
- [Deno services](../guides/deno.md) and [WASM services](../guides/wasm.md): write real handlers.
- [Tutorials](../tutorials/index.md): complete apps, step by step.
- [Production deployment](../guides/production.md): TLS, backups, monitoring, upgrades and key rotation.
