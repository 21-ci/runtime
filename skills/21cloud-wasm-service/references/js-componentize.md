# JavaScript components (`jco componentize`)

[ComponentizeJS](https://github.com/bytecodealliance/jco) embeds a JS engine (StarlingMonkey) in a WASI P2 component
and wires the service-worker style `fetch` event to `wasi:http/incoming-handler`. Example:
[sample/wasm/js-componentize](https://github.com/21-ci/runtime/tree/main/sample/wasm/js-componentize)
(`21ci init <dir> --template wasm-js` scaffolds the same files).

Choose this only when you need a WASM artifact. For plain TypeScript/JavaScript services the Deno runtime is simpler and
has the `std:*` modules (skill `21cloud-deno-service`).

## Layout

| Path | Role |
|---|---|
| `src/app.js` | the handler |
| `wit/world.wit` | `package example:hello-js; world hello { export wasi:http/incoming-handler@0.2.10; }` |
| `wit/deps/` | vendored `wasi:http`, `wasi:io`, `wasi:clocks` WIT packages at 0.2.10 (the version the engine of `@bytecodealliance/componentize-js` 0.22 speaks) |
| `package.json` | devDependencies `@bytecodealliance/jco` `^1.35.0`, `@bytecodealliance/preview2-shim` `0.26.0`; Node.js >= 20 |
| `build.sh` | `jco componentize` + export/import check |

## Handler

```js
// Not the Deno contract: no export default, no ctx, no std:* imports, no Deno.*
addEventListener("fetch", (event) => {
  event.respondWith(handle(event.request));
});

async function handle(request) {
  const url = new URL(request.url);                       // path is /{rest}; /s/{slug} is stripped
  const trigger = request.headers.get("x-rt-trigger");    // http, interval, cron, schedule, queue, call
  if (url.pathname === "/__rt/queue") {
    const { messages } = await request.json();            // [{ id, body, attempt }]
    // ... process; a 2xx acks the batch, anything else retries it
    return new Response(null, { status: 204 });
  }
  return new Response(JSON.stringify({ path: url.pathname, base: request.headers.get("x-rt-base-path") }), {
    headers: { "content-type": "application/json" },
  });
}
```

Module-level code runs once at build (pre-initialization); each task gets a fresh copy of that state, so module
variables do not carry data between requests (except as a cache with `wasm.reuse_instances`).

## Build

```bash
npm ci                                   # or npm install (the scaffold has no lockfile)
npx --no-install jco componentize src/app.js \
  --wit wit \
  --world-name hello \
  --disable stdio \
  --out dist/app.wasm                    # ~14 MB (engine embedded); the upload limit is 50 MB
21ci push dist/app.wasm --service my-fn --create --wait
```

`--disable stdio` keeps the import set small but drops `console.log` output. The runtime links all of `wasi:cli`
(stdin empty, `exit` ends the task, `terminal-*` as "no terminal"), so you can omit the flag to get `console.log`
lines in the service logs.

## Host capabilities

`std:host/*` interfaces reach a component only through its WIT world: add imports of
[wit/rt-host.wit](https://github.com/21-ci/runtime/blob/main/wit/rt-host.wit) interfaces to `world.wit` (and the
file under `wit/deps/` or `wit/`). The repository has no JavaScript sample that does this; follow jco's documentation
for importing WIT interfaces from JS and verify with `wasm-tools component wit dist/app.wasm` that the component
imports `std:host/<name>@0.1.0` and still exports `wasi:http/incoming-handler@0.2.x`.

Outbound `fetch` inside the component goes through `wasi:http/outgoing-handler` and the runtime's egress guard
(public internet only).

## Check

```bash
wasm-tools component wit dist/app.wasm | grep -E '^\s*(import|export) '
wasmtime serve dist/app.wasm             # local smoke test on any wasi:http host
```
