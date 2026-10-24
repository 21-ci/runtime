# js-componentize

A JavaScript handler compiled to a WASI Preview 2 HTTP component with [`jco componentize`](https://github.com/bytecodealliance/jco)
(ComponentizeJS on SpiderMonkey/StarlingMonkey), for a `runtime: "wasm"` service.

## Shows

- The JS listens for the service-worker style `fetch` event; ComponentizeJS wires it to `wasi:http/incoming-handler`
- A WIT world that exports `wasi:http/incoming-handler`, the one thing the runtime requires (`wit/world.wit`)
- It is **not** the Deno contract: no `export default`, no `ctx`, no `Deno.*`, and the Deno modules (`std:kv`, `std:sql`, …,
  `sdk:*`) do not exist. Platform capabilities reach a component through the `std:host/*` WIT interfaces instead (add them to the
  world; this minimal example uses none)
- The component is pre-initialized at build time (Wizer): module-level code runs once during the build, each task gets a fresh copy-on-write
  instance, so `hitsInThisInstance` is `1` on every request under the default profiles
- Build flag `--disable stdio` keeps the import set small: without it the engine also imports `wasi:cli/stdin` and
  `wasi:cli/terminal-*`. The runtime accepts those (it links all of `wasi:cli`: stdin empty, `exit` ends the task, `terminal-*` as
  "no terminal"), so the flag is optional. Trade-off: with it, `console.log` output is not captured; drop it to get `console.log`
  lines in the service logs

## Layout

| Path | Role |
|---|---|
| `src/app.js` | the handler |
| `wit/world.wit` | `world hello { export wasi:http/incoming-handler@0.2.10; }` |
| `wit/deps/` | WASI WIT packages the world refers to (`wasi:http`, `wasi:io`, `wasi:clocks` at 0.2.10, the version the engine of `@bytecodealliance/componentize-js` 0.22 speaks), vendored from the WebAssembly/WASI snapshots bundled with jco; see the header of each file |
| `build.sh` | `jco componentize` + export/import check (needs `wasm-tools` for the check) |

## Build

```bash
cd sample/wasm/js-componentize
./build.sh            # npm ci on the first run; ~3 s afterwards
# -> dist/app.wasm (~14 MB, the JS engine is embedded; the runtime limit is 50 MB)
```

## Deploy

```bash
../../scripts/deploy.sh dist/app.wasm js-componentize
```

## Invoke

```bash
../../scripts/invoke.sh js-componentize '/?name=Alice'
```

## Expected response

```json
{
  "message": "Hello from WASM component, Alice!",
  "method": "GET",
  "path": "/",
  "basePath": "/s/js-componentize",
  "hitsInThisInstance": 1,
  "timestamp": "2026-10-01T19:10:15.900Z"
}
```

## Try it without the runtime

`wasmtime serve dist/app.wasm` runs it on any `wasi:http` host.
