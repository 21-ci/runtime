---
name: 21ci-deploy
description: Deploys, configures and operates services on the 21 Cloud Functions Runtime with the `21ci` CLI (or its MCP server). Use when the user wants to push a Deno or WASM service, create or update a service, set env vars or secrets, add schedules, send queue messages, read logs or stats, roll back, attach a custom host, mint a scoped token, or wire 21ci into CI.
---

# Deploying with `21ci`

`21ci` is the command line client for the runtime's control API (a small binary, no runtime dependencies). Docs:
https://21-ci.github.io/runtime/ ; full CLI reference:
[docs/cli.md](https://github.com/21-ci/runtime/blob/main/docs/cli.md).

References:
- [references/commands.md](references/commands.md): every command and flag, service config blocks.
- [references/ci.md](references/ci.md): contexts, environment variables, CI recipes, exit codes, retries, MCP.

## Before anything

1. **Find the credentials.** Run `21ci doctor` (exit 1 when a check fails) and `21ci whoami`. Settings come from env
   first (`RT_API`, `RT_INVOKE`, `RT_KEY`, `RT_TENANT`, `RT_CONTEXT`), then the current context in
   `~/.config/21ci/config.toml`. Defaults: control `http://127.0.0.1:8081`, invoke `http://127.0.0.1:8080`.
2. **Never print, echo or commit a token.** Do not put `--token rtk_…` on a command line you show the user (it lands in
   shell history); prefer `RT_KEY` from the environment or an existing context. Pass secret values on stdin
   (`printf %s "$V" | 21ci secret set svc NAME`), never as arguments.
3. **Use `--json` when you parse output** (stdout stays JSON; errors go to stderr as one JSON line).
4. **Ask before destructive commands**: `service delete` (drops data unless `--keep-data`), `tenant delete --purge`,
   `queue dlq --purge`, `objects rm`, `sockets close`, `token revoke`, `secret delete`.

If `21ci` is missing: build it from the runtime repo (`make cli` -> `target/debug/21ci`, or `make cli-install` ->
`~/.cargo/bin/21ci`, or `cargo install --path crates/rt-cli`).

## Deploy loop

```bash
21ci push ./my-svc --service my-svc --create --wait     # create if missing, upload, build now, make live
21ci invoke my-svc '/?name=Ada'                          # call it on the invoke listener
21ci logs my-svc --since 10m --level warn                # or: 21ci logs my-svc -f
```

`push <path>` accepts:

| `<path>` | Sent as | Runtime |
|---|---|---|
| `x.ts` | `application/typescript` | deno |
| `x.js`, `x.mjs` | `text/javascript` | deno |
| `x.wasm` | `application/wasm` (WASI P2 component) | wasm |
| `x.zip`, `x.tar` | the archive (optional `rt.json` inside) | deno |
| directory with `rt.json` | JSON bundle `{entry, files, imports?}` | deno |
| directory without `rt.json` | one source file: that file; several: a bundle, entry guessed from `main.ts`, `main.js`, `index.ts`, `index.js`, `mod.ts` | deno |

Bundles include `.ts .tsx .mts .js .jsx .mjs .json` files; dot files, `node_modules` and `rt.json` itself are left out.
`rt.json` holds only `entry` and `imports` (import map).

Key flags: `--service SLUG` (default: cleaned-up file/dir name), `--create` (only then is a missing service created;
with `--project P --env ENV`), `--runtime deno|wasm`, `--wait` (poll until `ready`; exit 1 on `failed` or after
`--wait-timeout`, default 120s; implies `--compile eager`), `--no-activate`, `--close-sockets`.

Always deploy with `--wait` when you can: without it a lazy build runs on the first request, and build failures (an
unresolvable `npm:` import) only show up there as `502 build_failed`. Syntax errors are `422 validation_failed` at upload
either way, with `file:line:col` diagnostics printed under the error.

## Configure the service

Create explicitly when settings matter (a slug is `^[a-z][a-z0-9-]{1,62}$`):

```bash
21ci service create api --runtime deno --access key --project web --env staging
21ci service create rooms --durable --access internal --set 'ws.allowed_origins=["https://chat.example.com"]'
21ci service update api --set limits.cpu_ms=500 --set triggers.cron=false
21ci service update api --set 'ingress.allowed_methods=["GET","POST"]' --unset scaling.max_instances
21ci service get api            # --json: resolved values and their sources under "effective"
```

`--set BLOCK.KEY=VALUE` (value parsed as JSON when it parses, else a string) and `--unset BLOCK.KEY` merge into the
block and send it with `If-Match` (a concurrent change fails with `412 precondition_failed`). Blocks: `limits`,
`scaling`, `triggers`, `ws`, `ingress`, `egress`, `deno`, `wasm` (fields in
[references/commands.md](references/commands.md#config-blocks)). `access`: `public` (default), `key` (invoke needs a
key with scope `invoke`), `internal` (only the internal listener and service calls).

## Env, secrets, schedules, queue

```bash
21ci env set api GREETING=hi MODE=prod        # next task sees it, no redeploy
21ci env unset api MODE
printf %s "$STRIPE_KEY" | 21ci secret set api STRIPE_KEY       # or --from-file FILE
21ci secret list api                          # names and update times only

21ci schedule add api --every 5m --payload '{"job":"cleanup"}'  # interval (whole seconds; 60s, 5m, 1h)
21ci schedule add api --cron "*/15 * * * *" --tz Europe/Berlin  # cron (not on the free plan)
21ci schedule list api
21ci queue send api --body '{"orderId":1}' --delay 30s --dedup order-1
21ci queue stats api && 21ci queue dlq api [--redrive | --purge]
```

The code must handle the trigger: `scheduled(event, ctx)` / `queue(batch, ctx)` (Deno Form B) or the synthetic
`POST /__rt/scheduled` / `/__rt/queue` requests.

## Investigate and recover

| Goal | Command |
|---|---|
| Versions and build state | `21ci versions api`, `21ci versions api 3` (shows `failure`) |
| Build log | `21ci build-logs api 3` |
| Errors of one request | take `x-rt-task-id` from `21ci invoke` output, then `21ci logs api --task task_…` |
| Recent errors as JSON | `21ci logs api --since 15m --level error --json` |
| Latency, outcomes, queueing | `21ci stats api --window 1h` |
| Control-plane history | `21ci events api` |
| Roll back | `21ci rollback api` (previous good version) or `--to N`; `--close-sockets` to drop WebSockets |
| Rebuild a failed version | `21ci rebuild api 3` |
| Get uploaded source back | `21ci source api 3 --out src.json` |
| Plan limits left today | `21ci limits` |
| Typed RPC client | `21ci types billing -d types` -> `types/billing.d.ts` |

Invoke failures carry the reason in `x-rt-reason` (`guest_error`, `cpu_limit`, `oom`, `timeout`, `build_failed`,
`queue_timeout`, ...). `21ci invoke --fail` exits 1 on status >= 400 (4 when the answer is busy with `Retry-After`).

## Exit codes

`0` ok, `1` API error or failed command (build failed, `--wait` timeout, `doctor` check failed, `invoke --fail` >= 400),
`2` usage error (nothing was sent), `3` network error, `4` busy (`429`/`503` with `Retry-After`; retry later or use
`--retry N`). Details and CI recipes: [references/ci.md](references/ci.md).

## Writing the service

Use skill `21cloud-deno-service` (TypeScript/JavaScript) or `21cloud-wasm-service` (Rust / JS components).
Scaffold: `21ci init <dir> --template deno|deno-kv|wasm-rust|wasm-js [--name SLUG]`.
Runnable samples: https://github.com/21-ci/runtime/tree/main/sample
