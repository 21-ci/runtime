# service-call

Service-to-service calls with `std:services`, background work with `ctx.waitUntil`, and the call depth limit.

## Shows

- `import { call } from "std:services"`: `call(slug, request: Request) -> Promise<Response>`
- In-process child task (no HTTP hop, no TCP connection). Same tenant only; the target's invoke access is checked as an internal-tenant call, so even `access=internal` services can be called
- `ctx.waitUntil(promise)`: the response goes out at once, the isolate stays alive until the promise settles (bounded by the wall limit)
- A failing child arrives as a `Response` (for example `504 timeout`, `508 call_depth`), not as an exception. A call that cannot be made at all throws an error with a stable `code` (`not_found`, `cross_tenant_call`, `forbidden`, `call_failed`); the sample turns it into `status` 404 / 403 / 502 plus `reason` so the caller keeps running

## Deploy

The target first, then the caller:

```bash
cd sample/deno/hello         && ../../scripts/deploy.sh . hello
cd ../service-call           && ../../scripts/deploy.sh . service-call
```

## Invoke

```bash
S=../../scripts/invoke.sh
$S service-call '/call?target=hello&name=from-service'     # extra query params are forwarded; &path=/x picks a path
$S service-call '/background?target=hello'
$S service-call '/recurse'
```

## Expected responses

```json
// /call
{
  "caller": "service-call",
  "target": "hello",
  "status": 200,
  "reason": null,
  "response": { "message": "Hello, from-service!", "method": "GET", "path": "/", "…": "…" },
  "taskId": "task_01J…"
}

// /background (202, returns immediately; the call to hello finishes under waitUntil)
{ "message": "call scheduled", "target": "hello", "taskId": "task_01J…" }

// /recurse: every level calls itself again; the runtime answers 508 once the chain is too deep
{ "depth": 0, "child": { "depth": 1, "child": { "…": "…", "child": { "status": 508, "reason": "call_depth" } } } }
```

## Limits

| Aspect | Rule |
|---|---|
| Scope | same tenant; a service of another tenant answers `403 cross_tenant_call` unless the operator plan allows it |
| Depth | <= 15, otherwise `508` with `x-rt-reason: call_depth`; all tasks in the chain still finish |
| Deadline | child wall time = min(its own limit, the parent's remaining time); a child killed at that point shows up as a `504` response |
| Accounting | CPU and memory are charged to the called service |
