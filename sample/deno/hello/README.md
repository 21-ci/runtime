# hello

Form A handler: `export default async function (req, ctx)`.

## Shows

- Form A signature and the `ctx` fields (`taskId`, `service`, `trigger`)
- Routing: `/s/hello/x?name=Ada` reaches the handler as `/x?name=Ada`; the prefix is in `x-rt-base-path`
- No warm state: a module-level counter is `1` on every request (top-level code runs per task)

## Deploy

```bash
cd sample/deno/hello
../../scripts/deploy.sh . hello
```

## Invoke

```bash
../../scripts/invoke.sh hello '/x?name=Alice'
```

## Expected response

```json
{
  "message": "Hello, Alice!",
  "method": "GET",
  "path": "/x",
  "basePath": "/s/hello",
  "service": { "id": "svc_01J…", "slug": "hello", "version": 1 },
  "trigger": "http",
  "taskId": "task_01J…",
  "hitsInThisTask": 1
}
```

The response also carries `x-rt-task-id`, `x-rt-version` and `server-timing`; `invoke.sh` prints them.
