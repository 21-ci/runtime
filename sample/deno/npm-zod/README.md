# npm-zod

Validation with [zod](https://zod.dev) from npm, and a directory deploy with `rt.json`.

## Shows

- `rt.json`: `{ "entry": "main.ts", "imports": { "zod": "npm:zod@3" } }`; the import map is applied before the resolution rules
- `npm:pkg@range` specifiers resolve **at build time** through `build.npm_cdn` (default `https://esm.sh`, `?target=es2022`);
  the fetched URLs and hashes are pinned in the version's `rt.lock`. Nothing is fetched while a request runs
- The build is lazy: `deploy.sh` answers at once with `build_state: pending`, the **first request** fetches and compiles
  (a few seconds); later requests, and other services using the same dependency, hit the cache

## Deploy

```bash
cd sample/deno/npm-zod
../../scripts/deploy.sh . npm-zod
```

`deploy.sh` reads `rt.json` and uploads one `application/json` bundle built with `jq`:

```json
{ "entry": "main.ts", "files": { "main.ts": "…source…" }, "imports": { "zod": "npm:zod@3" } }
```

## Invoke

```bash
../../scripts/invoke.sh npm-zod / POST '{"title": "Learn Zod", "priority": "high"}'
../../scripts/invoke.sh npm-zod / POST '{"priority": "low"}'
```

## Expected responses

```json
// valid (201); `priority` would default to "medium"
{ "message": "valid", "todo": { "title": "Learn Zod", "priority": "high" } }

// invalid (400)
{
  "error": "validation failed",
  "issues": [
    { "code": "invalid_type", "expected": "string", "received": "undefined", "path": ["title"], "message": "Required" }
  ]
}
```

## Notes

- Hosts allowed for build-time fetches: `build.allowed_hosts` (default esm.sh, jsr.io, deno.land, cdn.jsdelivr.net, unpkg.com).
  Air-gapped nodes point `build.npm_cdn` at an internal mirror.
- If the build fails (CDN down, bad specifier) requests answer `502` with `x-rt-reason: build_failed`;
  `POST /v1/services/{id}/versions/{v}/rebuild` clears the negative cache.
