# kv-counter

Persistent state with `std:kv` (per-service store, strong consistency, TTL).

## Shows

- `import { kv } from "std:kv"`
- `kv.incr(key, delta, { ttlSeconds })`, `kv.get<T>(key)`, `kv.delete(key)`, `kv.list({ prefix, cursor, limit })`
- TTL: a counter expires 1 hour after the increment that created it (later increments keep that expiry), then starts
  over at 1; expired keys read as `null`
- Limits: key <= 2 KB, value <= 1 MB (plan)

## Deploy

```bash
cd sample/deno/kv-counter
../../scripts/deploy.sh . kv-counter
```

## Invoke

```bash
../../scripts/invoke.sh kv-counter '/?key=visits&action=increment'
../../scripts/invoke.sh kv-counter '/?key=visits&action=get'
../../scripts/invoke.sh kv-counter '/?action=list'
../../scripts/invoke.sh kv-counter '/?key=visits&action=delete'
```

## Expected responses

```json
// increment (first call, then 2, 3, …)
{ "key": "visits", "value": 1 }

// get
{ "key": "visits", "value": 1 }

// delete
{ "key": "visits", "deleted": true }
```

`list` returns what the SDK returns (one page of keys under `counters/` plus the cursor for the next page).

## Notes

- `increment` is `kv.incr`: atomic, so concurrent tasks never lose an update. The counter is stored as JSON, so `get`
  reads it as a number; a key holding something else (a string, bytes) fails with `kv_not_integer`.
- The KV store is private to the service: another service of the same tenant reads `null` for the same key.
