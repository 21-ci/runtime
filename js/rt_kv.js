// `std:kv` -- key/value storage: string keys, JSON or byte values, per-service isolation.
//
//   get(key)                      -> value | null
//   getMany(keys)                 -> (value | null)[]            (same order as `keys`)
//   set(key, value, {ttlSeconds}) -> void
//   delete(key)                   -> boolean
//   list({prefix, cursor, limit, reverse}) -> { entries: [{ key, value }], cursor: string | null }
//                                    reverse: descending key order (the cursor pages downwards)
//   incr(key, delta = 1, {ttlSeconds}) -> number (bigint for a bigint delta)
//                                    atomic; a missing/expired key starts at 0 and gets the TTL,
//                                    an existing counter keeps its expiry
//   cas(key, expected, next, {ttlSeconds}) -> boolean
//                                    atomic compare-and-swap; expected/next null = absent / delete
//   expire(key, ttlSeconds | null) -> boolean   set or clear the expiry of a live key
//   ttl(key)                       -> seconds left (rounded up) | null
//
// Values are JSON-serializable or Uint8Array (type-tagged). Limits: key <= 2 KB, value <= 1 MB.
// The implementation lives in the base snapshot (js/bootstrap.js); this module only exposes it.
export const kv = globalThis.__rt.kv;
