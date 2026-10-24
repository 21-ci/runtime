// `std:stream` -- persistent, partitioned, replayable topics, Kafka style (docs/handlers.md).
//
//   append(topic, body, { key, headers, dedupId }) -> Promise<{ partition, offset, duplicate }>
//       durable when it resolves; same `key` (<= 256 bytes) -> same partition -> ordered; the same
//       `dedupId` (<= 128 bytes) on the topic within 2 min returns the original offset
//   appendBatch(topic, [{ body, key, headers, dedupId }]) -> Promise<Array<{ partition, offset, duplicate }>>
//       <= 100 records, <= 4 MiB in total, one transaction (all or nothing)
//   read(topic, { partition, from = "earliest" | "latest" | offset, fromTime, limit = 100 })
//       -> Promise<{ records, next, end }>   replay without a consumer group; `partition` is
//       required when the topic has more than one; exactly one of `from` / `fromTime`;
//       `limit` 1..100 (and <= 1 MiB of bodies); next = offset after the last returned,
//       end = the partition's next offset
//   info(topic) -> Promise<{ name, partitions: [{ partition, first, next }], retentionHours, maxMb, bytes }>
//
// Record: { partition, offset, key, body, headers, timestamp } (body: parsed JSON, else a string).
// Bodies: a Uint8Array as is, anything else as JSON.stringify(body); at most 1 MiB each.
// Consumers declare `events.consume` in the service config and export `stream(batch, ctx)`:
//   batch = { topic, group, partition, attempt, messages, failAt(m | offset) }; a normal return
//   commits through the last message, failAt(m) commits before m and redelivers from m (lowest
//   call wins), a throw commits nothing and retries.
// Error codes: validation_failed, too_large, not_found, stream_full, rate_limited, quota_exceeded,
// unavailable.
// The implementation lives in the base snapshot (js/bootstrap.js); this module only exposes it.
export const append = globalThis.__rt.stream.append;
export const appendBatch = globalThis.__rt.stream.appendBatch;
export const read = globalThis.__rt.stream.read;
export const info = globalThis.__rt.stream.info;
