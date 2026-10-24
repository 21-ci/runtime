import { sql } from "std:sql";
import { enqueue } from "std:queue";

// Form B: explicit handlers `{ fetch, scheduled, queue }` (all optional, at least one required).
//
//   fetch      HTTP, plus a way to enqueue and to look at what the other handlers did
//   scheduled  called for every interval / cron schedule fire (event.payload = the schedule's payload)
//   queue      called with a batch (≤ 10) of messages; ack / retry per message
//
// Every task is a fresh isolate, so state lives in std:sql, never in module variables.

interface Ctx {
  taskId: string;
  service: { id: string; slug: string; version: number };
  trigger: "http" | "interval" | "cron" | "queue" | "call";
  invoker?: string;
  deadline: number;
  waitUntil(p: Promise<unknown>): void;
}
interface ScheduledEvent {
  scheduleId: string;
  scheduledTime: number;
  payload: unknown;
}
interface QueueMessage {
  id: string;
  body: unknown;
  attempt: number; // 1 on the first delivery
  ack(): void;
  retry(o?: { delaySeconds?: number }): void;
}
interface QueueBatch {
  messages: QueueMessage[];
}

// Message body used by this example (JSON, ≤ 1 MB).
interface Job {
  text: string;
  failUntilAttempt?: number; // simulate a failing job: fail while attempt < this value
}

async function ensureSchema() {
  await sql.execute(
    `CREATE TABLE IF NOT EXISTS events (
       id         INTEGER PRIMARY KEY AUTOINCREMENT,
       kind       TEXT    NOT NULL,
       detail     TEXT    NOT NULL,
       attempt    INTEGER,
       created_at INTEGER NOT NULL DEFAULT (CAST(strftime('%s', 'now') AS INTEGER))
     )`,
  );
}

async function record(kind: string, detail: unknown, attempt: number | null = null) {
  await ensureSchema();
  await sql.execute("INSERT INTO events (kind, detail, attempt) VALUES (?, ?, ?)", [kind, JSON.stringify(detail), attempt]);
}

export default {
  async fetch(req: Request, ctx: Ctx): Promise<Response> {
    const url = new URL(req.url);

    try {
      // POST /enqueue {"text": "…", "failUntilAttempt"?: 3, "delaySeconds"?: 5, "dedupId"?: "…"}
      if (url.pathname === "/enqueue" && req.method === "POST") {
        const body = await req.json().catch(() => null) as
          | (Job & { delaySeconds?: number; dedupId?: string })
          | null;
        if (!body || typeof body.text !== "string") {
          return Response.json({ error: 'body must be JSON like {"text": "…"}' }, { status: 400 });
        }
        const job: Job = { text: body.text, failUntilAttempt: body.failUntilAttempt };
        // enqueue(service, body, {delaySeconds?, dedupId?}): same tenant; here the target is this service.
        await enqueue(ctx.service.slug, job, {
          ...(body.delaySeconds !== undefined && { delaySeconds: body.delaySeconds }),
          ...(body.dedupId !== undefined && { dedupId: body.dedupId }), // same id within 24 h = one message
        });
        return Response.json({ enqueued: job }, { status: 202 });
      }

      // GET /  -> what scheduled() and queue() recorded so far
      if (url.pathname === "/" && req.method === "GET") {
        await ensureSchema();
        const r =await sql.execute("SELECT id, kind, detail, attempt, created_at FROM events ORDER BY id DESC LIMIT 20");
        const events = r.rows.map((row: ArrayLike<unknown>) =>
          Object.fromEntries((r.columns as string[]).map((c, i) => [c, row[i]]))
        );
        return Response.json({ events });
      }

      return Response.json({ error: "not found" }, { status: 404 });
    } catch (e) {
      console.error(e);
      return Response.json({ error: (e as Error).message }, { status: 500 });
    }
  },

  // Interval/cron fire. Schedules never overlap and missed ticks are skipped (no backfill).
  async scheduled(event: ScheduledEvent, ctx: Ctx): Promise<void> {
    console.log(`[scheduled] ${event.scheduleId} trigger=${ctx.trigger} task=${ctx.taskId}`);
    await record("scheduled", { scheduleId: event.scheduleId, scheduledTime: event.scheduledTime, payload: event.payload });
  },

  // Delivery is at-least-once: make the work idempotent. A message that is neither acked nor
  // retried counts as acked when the handler returns normally, so errors are handled per message.
  async queue(batch: QueueBatch, ctx: Ctx): Promise<void> {
    console.log(`[queue] ${batch.messages.length} message(s) task=${ctx.taskId}`);
    for (const msg of batch.messages) {
      const job = msg.body as Job;
      try {
        if (typeof job?.failUntilAttempt === "number" && msg.attempt < job.failUntilAttempt) {
          await record("queue-retry", { id: msg.id, text: job.text }, msg.attempt);
          // Without arguments the runtime applies exponential backoff (2 s × 2^attempt, ±20 %) and
          // moves the message to the DLQ after queue.max_attempts (5).
          msg.retry({ delaySeconds: 1 });
          continue;
        }
        await record("queue-done", { id: msg.id, text: job?.text ?? msg.body }, msg.attempt);
        msg.ack();
      } catch (e) {
        console.error(e);
        msg.retry();
      }
    }
  },
};
