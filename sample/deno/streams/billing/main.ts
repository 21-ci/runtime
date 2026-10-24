import { sql } from "std:sql";
import { kv } from "std:kv";

// Consumer and subscriber: Form B with `fetch`, `stream` and `pubsub`.
//
//   stream(batch, ctx)   the durable topic `orders` (consumer group `billing`, declared in `events.consume`).
//                        Batches are per partition. Records are idempotent: a redelivery after a failure
//                        finds its row (primary key = partition + offset) and skips it.
//   pubsub(msg, ctx)     `orders.*.created` (fan-out copy, queue group `billing`, declared in `events.subscribe`)
//                        and `pricing.quote` (request/reply: the return value is the reply).
//   fetch                GET /           counters: fan-out copies seen, invoices stored
//                        GET /invoices   the last 20 invoices
//
// Every task is a fresh isolate: state lives in std:sql and std:kv, never in module variables.

interface Ctx {
  taskId: string;
  service: { id: string; slug: string; version: number };
  trigger: "http" | "call" | "pubsub" | "stream";
  deadline: number;
  waitUntil(p: Promise<unknown>): void;
}

// A stream record as the consumer receives it (`batch.messages[i]`).
interface StreamRecord {
  partition: number;
  offset: number;
  key: string | null;
  body: unknown; // parsed JSON, else a string
  headers: Record<string, string>;
  timestamp: number; // epoch ms
}

// `batch` as `stream(batch, ctx)` receives it. `failAt(m)` commits the records before m and redelivers from m.
interface StreamBatch {
  topic: string;
  group: string;
  partition: number;
  attempt: number; // 1 on the first delivery of this record
  messages: StreamRecord[];
  failAt(m: StreamRecord | number): void;
}

// A pub/sub delivery (`msg`). `replyable` is true for a `request`: the return value is its reply.
interface PubsubMessage {
  id: string;
  subject: string;
  body: unknown;
  headers: Record<string, string>;
  replyable: boolean;
  publishedAt: number;
}

interface Order {
  orderId: string;
  customer: string;
  region: string;
  amount: number;
  flaky?: boolean; // the consumer fails this record on its first two deliveries
}

// VAT by region for the quote responder.
const VAT: Record<string, number> = { eu: 1.2, us: 1.07, apac: 1.1 };

async function ensureSchema() {
  await sql.execute(
    `CREATE TABLE IF NOT EXISTS invoices (
       stream_partition INTEGER NOT NULL,
       stream_offset    INTEGER NOT NULL,
       order_id         TEXT    NOT NULL,
       customer         TEXT    NOT NULL,
       amount           REAL    NOT NULL,
       PRIMARY KEY (stream_partition, stream_offset)
     )`,
  );
}

export default {
  async fetch(req: Request, ctx: Ctx): Promise<Response> {
    await ensureSchema();
    const url = new URL(req.url);

    if (req.method === "GET" && url.pathname === "/") {
      const seen = (await kv.get("fanout:seen")) as number | null;
      const count = await sql.execute("SELECT COUNT(*) FROM invoices");
      return Response.json({ fanoutSeen: seen ?? 0, invoices: count.rows[0][0] });
    }

    if (req.method === "GET" && url.pathname === "/invoices") {
      const r = await sql.execute(
        "SELECT stream_partition, stream_offset, order_id, customer, amount FROM invoices ORDER BY stream_partition, stream_offset DESC LIMIT 20",
      );
      const items = r.rows.map((row) => Object.fromEntries(r.columns.map((c, i) => [c, row[i]])));
      return Response.json({ items });
    }

    return new Response("not found", { status: 404 });
  },

  // Records of one partition, in order. Each `INSERT OR IGNORE` is idempotent, so a batch that is
  // redelivered after `failAt` does not bill a record twice.
  async stream(batch: StreamBatch, ctx: Ctx) {
    await ensureSchema();
    for (const m of batch.messages) {
      const order = m.body as Order;
      // Test switch: fail the first two deliveries of this record. `failAt` commits everything
      // before it and redelivers from it; the records after it come again with it.
      if (order.flaky === true && batch.attempt < 3) {
        batch.failAt(m);
        return;
      }
      await sql.execute(
        "INSERT OR IGNORE INTO invoices (stream_partition, stream_offset, order_id, customer, amount) VALUES (?, ?, ?, ?, ?)",
        [m.partition, m.offset, order.orderId, order.customer, order.amount],
      );
    }
  },

  async pubsub(msg: PubsubMessage, ctx: Ctx) {
    if (msg.subject === "pricing.quote") {
      // A request: the return value is the reply (JSON, since it is an object).
      const q = msg.body as { region: string; amount: number };
      const vat = VAT[q.region] ?? 1.2;
      return { region: q.region, currency: "EUR", total: Math.round(q.amount * vat * 100) / 100 };
    }
    // `orders.<region>.created`: a fan-out copy. The durable record is the stream above, so this
    // only counts what it saw. A plain delivery returns nothing (answered 204).
    await kv.incr("fanout:seen", 1);
    return undefined;
  },
};
