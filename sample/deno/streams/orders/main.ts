import { append } from "std:stream";
import { publish, request } from "std:pubsub";

// Producer: Form B with `fetch` only. One order does three things:
//   1. `append` to the durable topic `orders` (key = customer: same customer, same partition, in order;
//      `dedupId` = the order id: a retry within 2 minutes returns the first offset, nothing is stored twice)
//   2. `publish` `orders.<region>.created` (ephemeral fan-out: every subscriber gets a copy now, nothing is kept)
//   3. `request` `pricing.quote` (one responder answers; this service waits for the reply)
//
// Routes:
//   POST /orders                       create an order (also works through `21ci invoke --async`: the
//                                      response body is dropped there, only the status counts)
//   GET  /quote?region=eu&amount=120   ask the pricing responder
//
// No `events` block is needed here: append, publish and request are open to any service of the tenant.
// Every task is a fresh isolate, so nothing is kept in module variables.

interface Ctx {
  taskId: string;
  service: { id: string; slug: string; version: number };
  trigger: "http" | "call" | "pubsub" | "stream";
  invocation?: { id: string; attempt: number }; // async invocations only
  deadline: number;
  waitUntil(p: Promise<unknown>): void;
}

// The order as stored in the topic and published. `flaky` is a test switch for the consumer.
interface Order {
  orderId: string;
  customer: string;
  region: "eu" | "us" | "apac";
  amount: number;
  flaky?: boolean;
}

const REGIONS = new Set(["eu", "us", "apac"]);

function parseOrder(v: unknown): Order | null {
  if (v === null || typeof v !== "object") return null;
  const o = v as Partial<Order>;
  if (typeof o.orderId !== "string" || o.orderId === "" || o.orderId.length > 100) return null;
  if (typeof o.customer !== "string" || o.customer === "") return null;
  if (typeof o.region !== "string" || !REGIONS.has(o.region)) return null;
  if (typeof o.amount !== "number" || !(o.amount > 0)) return null;
  return { orderId: o.orderId, customer: o.customer, region: o.region as Order["region"], amount: o.amount, flaky: o.flaky === true };
}

// `code` of a std:* error ("no_responders", "timeout", ...), never its message.
function errorCode(e: unknown): string {
  return typeof (e as { code?: unknown })?.code === "string" ? (e as { code: string }).code : "unknown";
}

export default {
  async fetch(req: Request, ctx: Ctx): Promise<Response> {
    const url = new URL(req.url);

    if (req.method === "POST" && url.pathname === "/orders") {
      let order: Order | null;
      try {
        order = parseOrder(await req.json());
      } catch {
        order = null;
      }
      if (order === null) {
        return Response.json({ error: "bad_order" }, { status: 400 });
      }

      // Durable: resolves once the record is on disk. Same customer -> same partition -> in order.
      const stored = await append("orders", order, { key: order.customer, dedupId: order.orderId });
      // Fan-out: subscribers get it now, at most once. `dropped` counts copies that were not delivered.
      const fanout = await publish(`orders.${order.region}.created`, order);

      return Response.json({ stored, fanout }, { status: 201 });
    }

    if (req.method === "GET" && url.pathname === "/quote") {
      const region = url.searchParams.get("region") ?? "eu";
      const amount = Number(url.searchParams.get("amount") ?? "100");
      try {
        // One responder answers; the first reply wins. `timeoutMs` is capped by ctx.deadline.
        const quote = await request("pricing.quote", { region, amount }, { timeoutMs: 2000 });
        return Response.json({ quote });
      } catch (e) {
        const code = errorCode(e);
        const status = code === "no_responders" || code === "unavailable" ? 503 : 504;
        return Response.json({ error: code }, { status });
      }
    }

    return new Response("not found", { status: 404 });
  },
};
