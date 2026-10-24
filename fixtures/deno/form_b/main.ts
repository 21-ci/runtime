// Form B: explicit handlers. Everything the handlers observe is written to the task log as one
// JSON line so tests can assert on it without a response channel (`scheduled`/`queue` return void).
interface Ctx {
  taskId: string;
  trigger: string;
  service: { id: string; slug: string; version: number };
}
interface ScheduledEvent {
  scheduleId: string;
  scheduledTime: number;
  payload: unknown;
}
interface QueueMessage {
  id: string;
  body: unknown;
  attempt: number;
  ack(): void;
  retry(o?: { delaySeconds?: number }): void;
}

export default {
  fetch(_req: Request, ctx: Ctx): Response {
    return new Response("b:" + ctx.trigger);
  },
  scheduled(event: ScheduledEvent, ctx: Ctx): void {
    console.log(JSON.stringify({ handler: "scheduled", trigger: ctx.trigger, event }));
  },
  queue(batch: { messages: QueueMessage[] }, ctx: Ctx): void {
    console.log(
      JSON.stringify({
        handler: "queue",
        trigger: ctx.trigger,
        messages: batch.messages.map((m) => ({ id: m.id, body: m.body, attempt: m.attempt })),
      }),
    );
    for (const m of batch.messages) m.ack();
  },
};
