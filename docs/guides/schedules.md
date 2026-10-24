---
title: Schedules
description: Run a service on a fixed interval or a cron expression, manage schedules with 21ci or the API, and know how overlap, missed runs and plan limits behave.
---

# Schedules

A schedule runs a service's handler on its own: every N seconds (**interval**) or at the times a **cron** expression
names, in a timezone you choose. This guide shows how to create and manage schedules, how to handle a fire in Deno and
WASM, the exact timing guarantees, the cron syntax the node accepts and the limits that apply.

## How schedules work

Schedules are stored with the service's metadata and loaded into one timer loop on the node. When a schedule is due,
the node starts a task for the service's **live version** with the trigger `interval` or `cron` and the schedule's
JSON payload. Nothing is sent over HTTP: the task is created directly, like a queue batch or a service call.

- **Fixed grid.** An interval schedule fires at creation time + N, + 2N, + 3N… A cron schedule fires at every time
  that matches the expression.
- **Never overlapping.** If the previous run of the same schedule is still going when the next tick comes, that tick
  is skipped. The grid does not move.
- **No backfill.** Ticks missed while the node was down, or skipped because of overlap, are not run later.
- **No retries.** A run that throws or times out is logged; the next run happens at the next tick. Put work that must
  not be lost into a [queue](calls-and-queues.md#queues).
- **Small jitter.** Each fire is delayed by a fixed offset between 0 and `min(1 s, period / 10)`, derived from the
  schedule id, so many schedules on the same minute do not start at the same instant. The handler still sees the
  exact grid time.

## Create a schedule

You need a service with a live version and a key with `services:write`.

=== "21ci"

    ```console
    $ 21ci schedule add reports --every 1h --payload '{"job":"cleanup"}'
    ID:        sch_01J…
    Kind:      interval
    Schedule:  every 3600s
    Enabled:   true
    Next due:  2026-10-07T11:00:12Z
    Payload:   {"job":"cleanup"}

    $ 21ci schedule add reports --cron "0 9 * * MON-FRI" --tz Europe/Berlin --payload '{"job":"digest"}'
    $ 21ci schedule add reports --every 5m --disabled          # create it switched off
    ```

    `--every` takes `60s`, `5m`, `1h` (a bare number is seconds, whole seconds only). Exactly one of `--every` and
    `--cron` is required; `--tz` needs `--cron`. The payload defaults to `{}`.

=== "curl"

    ```bash
    curl -sS -H "Authorization: Bearer $RT_KEY" -H 'Content-Type: application/json' \
      -d '{"kind":"interval","every_seconds":3600,"payload":{"job":"cleanup"}}' \
      "$RT_API/v1/services/$SVC/schedules"

    curl -sS -H "Authorization: Bearer $RT_KEY" -H 'Content-Type: application/json' \
      -d '{"kind":"cron","cron":"0 9 * * MON-FRI","timezone":"Europe/Berlin","payload":{"job":"digest"}}' \
      "$RT_API/v1/services/$SVC/schedules"
    ```

    ```json
    {"id":"sch_01J…","kind":"interval","every_seconds":3600,"cron":null,"timezone":null,
     "payload":{"job":"cleanup"},"enabled":true,"next_due":1791370812000}
    ```

    `$SVC` is the service id (`svc_…`). `POST` accepts an `Idempotency-Key` header.

| Field | Meaning |
|---|---|
| `kind` | `interval` or `cron` |
| `every_seconds` | Interval in whole seconds (`interval` only, at least 1 and at least the plan minimum) |
| `cron` | Cron expression (`cron` only), see [Cron syntax](#cron-syntax) |
| `timezone` | IANA zone name for `cron`, such as `Europe/Berlin` or `Asia/Tashkent`. Default `UTC` |
| `payload` | Any JSON up to 64 KB, handed to the handler. Default `{}` |
| `enabled` | `false` keeps the schedule but stops its fires. Default `true` |
| `next_due` | Next scheduled time in Unix ms (without jitter); `null` while disabled |

## List, update and delete

=== "21ci"

    ```console
    $ 21ci schedule list reports
    ID                              KIND      SCHEDULE    TZ  ENABLED  NEXT_DUE              PAYLOAD
    sch_01J…                        interval  every 3600s -   true     2026-10-07T11:00:12Z  {"job":"cleanup"}

    $ 21ci schedule update reports sch_01J… --every 30m
    $ 21ci schedule update reports sch_01J… --payload '{"job":"cleanup","maxAgeDays":7}'
    $ 21ci schedule update reports sch_01J… --disable        # --enable switches it back on
    $ 21ci schedule rm reports sch_01J…
    ```

=== "curl"

    ```bash
    curl -sS -H "Authorization: Bearer $RT_KEY" "$RT_API/v1/services/$SVC/schedules"

    curl -sS -X PATCH -H "Authorization: Bearer $RT_KEY" -H 'Content-Type: application/json' \
      -d '{"kind":"interval","every_seconds":1800}' "$RT_API/v1/services/$SVC/schedules/$SCH"

    curl -sS -X PATCH -H "Authorization: Bearer $RT_KEY" -H 'Content-Type: application/json' \
      -d '{"kind":"interval","enabled":false}' "$RT_API/v1/services/$SVC/schedules/$SCH"

    curl -sS -X DELETE -H "Authorization: Bearer $RT_KEY" "$RT_API/v1/services/$SVC/schedules/$SCH"   # 204
    ```

    `PATCH` requires `kind`. Fields you leave out keep their current values, so `{"kind":"interval","enabled":false}`
    only disables the schedule. To switch kinds, send the new kind's field (`every_seconds` or `cron`).

Deleting a service deletes its schedules.

## Handle the fire

=== "Deno, Form B"

    Export `scheduled(event, ctx)`. `event.payload` is the schedule's payload, `event.scheduledTime` the grid time in
    Unix ms, `ctx.trigger` is `"interval"` or `"cron"`.

    ```ts title="sample/deno/scheduled-queue/main.ts (excerpt)"
    --8<-- "sample/deno/scheduled-queue/main.ts:20:24"

    --8<-- "sample/deno/scheduled-queue/main.ts:98:102"
    ```

    The returned value is ignored. A throw ends the task as a `guest_error` in the logs.

=== "Deno Form A, WASM"

    A handler without `scheduled()` (a Form A function, or any WASM component) receives a synthetic request:

    ```http
    POST /__rt/scheduled
    content-type: application/json
    x-rt-trigger: interval

    {"scheduleId":"sch_01J…","scheduledTime":1791370812000,"payload":{"job":"cleanup"}}
    ```

    Decide by the `x-rt-trigger` header (`interval` or `cron`), which only the runtime can set, not by the path
    alone: any client can send `POST /__rt/scheduled` too. The response status is only written to the logs.

A module that exports only `scheduled` (and maybe `queue`) is valid: it answers HTTP requests with `500 guest_error`,
so keep it `access: internal` if it should not be reachable at all.

The full program is `sample/deno/scheduled-queue/` in the runtime repository: an interval schedule, a queue consumer
and an HTTP endpoint that shows what both recorded.

## Timing guarantees

| Situation | What happens |
|---|---|
| First run of an interval schedule | One interval after it was created (plus jitter), not immediately |
| First run of a cron schedule | The next time the expression matches |
| A run takes longer than the interval | The ticks during the run are skipped; the next run starts at the next tick after it finished. Counted in `rt_schedule_skipped_total` |
| The node was down | Missed ticks are dropped. A schedule whose next run is still ahead keeps it; otherwise an interval schedule next fires one interval after the node started, a cron schedule at its next match |
| The schedule is disabled, or the service's `interval` / `cron` trigger is switched off | No fires |
| The service has no live version | The tick is skipped |
| The tenant's plan no longer allows the trigger (an operator changed it) | Fires are dropped and counted in `rt_schedule_denied_total`; they resume when the plan allows it again |
| The run fails | Logged; no retry |

Because missed runs are not backfilled, write jobs that catch up by themselves: "delete everything older than 30
days", not "delete yesterday's rows".

## Cron syntax

The node parses expressions with the Rust `cron` crate. A standard **5-field** expression gets a seconds field of `0`
added in front; you can also write 6 fields (with seconds) or 7 (with a year).

```text
┌─ second (0-59)           optional: only in the 6- and 7-field forms
│ ┌─ minute (0-59)
│ │ ┌─ hour (0-23)
│ │ │ ┌─ day of month (1-31)
│ │ │ │ ┌─ month (1-12 or JAN-DEC)
│ │ │ │ │ ┌─ day of week (1-7 = SUN-SAT, or SUN-SAT)
│ │ │ │ │ │ ┌─ year           optional
* * * * * * *
```

| Syntax | Example | Meaning |
|---|---|---|
| `*` | `* * * * *` | every value |
| list | `0 8,12,18 * * *` | 08:00, 12:00 and 18:00 |
| range | `0 9-17 * * *` | every hour from 09:00 to 17:00 |
| step | `*/15 * * * *` | every 15 minutes |
| names | `0 9 * * MON-FRI` | 09:00 on weekdays (names are case-insensitive) |
| `?` | `0 0 1 * ?` | "any" in the day-of-month or day-of-week field |
| shorthand | `@hourly`, `@daily`, `@weekly`, `@monthly`, `@yearly` | top of the hour, midnight, Sunday midnight, the 1st, January 1st |

!!! warning "Day-of-week numbers start at Sunday = 1"
    Unlike classic Unix cron (`0`-`6`, Sunday = `0`), this parser numbers days `1`-`7` with **Sunday = 1** and
    rejects `0`. `0 9 * * 1-5` therefore means Sunday to Thursday. Use names (`MON-FRI`) and the question never
    comes up.

More things to know:

- When both day fields are restricted (`0 0 13 * FRI`), a day must match **both** (Friday the 13th), not either.
- `L`, `W` and `#` are not supported.
- The expression is evaluated in the schedule's `timezone`: `0 9 * * *` with `Europe/Berlin` runs at 09:00 Berlin
  time all year. Use `UTC` when you need a constant spacing.
- A 6-field expression can fire more than once a minute (`*/30 * * * * *`), but the plan's minimum period still
  applies, see below.

Common expressions:

| Expression | Runs |
|---|---|
| `*/5 * * * *` | every 5 minutes |
| `0 * * * *` | every hour, on the hour |
| `30 2 * * *` | 02:30 every day |
| `0 9 * * MON-FRI` | 09:00 Monday to Friday |
| `0 0 1 * *` | midnight on the 1st of every month |
| `0 */6 * * *` | 00:00, 06:00, 12:00, 18:00 |

## Limits

| Limit | Value | When exceeded |
|---|---|---|
| Payload | 64 KB of JSON | `422 validation_failed` |
| Schedules per service | plan `max_schedules` (3 on `free`) | `403 quota_exceeded` |
| Minimum period | plan `min_schedule_seconds` (60 s on `free`). For cron, the gap between its next two fires | `422 schedule_too_frequent` |
| `cron` schedules | `cron` must be in the plan's `allowed_triggers` (the `free` plan does not include it) | `403 trigger_not_allowed` |
| Service trigger switches | `triggers.interval` / `triggers.cron` must be on | `403 trigger_not_allowed` |
| Invalid expression, timezone or interval | | `422 validation_failed` |

Scheduled tasks get more time than HTTP requests by default: **300 s wall time and 10 s CPU**, capped by the plan's
[`limits_max`](../api.md#plans-built-in-free-more-under-plansname). On the built-in `free` plan that cap is 30 s and 1 s. A `wall_ms` or
`cpu_ms` that you set in the service's `limits` applies to scheduled tasks too. A scheduled task may make up to 200
outbound `fetch` calls (6 at a time).

Operators set the plan values in [`[plans.<name>]`](../api.md#plans-built-in-free-more-under-plansname); see
[Tenants and plans](../concepts/tenants-and-plans.md).

## Recipes

### Cleanup job

Delete old rows once an hour. The payload carries the retention, so you can change it with `schedule update` and
without a redeploy.

```ts title="main.ts"
import { sql } from "std:sql";

interface ScheduledEvent { scheduleId: string; scheduledTime: number; payload: unknown }

export default {
  async scheduled(event: ScheduledEvent): Promise<void> {
    const { maxAgeDays = 30 } = (event.payload ?? {}) as { maxAgeDays?: number };
    const cutoff = event.scheduledTime - maxAgeDays * 86_400_000;
    await sql.execute(
      "CREATE TABLE IF NOT EXISTS sessions (id TEXT PRIMARY KEY, created_at INTEGER NOT NULL)",
    );
    const r = await sql.execute("DELETE FROM sessions WHERE created_at < ?", [cutoff]);
    console.log(`cleanup: removed ${r.rowsAffected} sessions older than ${maxAgeDays} days`);
  },
};
```

```console
$ 21ci push main.ts --service cleanup --create
$ 21ci schedule add cleanup --every 1h --payload '{"maxAgeDays":30}'
```

The `DELETE` is relative to "now", so a skipped or missed run is harmless: the next one removes the same rows.

### Polling an API

Fetch new items every 5 minutes and process each one through the queue, so a slow or failing item is retried on its
own instead of failing the whole poll. `dedupId` keeps an item from being processed twice when two polls see it.

```ts title="main.ts"
import { enqueue } from "std:queue";

interface Ctx { service: { id: string; slug: string; version: number }; trigger: string }
interface Order { id: string; total: number }
interface QueueBatch {
  messages: { id: string; body: unknown; attempt: number; ack(): void; retry(o?: { delaySeconds?: number }): void }[];
}

export default {
  async scheduled(_event: unknown, ctx: Ctx): Promise<void> {
    const res = await fetch("https://api.example.com/v1/orders?status=new", {
      headers: { authorization: `Bearer ${Deno.env.get("API_TOKEN")}` },
    });
    if (!res.ok) {
      console.warn(`poll failed: HTTP ${res.status}`); // the next tick tries again
      return;
    }
    const { orders } = (await res.json()) as { orders: Order[] };
    for (const order of orders) {
      // Same id within 24 h = one message.
      await enqueue(ctx.service.slug, order, { dedupId: `order:${order.id}` });
    }
  },

  async queue(batch: QueueBatch): Promise<void> {
    for (const msg of batch.messages) {
      const order = msg.body as Order;
      try {
        // … handle the order, idempotently …
        console.log(`order ${order.id}: ${order.total}`);
        msg.ack();
      } catch (e) {
        console.error(e);
        msg.retry(); // backoff 2 s, 4 s, 8 s, … then the DLQ
      }
    }
  },
};
```

```console
$ printf %s "$API_TOKEN" | 21ci secret set orders API_TOKEN
$ 21ci schedule add orders --every 5m
```

Use `--every 5m` on the `free` plan; with a plan that allows `cron`, `--cron "*/5 * * * *"` aligns the polls to the
clock instead.

## Observe schedules

- `21ci logs <svc>` shows each run's `console.*` output; filter one run with `--task task_…`.
- On the metrics listener: `rt_schedule_fires_total` (runs started), `rt_schedule_skipped_total` (ticks skipped
  because of overlap or because the run could not start) and `rt_schedule_denied_total` (fires the plan refused). See
  [Metrics](../reference/metrics.md).
- `next_due` in `21ci schedule list` tells you when the next run is planned.

## One-off schedules from code

Code can also schedule a single run of its own service, for example a retry at the end of a back-off:

```ts
import { schedule } from "std:schedule";

// Due in 90 seconds; a second call with the same id replaces the pending one.
await schedule.after(90, { reason: "retry" }, { id: "cbu-rates-retry" });

export default {
  async scheduled(event, ctx) {
    if (event.id === "cbu-rates-retry") { /* event.payload, event.scheduledAt */ }
  },
};
```

The handler gets `event.id`, `event.scheduledAt` and `event.payload`, and `ctx.trigger` is `"schedule"`.
Delivery is at-least-once: a delivery that fails is retried with backoff and dropped after the fifth failed attempt, and a lost
one runs again, so key the work on `event.id`. Pending schedules are listed next to the recurring ones by `21ci schedule ls`
(kind `once`). The limits (1 000 pending per service, 16 KiB payloads, 30 days ahead) are in the
[handler reference](../reference/guest-api.md).

## Next steps

- [Service calls and queues](calls-and-queues.md): move slow or fallible work out of the scheduled run.
- [Triggers](../concepts/triggers.md): how schedules fit next to HTTP, queues and calls.
- [CLI reference: `schedule`](../cli.md#schedule) and [API reference](../api.md#schedules-and-queue).
- The [currency rates tutorial](../tutorials/cbu-rates.md) polls an outside API on a schedule.
