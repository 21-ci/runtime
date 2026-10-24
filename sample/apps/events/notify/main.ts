// Registration notifier for the events sample: a second service that the events backend calls over
// RPC. It records a confirmation per registration and returns a receipt number.
// Its `GET /last` shows the caller the runtime stamped (`ctx.caller`), not anything the request claims.
//
//   rpc.confirmRegistration({ eventId, registrationId, name }) -> { receipt: "RC-n" }
//   GET /last                                                   the last confirmation, with its caller

import { kv } from "std:kv";
import { ServiceError } from "std:services";

interface Confirmation {
  eventId: number;
  registrationId: number;
  name: string;
}

interface Caller {
  service: { id: string; slug: string | null };
  project: string | null;
  environment: string | null;
}

export const rpc = {
  async confirmRegistration(args: Confirmation, ctx: { caller?: Caller }) {
    if (
      args === null || typeof args !== "object" ||
      !Number.isSafeInteger(args.eventId) || !Number.isSafeInteger(args.registrationId) ||
      typeof args.name !== "string" || args.name.length === 0 || args.name.length > 200
    ) {
      throw new ServiceError("validation_failed", "eventId, registrationId and a name of 1 to 200 characters are required");
    }
    const n = await kv.incr("receipts", 1);
    const receipt = `RC-${n}`;
    await kv.set("last", {
      receipt,
      eventId: args.eventId,
      registrationId: args.registrationId,
      caller: ctx.caller?.service.slug ?? null,
    });
    return { receipt };
  },
};

export default {
  async fetch(req: Request): Promise<Response> {
    if (new URL(req.url).pathname === "/last") {
      return Response.json((await kv.get("last")) ?? null);
    }
    return new Response("not found", { status: 404 });
  },
};
