// A tenant service using the operator SDK `acme` (../../operator-sdk), a runtime service.
//
// The build replaces both imports with shims generated from the SDK's published contract: no SDK
// code is in this service. Each call runs in the runtime, in the release this service's grants
// select (21ci sdk ls shows which); a new release, a rollback or a removal reaches it without a
// redeploy. Types for the editor: `21ci sdk types acme -d types` writes types/acme.d.ts.
//
//   GET  /              index
//   GET  /info          acme.getPlatformInfo()
//   GET  /integrations  listIntegrations() (a named export of "sdk:acme")
//   POST /deploy        acme.callService(...)
//   GET  /raw           platform.fetch directly: refused, bindings belong to SDK services
import { acme, SdkError } from "std:sdk";
import { listIntegrations } from "sdk:acme";
import { platform } from "std:platform";

const json = (data: unknown, status = 200) => Response.json(data, { status });

export default async function (req: Request): Promise<Response> {
  const url = new URL(req.url);

  try {
    if (url.pathname === "/") {
      return json({
        paths: ["/info", "/integrations", "POST /deploy", "/raw"],
        note: "needs the acme SDK published and granted (21ci sdk publish / grants set)",
      });
    }

    if (url.pathname === "/info") return json(await acme.getPlatformInfo());

    if (url.pathname === "/integrations") return json({ integrations: await listIntegrations() });

    if (url.pathname === "/deploy" && req.method === "POST") {
      return json(await acme.callService({ name: "my-integration", method: "POST", body: { action: "deploy" } }));
    }

    if (url.pathname === "/raw") {
      // A tenant service never reaches an operator binding itself: 403 binding_not_allowed.
      const res = await platform.fetch("acme", new Request("http://platform/info"));
      return json({ status: res.status, reason: res.headers.get("x-rt-reason") });
    }

    return json({ error: "not found" }, 404);
  } catch (e) {
    // sdk_not_granted, sdk_unavailable (removed), sdk_not_found, or the SDK's own code.
    if (e instanceof SdkError) return json({ error: { code: e.code, message: e.message } }, 502);
    console.error(e);
    return json({ error: (e as Error).message }, 502);
  }
}
