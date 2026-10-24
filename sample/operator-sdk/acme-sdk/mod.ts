// Operator SDK `acme`: a runtime service of the system tenant (docs/api.md,
// "Operator SDKs"). Published with
//
//   21ci sdk publish acme 1.0.0 sample/operator-sdk/acme-sdk
//   21ci sdk grants set node acme --allow
//
// Tenants never get this code. Their build sees the contract only (`import { acme } from "std:sdk"`
// or `import acme from "sdk:acme"`), and every call runs here, in the release their grants select.
// A new release, a rollback or a removal reaches them without a rebuild or a redeploy.
//
// The SDK reaches the operator's API through the `acme` binding (../rt.toml): the runtime injects
// the credentials, and the tenant header is the *calling* tenant. Bindings are reachable only from
// SDK services like this one. Every method authorizes by `ctx.caller`, the caller the runtime stamped:
// defineSdk refuses a method that never reads it.

import { defineSdk } from "std:sdk/server";
import { platform } from "std:platform";

const BINDING = "acme";

// 1.1.0 also lists the integrations in getPlatformInfo (sample/apps/edge-gateway/smoke.sh publishes
// 1.1.0 from this file with the flag on, to show an SDK update reaching a running service).
const INFO_WITH_INTEGRATIONS = false;

export interface Caller {
  tenant: string;
  project: string | null;
  environment: string | null;
  service: { id: string; slug: string | null };
  plan?: string;
  sdk?: { name: string; version: string };
}

export interface PlatformInfo {
  platform: string;
  version: string;
  features: string[];
  tenant: string;
  /** The SDK release that answered. */
  sdk: string;
  /** 1.1.0 and later. */
  integrations?: string[];
}

export interface AuditEvent {
  id: string;
  at: number;
  method: string;
  path: string;
  status: number;
  [field: string]: unknown;
}

export interface ShipReceipt {
  accepted: number;
  sdk: string;
}

export interface ServiceCall {
  name: string;
  method?: "GET" | "POST";
  body?: unknown;
}

interface Ctx {
  caller: Caller;
}

// The host in the URL is a placeholder: platform.fetch re-targets the request to the binding's
// base_url. Only the method, path, query, non-credential headers and body are used.
async function api<T>(path: string, init?: RequestInit): Promise<T> {
  const res = await platform.fetch(BINDING, new Request(`http://platform${path}`, init));
  if (!res.ok) {
    const e = new Error(`acme ${init?.method ?? "GET"} ${path}: ${res.headers.get("x-rt-reason") ?? `HTTP ${res.status}`}`);
    (e as { code?: string }).code = res.headers.get("x-rt-reason") ?? "acme_upstream";
    throw e;
  }
  return await res.json() as T;
}

function requireTenant(caller: Caller | undefined): Caller {
  if (!caller?.tenant) {
    const e = new Error("no caller");
    (e as { code?: string }).code = "forbidden";
    throw e;
  }
  return caller;
}

export default defineSdk({
  methods: {
    async getPlatformInfo(_args: void, ctx: Ctx): Promise<PlatformInfo> {
      const caller = requireTenant(ctx.caller);
      const info = await api<Omit<PlatformInfo, "sdk" | "tenant">>("/info");
      const out: PlatformInfo = { ...info, tenant: caller.tenant, sdk: caller.sdk?.version ?? "?" };
      if (INFO_WITH_INTEGRATIONS) {
        out.integrations = (await api<{ integrations?: string[] }>("/integrations")).integrations ?? [];
      }
      return out;
    },

    async listIntegrations(_args: void, ctx: Ctx): Promise<string[]> {
      requireTenant(ctx.caller);
      return (await api<{ integrations?: string[] }>("/integrations")).integrations ?? [];
    },

    async callService(args: ServiceCall, ctx: Ctx): Promise<unknown> {
      requireTenant(ctx.caller);
      if (!/^[A-Za-z0-9._-]{1,64}$/.test(args?.name ?? "")) {
        const e = new Error("name: 1-64 of A-Z a-z 0-9 . _ -");
        (e as { code?: string }).code = "validation_failed";
        throw e;
      }
      return await api(`/services/${args.name}`, {
        method: args.method ?? "POST",
        headers: args.body === undefined ? undefined : { "content-type": "application/json" },
        body: args.body === undefined ? undefined : JSON.stringify(args.body),
      });
    },

    audit: {
      // Ships audit events to the operator's audit log. The tenant is the caller's, never a field
      // of the events: one tenant cannot write into another's log.
      async ship(args: { source: string; events: AuditEvent[] }, ctx: Ctx): Promise<ShipReceipt> {
        const caller = requireTenant(ctx.caller);
        const events = Array.isArray(args?.events) ? args.events.slice(0, 500) : [];
        await api("/services/audit-log", {
          method: "POST",
          headers: { "content-type": "application/json" },
          body: JSON.stringify({ source: String(args?.source ?? caller.service.slug ?? "unknown"), events }),
        });
        return { accepted: events.length, sdk: caller.sdk?.version ?? "?" };
      },
    },
  },
});
