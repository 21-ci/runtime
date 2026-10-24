// DNO-03 / SEC-23: probes every API a tenant must not reach. The handler never fails; it reports,
// per probe, one of:
//   "undefined"        the global / member does not exist
//   "threw:<message>"  it exists but using it throws
//   "allowed:<type>"   it worked (a sandbox escape: the test fails)
//
// Dynamic imports use **non-literal** specifiers on purpose: a literal `import("node:fs")` would
// be rejected at build time, while the runtime guarantee under test is that a specifier outside the
// bundle fails with `module_not_in_bundle` when it is evaluated.

const UNDEF = Symbol("undefined");
// deno-lint-ignore no-explicit-any
const g = globalThis as any;

type Probe = () => unknown | Promise<unknown>;

const probes: Record<string, Probe> = {
  "Deno.readFile": () => (typeof g.Deno?.readFile === "function" ? g.Deno.readFile("/etc/passwd") : UNDEF),
  "Deno.connect": () =>
    typeof g.Deno?.connect === "function" ? g.Deno.connect({ hostname: "127.0.0.1", port: 1 }) : UNDEF,
  "Deno.Command": () =>
    typeof g.Deno?.Command === "function" ? new g.Deno.Command("sh", { args: ["-c", "id"] }).output() : UNDEF,
  "Deno.run": () => (typeof g.Deno?.run === "function" ? g.Deno.run({ cmd: ["id"] }) : UNDEF),
  "Deno.openKv": () => (typeof g.Deno?.openKv === "function" ? g.Deno.openKv() : UNDEF),
  "Worker": () => (typeof g.Worker === "undefined" ? UNDEF : new g.Worker("data:text/javascript,1")),
  "SharedArrayBuffer": () => (typeof g.SharedArrayBuffer === "undefined" ? UNDEF : new g.SharedArrayBuffer(8)),
  "Atomics.wait": () =>
    typeof g.Atomics?.wait !== "function" ? UNDEF : g.Atomics.wait(new Int32Array(new ArrayBuffer(16)), 0, 0, 1),
  "ShadowRealm": () => (typeof g.ShadowRealm === "undefined" ? UNDEF : new g.ShadowRealm()),
  // Outbound WebSockets exist but go through the egress guard: loopback is refused.
  "WebSocket(loopback)": () =>
    typeof g.WebSocket === "undefined"
      ? UNDEF
      : new Promise((resolve, reject) => {
          const ws = new g.WebSocket("ws://127.0.0.1:1");
          ws.onopen = () => resolve(ws);
          ws.onerror = (e: { code?: string }) => reject(new Error(e.code ?? "error"));
        }),
  "caches": () => (typeof g.caches === "undefined" ? UNDEF : g.caches.open("x")),
  // Runtime imports of anything that is not in the bundle.
  "import(node:fs)": () => import(["node", "fs"].join(":")),
  "import(https)": () => import(["https:", "", "example.com", "x.js"].join("/")),
  "import(file)": () => import(["file:", "", "", "etc", "passwd"].join("/")),
  // `import.meta` must not give a way out of the bundle either.
  "import.meta.resolve+import": () => {
    const meta = import.meta as unknown as { resolve?: (s: string) => string };
    const target = typeof meta.resolve === "function" ? meta.resolve("./__not_in_bundle__.js") : "./__not_in_bundle__.js";
    return import(target);
  },
};

async function run(name: string, f: Probe): Promise<[string, string]> {
  try {
    const v = await f();
    if (v === UNDEF) return [name, "undefined"];
    return [name, "allowed:" + typeof v];
  } catch (e) {
    return [name, "threw:" + String((e as Error)?.message ?? e)];
  }
}

export default async function (): Promise<Response> {
  const results: Record<string, string> = {};
  for (const [name, f] of Object.entries(probes)) {
    const [n, r] = await run(name, f);
    results[n] = r;
  }
  return Response.json({
    probes: results,
    // Only the read-only `Deno.env` shim may exist on `Deno`.
    denoKeys: typeof g.Deno === "undefined" ? [] : Object.getOwnPropertyNames(g.Deno).sort(),
    metaUrl: import.meta.url,
  });
}
