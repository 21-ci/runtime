// JS handler compiled to a WASI P2 component with `jco componentize`.
//
// ComponentizeJS (StarlingMonkey) exposes `wasi:http/incoming-handler` through
// the standard service-worker style `fetch` event: listen for it and answer
// with `event.respondWith(Response | Promise<Response>)`.
//
// This is NOT the Deno executor's `export default function (req, ctx)`
// contract: a WASM service has no `ctx`, no `std:*` / `sdk:*` imports from JS and
// no `Deno.*` APIs. Capabilities come through `std:host/*` WIT interfaces.

// Module-level state is captured at build time (pre-initialization) and every
// task instantiates a fresh copy-on-write image of it, so this is always 1
// under the default profiles (WSM-04), only larger with wasm.reuse_instances.
let hitsInThisInstance = 0;

addEventListener("fetch", (event) => {
  event.respondWith(handle(event.request));
});

async function handle(request) {
  hitsInThisInstance += 1;

  // The runtime strips `/s/{slug}`; the original prefix is in `x-rt-base-path`.
  const url = new URL(request.url);
  const name = url.searchParams.get("name") || "WASM";

  const body = JSON.stringify({
    message: `Hello from WASM component, ${name}!`,
    method: request.method,
    path: url.pathname,
    basePath: request.headers.get("x-rt-base-path"),
    hitsInThisInstance,
    timestamp: new Date().toISOString(),
  });

  return new Response(body, {
    status: 200,
    headers: { "content-type": "application/json" },
  });
}
