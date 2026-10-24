// SEC-22 / DNO-13: clock behaviour as seen by the guest. `?mode=` selects the probe.
//
//   busy     (default) spins a fixed iteration count (never a time-based loop: under the frozen
//            clock that would not terminate) and reports Date.now / performance.now deltas.
//            Frozen clock: both deltas are exactly 0.
//   timer    awaits a `?ms=` timer (default 10): the clock advanced at the timer boundary.
//   samples  samples performance.now() `?n=` times (default 1000) with a little work in between.
//            `security.clock = "coarse"`: every delta is a multiple of 1 ms.

function busy(iters: number): number {
  let s = 0;
  for (let i = 0; i < iters; i++) {
    s += Math.sqrt(i);
  }
  return s;
}

export default async function (req: Request): Promise<Response> {
  const q = new URL(req.url).searchParams;
  const mode = q.get("mode") ?? "busy";

  if (mode === "busy") {
    const iters = Number(q.get("iters") ?? "30000000");
    const d0 = Date.now();
    const p0 = performance.now();
    const sink = busy(iters);
    return Response.json({ dateDelta: Date.now() - d0, perfDelta: performance.now() - p0, sink });
  }

  if (mode === "timer") {
    const ms = Number(q.get("ms") ?? "10");
    const d0 = Date.now();
    const p0 = performance.now();
    await new Promise((r) => setTimeout(r, ms));
    return Response.json({ dateDelta: Date.now() - d0, perfDelta: performance.now() - p0 });
  }

  if (mode === "samples") {
    const n = Number(q.get("n") ?? "1000");
    const samples: number[] = [];
    for (let i = 0; i < n; i++) {
      busy(2000);
      samples.push(performance.now());
    }
    return Response.json({ samples });
  }

  return new Response("unknown mode", { status: 400 });
}
