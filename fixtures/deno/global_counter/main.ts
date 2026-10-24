// DNO-01: module/global state is per task, so every request returns "1".
export default function (): Response {
  const g = globalThis as unknown as { x?: number };
  g.x = (g.x ?? 0) + 1;
  return new Response(String(g.x));
}
