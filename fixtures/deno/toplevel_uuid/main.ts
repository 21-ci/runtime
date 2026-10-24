// DNO-02: top-level code runs once per TASK, so two requests see different UUIDs.
const t = crypto.randomUUID();

export default function (): Response {
  return new Response(t);
}
