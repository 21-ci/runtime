// DNO-04: env var `A` and secret `S` are readable through the read-only `Deno.env` shim; writing
// is not possible. Another service (without `S`) must see `null`.
// The handler never logs the values (SEC-17 canary check in the test).
export default function (): Response {
  // deno-lint-ignore no-explicit-any
  const env = (globalThis as any).Deno.env;
  let setResult = "no-throw";
  try {
    env.set("X", "1");
  } catch (e) {
    setResult = "threw:" + (e as Error).name;
  }
  let deleteResult = "no-throw";
  try {
    env.delete("A");
  } catch (e) {
    deleteResult = "threw:" + (e as Error).name;
  }
  return Response.json({
    a: env.get("A") ?? null,
    s: env.get("S") ?? null,
    x: env.get("X") ?? null,
    setResult,
    deleteResult,
  });
}
