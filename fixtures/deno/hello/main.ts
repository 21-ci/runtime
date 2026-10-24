// Form A, empty-ish handler: returns "hello". Used by DNO-10/14/16 and most build tests.
export default function (_req: Request): Response {
  return new Response("hello");
}
