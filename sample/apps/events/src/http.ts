// HTTP and auth helpers. Web APIs only (no std:* imports), so `node --test` / `deno test` can load it.

const encoder = new TextEncoder();

export type ErrorBody = { error: { code: string; message: string } };

export function json(data: unknown, status = 200, headers: HeadersInit = {}): Response {
  const h = new Headers(headers);
  h.set("content-type", "application/json; charset=utf-8");
  if (!h.has("cache-control")) h.set("cache-control", "no-store");
  return new Response(JSON.stringify(data), { status, headers: h });
}

export function error(status: number, code: string, message: string, headers: HeadersInit = {}): Response {
  return json({ error: { code, message } } satisfies ErrorBody, status, headers);
}

export type JsonBody = { ok: true; value: unknown } | { ok: false; response: Response };

// Reads at most `maxBytes` of the body and parses it as JSON. The runtime has its own body limit
// (`limits.max_body_bytes`); this one is the much smaller limit the endpoint really needs.
// With `optional`, an empty body is `{ok: true, value: null}`.
export async function readJson(req: Request, maxBytes: number, optional = false): Promise<JsonBody> {
  const declared = Number(req.headers.get("content-length") ?? "0");
  if (Number.isFinite(declared) && declared > maxBytes) {
    return { ok: false, response: error(413, "too_large", `body must be at most ${maxBytes} bytes`) };
  }
  const missing = (): JsonBody =>
    optional ? { ok: true, value: null } : { ok: false, response: error(400, "invalid_body", "a JSON body is required") };
  if (!req.body) return missing();

  const reader = req.body.getReader();
  const chunks: Uint8Array[] = [];
  let size = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    size += value.byteLength;
    if (size > maxBytes) {
      await reader.cancel().catch(() => {});
      return { ok: false, response: error(413, "too_large", `body must be at most ${maxBytes} bytes`) };
    }
    chunks.push(value);
  }
  if (size === 0) return missing();
  const bytes = new Uint8Array(size);
  let offset = 0;
  for (const c of chunks) {
    bytes.set(c, offset);
    offset += c.byteLength;
  }
  try {
    return { ok: true, value: JSON.parse(new TextDecoder().decode(bytes)) };
  } catch {
    return { ok: false, response: error(400, "invalid_json", "body is not valid JSON") };
  }
}

export function isObject(v: unknown): v is Record<string, unknown> {
  return typeof v === "object" && v !== null && !Array.isArray(v);
}

// Compares two secrets in time that depends on neither their content nor their length: both are
// hashed to 32 bytes first, then every byte is compared.
export async function safeEqual(a: string, b: string): Promise<boolean> {
  const [da, db] = await Promise.all([
    crypto.subtle.digest("SHA-256", encoder.encode(a)),
    crypto.subtle.digest("SHA-256", encoder.encode(b)),
  ]);
  const x = new Uint8Array(da);
  const y = new Uint8Array(db);
  let diff = 0;
  for (let i = 0; i < x.length; i++) diff |= x[i] ^ y[i];
  return diff === 0;
}

export function bearerToken(req: Request): string | null {
  const h = req.headers.get("authorization");
  if (!h) return null;
  const m = /^Bearer[ \t]+([^\s]+)[ \t]*$/i.exec(h);
  return m ? m[1] : null;
}

export const MIN_SECRET_LENGTH = 16;

// "unconfigured" when the expected secret is missing or too short to be safe.
export async function checkBearer(
  req: Request,
  expected: string | undefined,
): Promise<"ok" | "denied" | "unconfigured"> {
  if (!expected || expected.length < MIN_SECRET_LENGTH) return "unconfigured";
  const given = bearerToken(req);
  if (given === null) return "denied";
  return (await safeEqual(given, expected)) ? "ok" : "denied";
}

// Maps checkBearer's verdict to an error response (null = allowed).
export async function requireAdmin(req: Request, expected: string | undefined): Promise<Response | null> {
  const verdict = await checkBearer(req, expected);
  if (verdict === "ok") return null;
  if (verdict === "unconfigured") {
    return error(503, "admin_disabled", `set the ADMIN_TOKEN secret (at least ${MIN_SECRET_LENGTH} characters)`);
  }
  return error(401, "unauthorized", "admin token required", { "www-authenticate": "Bearer" });
}

// `prefix` + base64url of `bytes` random bytes (32 bytes = 43 characters).
export function randomToken(prefix: string, bytes = 32): string {
  const b = crypto.getRandomValues(new Uint8Array(bytes));
  return prefix + btoa(String.fromCharCode(...b)).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

// Tokens are stored only as their SHA-256, so a copy of the database cannot be used to act on them.
export async function sha256Hex(s: string): Promise<string> {
  const d = new Uint8Array(await crypto.subtle.digest("SHA-256", encoder.encode(s)));
  return Array.from(d, (x) => x.toString(16).padStart(2, "0")).join("");
}

export function errorText(e: unknown): string {
  if (e instanceof Error) {
    const code = (e as { code?: unknown }).code;
    return typeof code === "string" && code !== e.message ? `${code}: ${e.message}` : e.message;
  }
  return String(e);
}
