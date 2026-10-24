/* A tiny stand-in for zod 3 (MockCdn serves it at /zod@3.23.8/es2022/zod.mjs). */
function ok(data) { return { success: true, data }; }
function fail(message) { return { success: false, error: { issues: [{ message }] } }; }
function prim(type) {
  const self = {
    safeParse(v) { return typeof v === type ? ok(v) : fail(`expected ${type}, got ${typeof v}`); },
    parse(v) { const r = self.safeParse(v); if (!r.success) throw new Error(r.error.issues[0].message); return r.data; },
  };
  return self;
}
export const z = {
  string: () => prim("string"),
  number: () => prim("number"),
  boolean: () => prim("boolean"),
  object(shape) {
    const self = {
      safeParse(v) {
        if (v === null || typeof v !== "object") return fail("expected object");
        const out = {};
        for (const [k, s] of Object.entries(shape)) {
          const r = s.safeParse(v[k]);
          if (!r.success) return fail(`${k}: ${r.error.issues[0].message}`);
          out[k] = r.data;
        }
        return ok(out);
      },
      parse(v) { const r = self.safeParse(v); if (!r.success) throw new Error(r.error.issues[0].message); return r.data; },
    };
    return self;
  },
};
export default z;
