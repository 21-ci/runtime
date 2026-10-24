// Multi-file bundle (zip/tar/json deploy): relative imports, a JSON module, an import-map alias,
// a literal dynamic import (included in the bundle) and a file nothing imports (excluded).
import { greet } from "greet";
import { upper } from "./lib/util.ts";
import data from "./data.json" with { type: "json" };

export default async function (req: Request): Promise<Response> {
  const name = new URL(req.url).searchParams.get("name") ?? data.defaultName;
  const lazy = await import("./lib/lazy.ts");
  return Response.json({ message: upper(greet(name)), lazy: lazy.LAZY_MARKER });
}
