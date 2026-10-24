#!/usr/bin/env node
// Mock operator API for the `acme` binding (see ../rt.toml).
// Not part of the runtime: it stands in for "your platform API".
//
//   ACME_INTERNAL_TOKEN=dev-internal-token node server.js [port]     (default port 3000, loopback only)
//
// The runtime injects `authorization` and `x-rt-tenant` (the binding's inject_headers / tenant_header);
// tenant code never sees them. This server therefore requires the token and echoes the tenant.
"use strict";

const http = require("node:http");
const crypto = require("node:crypto");

const PORT = Number(process.argv[2] || process.env.PORT || 3000);
const TOKEN = process.env.ACME_INTERNAL_TOKEN || "dev-internal-token";

function safeEqual(a, b) {
  const x = Buffer.from(a);
  const y = Buffer.from(b);
  return x.length === y.length && crypto.timingSafeEqual(x, y);
}

function send(res, status, body) {
  res.writeHead(status, { "content-type": "application/json" });
  res.end(JSON.stringify(body));
}

function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    req.on("data", (c) => {
      size += c.length;
      if (size > 1_000_000) {
        reject(new Error("body too large"));
        req.destroy();
      } else {
        chunks.push(c);
      }
    });
    req.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
    req.on("error", reject);
  });
}

const server = http.createServer(async (req, res) => {
  const { pathname } = new URL(req.url, "http://localhost");
  const tenant = req.headers["x-rt-tenant"] || null;
  console.log(`${req.method} ${pathname} tenant=${tenant}`);

  // The credential is injected by the runtime from the binding's inject_headers. `env:NAME` injects
  // the variable's value as is, so accept the bare token as well as `Bearer <token>`.
  const auth = req.headers.authorization || "";
  if (!safeEqual(auth, TOKEN) && !safeEqual(auth, `Bearer ${TOKEN}`)) {
    return send(res, 401, { error: "missing or invalid authorization" });
  }

  if (req.method === "GET" && pathname === "/info") {
    return send(res, 200, {
      platform: "acme",
      version: "1.0.0",
      features: ["storage", "api", "webhooks"],
      tenant,
    });
  }

  if (req.method === "GET" && pathname === "/integrations") {
    return send(res, 200, { integrations: ["slack", "github", "stripe", "sendgrid"] });
  }

  const m = pathname.match(/^\/services\/([^/]+)$/);
  if (req.method === "POST" && m) {
    let input = null;
    try {
      const raw = await readBody(req);
      input = raw ? JSON.parse(raw) : null;
    } catch {
      return send(res, 400, { error: "body must be JSON" });
    }
    return send(res, 200, { service: decodeURIComponent(m[1]), action: "executed", input, tenant });
  }

  // Anything else (for example /admin): the runtime should never have forwarded it.
  send(res, 404, { error: "not found" });
});

server.listen(PORT, "127.0.0.1", () => {
  console.log(`mock acme API on http://127.0.0.1:${PORT} (token from ACME_INTERNAL_TOKEN)`);
  console.log("routes: GET /info, GET /integrations, POST /services/{name}");
});
