// The service's own HTML pages: take a ticket, my ticket, a public screen, admin list and board.
//
// Security model: pages are static. The only server-side input is a JSON config ({base, qid}, both
// validated) in a non-executable <script type="application/json">. Every queue or person name is
// loaded with fetch and written with textContent. Scripts and styles carry a per-response CSP nonce;
// inline handlers and style attributes are blocked by that CSP.

export interface PageConfig {
  base: string; // "/s/live-queue" when reached through /s/{slug}, "" on a custom host
  qid?: string;
}

// x-rt-base-path is set by the runtime; accept only the shape it can have.
export function sanitizeBase(h: string | null): string {
  return h !== null && /^\/s\/[a-z][a-z0-9-]{1,62}$/.test(h) ? h : "";
}

export function newNonce(): string {
  return btoa(String.fromCharCode(...crypto.getRandomValues(new Uint8Array(16))));
}

export function csp(nonce: string): string {
  return [
    "default-src 'none'",
    `script-src 'nonce-${nonce}'`,
    `style-src 'nonce-${nonce}'`,
    "connect-src 'self'",
    "img-src 'self' data:",
    "base-uri 'none'",
    "form-action 'none'",
    "frame-ancestors 'none'",
  ].join("; ");
}

const LS = String.fromCharCode(0x2028);
const PS = String.fromCharCode(0x2029);

// JSON that cannot close the <script> element it sits in.
export function scriptJson(v: unknown): string {
  // U+2028 and U+2029 come from char codes: typed into a regex literal they act as line breaks.
  return JSON.stringify(v).replace(/</g, "\\u003c").split(LS).join("\\u2028").split(PS).join("\\u2029");
}

const CSS = `
:root{color-scheme:light dark;--bg:#f5f6f8;--fg:#15181b;--muted:#5d6670;--card:#fff;--line:#e2e5e9;--accent:#0b63c4;--accent-fg:#fff;--ok:#16794a;--bad:#c22a2a}
@media (prefers-color-scheme:dark){:root{--bg:#0e1113;--fg:#e7e9ec;--muted:#9aa3ad;--card:#171b1e;--line:#2a3035;--accent:#4f9be6;--accent-fg:#06101a}}
*{box-sizing:border-box}
body{margin:0;font:16px/1.45 system-ui,-apple-system,"Segoe UI",Roboto,sans-serif;background:var(--bg);color:var(--fg)}
main{max-width:640px;margin:0 auto;padding:16px}
main.wide{max-width:none;padding:24px 32px}
h1{font-size:1.45rem;margin:4px 0 8px}
h2{font-size:1.05rem;margin:0 0 8px}
a{color:var(--accent)}
.muted{color:var(--muted)}
.error{color:var(--bad)}
.center{text-align:center}
.card{background:var(--card);border:1px solid var(--line);border-radius:14px;padding:16px;margin:12px 0}
.card.called{background:var(--ok);color:#fff;border-color:transparent}
.card.called .muted{color:#e8fff2}
.big{font-size:clamp(72px,24vw,150px);font-weight:800;letter-spacing:-2px;text-align:center;line-height:1;margin:8px 0;font-variant-numeric:tabular-nums}
.state{text-align:center;font-size:1.2rem;font-weight:600;margin:8px 0 0}
.flash{animation:flash .6s ease-in-out 5}
@keyframes flash{50%{transform:scale(1.03);filter:brightness(1.3)}}
button,input{font:inherit}
button{min-height:48px;padding:0 18px;border-radius:12px;border:1px solid var(--line);background:var(--card);color:var(--fg);cursor:pointer}
button.primary{background:var(--accent);color:var(--accent-fg);border-color:transparent;font-weight:600;width:100%}
button.small{min-height:36px;padding:0 12px}
button:disabled{opacity:.5;cursor:default}
input{min-height:48px;padding:0 12px;border-radius:12px;border:1px solid var(--line);background:var(--bg);color:var(--fg);width:100%}
label{display:block;margin:0 0 6px}
.row{display:flex;gap:8px;align-items:center;flex-wrap:wrap}
.row>*{flex:1 1 140px}
.gap{height:10px}
.stat{display:flex;justify-content:space-between;gap:12px;padding:7px 0;border-bottom:1px solid var(--line)}
.stat:last-child{border-bottom:0}
.badge{display:inline-block;padding:2px 10px;border-radius:999px;font-size:.85rem;border:1px solid var(--line);white-space:nowrap}
ul.list{list-style:none;margin:0;padding:0}
ul.list li{display:flex;gap:8px;align-items:center;justify-content:space-between;padding:8px 0;border-bottom:1px solid var(--line)}
ul.list li:last-child{border-bottom:0}
.num{font-weight:700;font-variant-numeric:tabular-nums;min-width:3.5ch}
.grow{flex:1;overflow:hidden;text-overflow:ellipsis;white-space:nowrap}
.serving{display:grid;grid-template-columns:repeat(auto-fill,minmax(110px,1fr));gap:8px}
.serving div{background:var(--bg);border:1px solid var(--line);border-radius:12px;text-align:center;padding:10px}
.serving b{display:block;font-size:2rem;font-variant-numeric:tabular-nums}
.screen .serving{grid-template-columns:repeat(auto-fill,minmax(260px,1fr));gap:16px}
.screen .serving b{font-size:clamp(64px,9vw,140px)}
.screen h1{font-size:clamp(28px,4vw,56px)}
`;

// Shared client code. `follow` is the long-poll loop: each request carries the last seen version
// and the server answers when the queue changes (or after LONGPOLL_SECONDS).
const COMMON_JS = `
"use strict";
const CFG = JSON.parse(document.getElementById("cfg").textContent);
const $ = (id) => document.getElementById(id);
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
function el(tag, props, ...kids) {
  const e = document.createElement(tag);
  for (const [k, v] of Object.entries(props || {})) {
    if (k === "class") e.className = v;
    else if (k === "text") e.textContent = v;
    else if (k.startsWith("on")) e.addEventListener(k.slice(2), v);
    else e.setAttribute(k, v);
  }
  for (const c of kids) e.append(c);
  return e;
}
async function api(path, opts) {
  const o = opts || {};
  const headers = Object.assign(o.body ? { "content-type": "application/json" } : {}, o.headers || {});
  const res = await fetch(CFG.base + path, { method: o.method || "GET", headers, body: o.body ? JSON.stringify(o.body) : undefined, cache: "no-store" });
  let data = null;
  try { data = await res.json(); } catch (_) {}
  return { status: res.status, data };
}
const errMsg = (r) => (r.data && r.data.error && r.data.error.message) || ("HTTP " + r.status);
function setStatus(t) { const s = $("status"); if (s) s.textContent = t || ""; }
function fmtEta(s) {
  if (s === null || s === undefined) return "-";
  if (s < 60) return "less than a minute";
  const m = Math.round(s / 60);
  if (m < 60) return "about " + m + " min";
  return "about " + Math.floor(m / 60) + " h " + (m % 60) + " min";
}
const fmtTime = (ms) => new Date(ms).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
function servingTiles(list, box) {
  box.replaceChildren(...(list.length ? list.map((s) => el("div", null, el("b", { text: String(s.number) }), el("span", { class: "muted", text: s.counter }))) : [el("span", { class: "muted", text: "Nobody yet" })]));
}
async function follow(fetchOnce) {
  let since = 0, backoff = 0;
  for (;;) {
    const started = Date.now();
    let next;
    try {
      next = await fetchOnce(since);
      backoff = 0;
    } catch (_) {
      backoff = Math.min(30000, backoff ? backoff * 2 : 2000);
      setStatus("Connection lost, retrying...");
      await sleep(backoff);
      continue;
    }
    if (next === null) return;
    if (next === since && Date.now() - started < 1000) await sleep(3000); // server without long-polling
    since = next;
  }
}
`;

function layout(title: string, nonce: string, cfg: PageConfig, body: string, script: string): string {
  return `<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="referrer" content="no-referrer">
<title>${title}</title>
<style nonce="${nonce}">${CSS}</style>
</head>
<body>
${body}
<script type="application/json" id="cfg">${scriptJson(cfg)}</script>
<script nonce="${nonce}">${COMMON_JS}
${script}</script>
</body>
</html>`;
}

// ---------------------------------------------------------------------------------- pages

export function landingPage(nonce: string, cfg: PageConfig): string {
  return layout("Live queue", nonce, cfg, `
<main>
  <h1>Live queue</h1>
  <p class="muted">An electronic queue: take a ticket on your phone, see your place and wait time, get called.</p>
  <div class="card">
    <p>Have a queue link or code? Open <b>…/q/&lt;code&gt;</b>, or enter the code:</p>
    <div class="row"><input id="code" maxlength="10" autocomplete="off" placeholder="code"><button id="go">Open</button></div>
  </div>
  <p><a id="admin">Admin</a></p>
</main>`, `
$("admin").href = CFG.base + "/admin";
$("go").addEventListener("click", () => {
  const c = $("code").value.trim().toLowerCase();
  if (/^[a-z0-9]{10}$/.test(c)) location.href = CFG.base + "/q/" + c;
});
`);
}

export function takePage(nonce: string, cfg: PageConfig): string {
  return layout("Take a ticket", nonce, cfg, `
<main>
  <p class="muted">Electronic queue</p>
  <h1 id="name">Loading...</h1>
  <div class="card">
    <div class="stat"><span>Waiting now</span><b id="waiting">-</b></div>
    <div class="stat"><span>Wait for a new ticket</span><b id="eta">-</b></div>
  </div>
  <div class="card"><h2>Now serving</h2><div class="serving" id="serving"></div></div>
  <div class="card">
    <label for="person" class="muted">Your name (optional, shown to staff)</label>
    <input id="person" maxlength="40" autocomplete="given-name">
    <div class="gap"></div>
    <button class="primary" id="take" disabled>Take a ticket</button>
    <p id="mine"></p>
  </div>
  <p id="status" class="error"></p>
</main>`, `
const KEY = "lq:" + CFG.qid;
const ticketUrl = (n) => CFG.base + "/q/" + CFG.qid + "/ticket?n=" + n;
let mine = null;
try { mine = JSON.parse(localStorage.getItem(KEY) || "null"); } catch (_) {}
if (mine) $("mine").append(el("a", { href: ticketUrl(mine.n), text: "Show my ticket #" + mine.n }));
$("take").addEventListener("click", async () => {
  $("take").disabled = true;
  setStatus("");
  try {
    const r = await api("/api/queues/" + CFG.qid + "/tickets", { method: "POST", body: { name: $("person").value } });
    if (r.status === 201) {
      try { localStorage.setItem(KEY, JSON.stringify({ n: r.data.ticket.number, token: r.data.token })); } catch (_) {}
      // The fragment keeps the ticket usable even where localStorage is off; it is never sent to the server.
      location.href = ticketUrl(r.data.ticket.number) + "#" + encodeURIComponent(r.data.token);
      return;
    }
    setStatus(errMsg(r));
  } catch (_) {
    setStatus("Network error, try again.");
  }
  $("take").disabled = false;
});
follow(async (since) => {
  const r = await api("/api/queues/" + CFG.qid + "?since=" + since);
  if (r.status === 404) { $("name").textContent = "There is no such queue."; return null; }
  if (r.status !== 200) throw new Error(errMsg(r));
  const q = r.data;
  $("name").textContent = q.name;
  document.title = q.name;
  $("waiting").textContent = String(q.waiting);
  $("eta").textContent = fmtEta(q.etaForNewTicket);
  servingTiles(q.nowServing, $("serving"));
  $("take").disabled = q.status !== "open";
  $("take").textContent = q.status === "open" ? "Take a ticket" : "Not taking new tickets right now";
  setStatus("");
  return q.version;
});
`);
}

export function ticketPage(nonce: string, cfg: PageConfig): string {
  return layout("My ticket", nonce, cfg, `
<main>
  <p class="muted" id="qname">Electronic queue</p>
  <div class="card" id="card">
    <p class="muted center">Your number</p>
    <div class="big" id="number">-</div>
    <p class="state" id="state">Loading...</p>
  </div>
  <div class="card">
    <div class="stat"><span>People ahead of you</span><b id="ahead">-</b></div>
    <div class="stat"><span>Estimated wait</span><b id="eta">-</b></div>
    <div class="stat"><span>Taken at</span><b id="taken">-</b></div>
  </div>
  <div class="card"><h2>Now serving</h2><div class="serving" id="serving"></div></div>
  <div class="row"><button id="notify">Notify me</button><button id="leave">Leave the queue</button></div>
  <p class="muted">Keep this page open. It updates by itself and vibrates when it is your turn.</p>
  <p id="status" class="error"></p>
</main>`, `
const KEY = "lq:" + CFG.qid;
const n = Number(new URLSearchParams(location.search).get("n"));
let mine = null;
try { mine = JSON.parse(localStorage.getItem(KEY) || "null"); } catch (_) {}
if (location.hash.length > 1) {
  mine = { n, token: decodeURIComponent(location.hash.slice(1)) };
  try { localStorage.setItem(KEY, JSON.stringify(mine)); } catch (_) {}
  history.replaceState(null, "", location.pathname + location.search);
}
const headers = mine && mine.n === n ? { "x-ticket-token": mine.token } : null;
let last = null;
function finish(text) {
  $("state").textContent = text;
  $("leave").disabled = true;
  $("notify").disabled = true;
}
function alertTurn(t) {
  $("card").classList.remove("flash");
  void $("card").offsetWidth;
  $("card").classList.add("flash");
  if (navigator.vibrate) navigator.vibrate([400, 150, 400, 150, 800]);
  if ("Notification" in window && Notification.permission === "granted" && document.hidden) {
    new Notification("Your turn: #" + t.number, { body: "Go to " + t.counter });
  }
}
function render(v) {
  const t = v.ticket;
  $("number").textContent = String(t.number);
  $("qname").textContent = v.queue.name;
  $("taken").textContent = fmtTime(t.createdAt);
  servingTiles(v.queue.nowServing, $("serving"));
  $("card").classList.toggle("called", t.status === "called");
  if (t.status === "waiting") {
    $("state").textContent = "Please wait";
    $("ahead").textContent = String(v.ahead);
    $("eta").textContent = fmtEta(v.etaSeconds);
  } else if (t.status === "called") {
    $("state").textContent = "It is your turn: go to " + t.counter;
    $("ahead").textContent = "0";
    $("eta").textContent = "now";
    if (last !== "called") alertTurn(t);
  } else {
    $("ahead").textContent = "-";
    $("eta").textContent = "-";
    finish(t.status === "done" ? "Served. Thank you!" : t.status === "skipped"
      ? "Your number was skipped. Take a new ticket if you still need service." : "You left the queue.");
    try { localStorage.removeItem(KEY); } catch (_) {}
  }
  document.title = (t.status === "called" ? "YOUR TURN - " : "#" + t.number + " - ") + v.queue.name;
  last = t.status;
}
$("notify").addEventListener("click", async () => {
  if (!("Notification" in window)) { $("notify").textContent = "Not supported"; return; }
  const p = await Notification.requestPermission();
  $("notify").textContent = p === "granted" ? "Notifications on" : "Notifications blocked";
});
$("leave").addEventListener("click", async () => {
  if (!confirm("Leave the queue? Your number will be cancelled.")) return;
  const r = await api("/api/queues/" + CFG.qid + "/tickets/" + n, { method: "DELETE", headers });
  if (r.status === 200) render(r.data); else setStatus(errMsg(r));
});
if (!headers) {
  $("number").textContent = Number.isInteger(n) && n > 0 ? String(n) : "-";
  finish("This ticket is not on this device.");
} else {
  follow(async (since) => {
    const r = await api("/api/queues/" + CFG.qid + "/tickets/" + n + "?since=" + since, { headers });
    if (r.status === 401 || r.status === 404) { finish("Ticket not found."); return null; }
    if (r.status !== 200) throw new Error(errMsg(r));
    setStatus("");
    render(r.data);
    return ["waiting", "called"].includes(r.data.ticket.status) ? r.data.queue.version : null;
  });
}
`);
}

export function screenPage(nonce: string, cfg: PageConfig): string {
  return layout("Now serving", nonce, cfg, `
<main class="wide screen">
  <h1 id="name">Loading...</h1>
  <p class="muted">Take a ticket: <b id="link"></b> &middot; waiting: <b id="waiting">-</b></p>
  <div class="serving" id="serving"></div>
  <p id="status" class="error"></p>
</main>`, `
$("link").textContent = location.origin + CFG.base + "/q/" + CFG.qid;
follow(async (since) => {
  const r = await api("/api/queues/" + CFG.qid + "?since=" + since);
  if (r.status === 404) { $("name").textContent = "There is no such queue."; return null; }
  if (r.status !== 200) throw new Error(errMsg(r));
  $("name").textContent = r.data.name;
  $("waiting").textContent = String(r.data.waiting);
  servingTiles(r.data.nowServing, $("serving"));
  setStatus("");
  return r.data.version;
});
`);
}

// Admin token lives in sessionStorage (this tab only) and is sent as a Bearer header.
const ADMIN_JS = `
const TOKEN_KEY = "lq_admin";
function adminToken() { try { return sessionStorage.getItem(TOKEN_KEY) || ""; } catch (_) { return ""; } }
const auth = () => ({ authorization: "Bearer " + adminToken() });
function signIn(onDone) {
  $("login").hidden = false;
  $("app").hidden = true;
  $("save").onclick = () => {
    try { sessionStorage.setItem(TOKEN_KEY, $("token").value.trim()); } catch (_) {}
    onDone();
  };
}
`;

export function adminPage(nonce: string, cfg: PageConfig): string {
  return layout("Queue admin", nonce, cfg, `
<main>
  <h1>Queue admin</h1>
  <div class="card" id="login" hidden>
    <label for="token">Admin token</label>
    <input id="token" type="password" autocomplete="current-password">
    <div class="gap"></div>
    <button class="primary" id="save">Sign in</button>
  </div>
  <div id="app" hidden>
    <div class="card">
      <h2>Open a new queue</h2>
      <label for="qname" class="muted">Name</label>
      <input id="qname" maxlength="80" placeholder="Passport office, window 3">
      <div class="gap"></div>
      <label for="mpp" class="muted">Minutes per person (first estimate)</label>
      <input id="mpp" type="number" min="0.1" max="240" step="0.5" value="3">
      <div class="gap"></div>
      <button class="primary" id="create">Open queue</button>
    </div>
    <div class="card"><h2>Queues</h2><ul class="list" id="queues"></ul></div>
    <button id="logout">Sign out</button>
  </div>
  <p id="status" class="error"></p>
</main>`, ADMIN_JS + `
async function load() {
  const r = await api("/api/queues", { headers: auth() });
  if (r.status === 401 || r.status === 503) { setStatus(adminToken() ? errMsg(r) : ""); signIn(load); return; }
  if (r.status !== 200) { setStatus(errMsg(r)); return; }
  setStatus("");
  $("login").hidden = true;
  $("app").hidden = false;
  const ul = $("queues");
  ul.replaceChildren(...r.data.queues.map((q) => el("li", null,
    el("a", { class: "grow", href: CFG.base + "/admin/q/" + q.id, text: q.name }),
    el("span", { class: "badge", text: q.status + ", " + q.waiting + " waiting" }))));
  if (!r.data.queues.length) ul.append(el("li", { class: "muted", text: "No queues yet." }));
}
$("create").addEventListener("click", async () => {
  const r = await api("/api/queues", { method: "POST", headers: auth(), body: { name: $("qname").value, minutesPerPerson: Number($("mpp").value) } });
  if (r.status === 201) location.href = CFG.base + "/admin/q/" + r.data.queue.id;
  else setStatus(errMsg(r));
});
$("logout").addEventListener("click", () => { try { sessionStorage.removeItem(TOKEN_KEY); } catch (_) {} location.reload(); });
load();
`);
}

export function boardPage(nonce: string, cfg: PageConfig): string {
  return layout("Queue board", nonce, cfg, `
<main>
  <p><a id="back">All queues</a></p>
  <div class="card" id="login" hidden>
    <label for="token">Admin token</label>
    <input id="token" type="password" autocomplete="current-password">
    <div class="gap"></div>
    <button class="primary" id="save">Sign in</button>
  </div>
  <div id="app" hidden>
    <h1 id="name">Loading...</h1>
    <p class="muted">People: <a id="public" target="_blank" rel="noopener"></a> &middot; <a id="screen" target="_blank" rel="noopener">screen</a></p>
    <div class="card">
      <div class="row">
        <div><label for="counter" class="muted">Counter</label><input id="counter" maxlength="20" value="1"></div>
        <div><label for="count" class="muted">People to call</label><input id="count" type="number" min="1" max="20" value="1"></div>
      </div>
      <div class="gap"></div>
      <button class="primary" id="call">Call next</button>
      <div class="gap"></div>
      <button id="pause">Pause new tickets</button>
    </div>
    <div class="card">
      <div class="stat"><span>Status</span><b id="qstatus">-</b></div>
      <div class="stat"><span>Waiting</span><b id="waiting">-</b></div>
      <div class="stat"><span>Time per person</span><b id="spp">-</b></div>
      <div class="stat"><span>Served / skipped (24 h)</span><b id="served">-</b></div>
    </div>
    <div class="card"><h2>Called</h2><ul class="list" id="called"></ul></div>
    <div class="card"><h2>Waiting</h2><ul class="list" id="waitlist"></ul></div>
  </div>
  <p id="status" class="error"></p>
</main>`, ADMIN_JS + `
const Q = "/api/queues/" + CFG.qid;
const COUNTER_KEY = "lq_counter:" + CFG.qid;
$("back").href = CFG.base + "/admin";
$("public").href = $("public").textContent = location.origin + CFG.base + "/q/" + CFG.qid;
$("screen").href = CFG.base + "/q/" + CFG.qid + "/screen";
try { $("counter").value = localStorage.getItem(COUNTER_KEY) || "1"; } catch (_) {}
let board = null;
function ticketItem(t, actions) {
  return el("li", null,
    el("span", { class: "num", text: "#" + t.number }),
    el("span", { class: "grow", text: (t.name || "") + (t.counter ? " (" + t.counter + ")" : "") + " " + fmtTime(t.calledAt || t.createdAt) }),
    ...actions.map(([label, action]) => el("button", { class: "small", text: label, onclick: () => act(Q + "/tickets/" + t.number + "/" + action) })));
}
function render(b) {
  board = b;
  const q = b.queue;
  $("name").textContent = q.name;
  document.title = q.name + " - board";
  $("qstatus").textContent = q.status;
  $("waiting").textContent = String(q.waiting);
  $("spp").textContent = fmtEta(q.secondsPerPerson);
  $("served").textContent = b.stats.served + " / " + b.stats.skipped;
  $("pause").textContent = q.status === "open" ? "Pause new tickets" : "Resume new tickets";
  $("call").disabled = q.waiting === 0;
  $("called").replaceChildren(...b.called.map((t) => ticketItem(t, [["Done", "done"], ["No-show", "skip"]])));
  if (!b.called.length) $("called").append(el("li", { class: "muted", text: "Nobody called" }));
  $("waitlist").replaceChildren(...b.waitingList.map((t) => ticketItem(t, [["Skip", "skip"]])));
  if (!b.waitingList.length) $("waitlist").append(el("li", { class: "muted", text: "Nobody waiting" }));
}
async function act(path, body) {
  const r = await api(path, { method: "POST", headers: auth(), body: body || {} });
  if (r.status === 200) {
    render(r.data);
    const now = r.data.calledNow;
    setStatus(now ? (now.length ? "Called #" + now.join(", #") : "Nobody is waiting") : "");
  }
  else setStatus(errMsg(r));
}
$("call").addEventListener("click", () => {
  const counter = $("counter").value.trim() || "1";
  try { localStorage.setItem(COUNTER_KEY, counter); } catch (_) {}
  act(Q + "/call-next", { count: Number($("count").value) || 1, counter });
});
$("pause").addEventListener("click", () => act(Q + "/" + (board && board.queue.status === "open" ? "pause" : "resume")));
function start() {
  follow(async (since) => {
    const r = await api(Q + "/board?since=" + since, { headers: auth() });
    if (r.status === 401 || r.status === 503) { setStatus(adminToken() ? errMsg(r) : ""); signIn(start); return null; }
    if (r.status === 404) { setStatus("There is no such queue."); return null; }
    if (r.status !== 200) throw new Error(errMsg(r));
    $("login").hidden = true;
    $("app").hidden = false;
    setStatus("");
    render(r.data);
    return r.data.queue.version;
  });
}
start();
`);
}
