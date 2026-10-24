// The WebSocket half of sample/apps/chat-room/smoke.sh (Node 22+: global WebSocket and fetch).
// Prints "  ok    ..." / "  FAIL  ..." lines like the shell smokes; exits 1 when anything failed.
//
//   RT_API=… RT_INVOKE=… RT_KEY=… GATE=chat ROOMS=chat-rooms node test/chat.mjs <rooms.ts>
import { readFileSync } from "node:fs";

const [roomsFile] = process.argv.slice(2);
const { RT_API, RT_INVOKE, RT_KEY, GATE, ROOMS } = process.env;
let failed = 0;
const ok = (m) => console.log(`  ok    ${m}`);
const fail = (m) => { failed++; console.log(`  FAIL  ${m}`); };
const check = (cond, m) => (cond ? ok(m) : fail(m));
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
const api = (method, path, body, type = "application/json") =>
  fetch(RT_API + path, { method, headers: { authorization: `Bearer ${RT_KEY}`, "content-type": type }, body });

class Client {
  constructor(room, name) {
    this.name = name;
    this.inbox = [];
    this.waiters = [];
    this.closed = null;
    this.ws = new WebSocket(`${RT_INVOKE.replace(/^http/, "ws")}/s/${GATE}/rooms/${room}/ws?name=${name}`);
    this.ws.onmessage = (e) => this.push(e.data === "pong" ? { type: "pong" } : JSON.parse(e.data));
    this.ws.onclose = (e) => { this.closed = e.code; this.push({ type: "closed", code: e.code }); };
    this.opened = new Promise((res, rej) => { this.ws.onopen = res; this.ws.onerror = () => rej(new Error("connect failed")); });
  }
  push(m) { this.inbox.push(m); this.waiters.splice(0).forEach((w) => w()); }
  async next(type, ms = 10000) {
    const end = Date.now() + ms;
    for (;;) {
      const i = this.inbox.findIndex((m) => m.type === type);
      if (i >= 0) return this.inbox.splice(i, 1)[0];
      if (Date.now() > end) return null;
      await Promise.race([new Promise((r) => this.waiters.push(r)), sleep(200)]);
    }
  }
  say(text) { this.ws.send(JSON.stringify({ text })); }
}

async function serviceId(slug) {
  const r = await (await api("GET", `/v1/services?slug=${slug}`)).json();
  return r.items[0].id;
}

async function redeploy(version, closeSockets) {
  const id = await serviceId(ROOMS);
  const src = readFileSync(roomsFile, "utf8").replace('const VERSION = "v1"', `const VERSION = "${version}"`);
  const q = `activate=1&compile=eager${closeSockets ? "&close_sockets=1" : ""}`;
  const r = await api("POST", `/v1/services/${id}/versions?${q}`, src, "application/typescript");
  if (r.status !== 201 && r.status !== 202) throw new Error(`deploy ${version}: HTTP ${r.status} ${await r.text()}`);
  // eager: wait for the build
  const n = (await r.json()).version;
  for (let i = 0; i < 120; i++) {
    const v = await (await api("GET", `/v1/services/${id}/versions/${n}`)).json();
    if (v.build_state === "ready") return;
    if (v.build_state === "failed") throw new Error(`build of ${version} failed`);
    await sleep(500);
  }
}

async function main() {
  console.log("\n== join, presence, history");
  const room = `room-${Date.now() % 100000}`;
  const alice = new Client(room, "alice");
  await alice.opened;
  const w1 = await alice.next("welcome");
  check(w1 && w1.room === room && w1.users.join() === "alice" && w1.history.length === 0, "alice joins an empty room");
  const bob = new Client(room, "bob");
  await bob.opened;
  const w2 = await bob.next("welcome");
  check(w2 && w2.users.join() === "alice,bob", "bob sees alice and himself");
  const j = await alice.next("join");
  check(j && j.name === "bob", "alice is told bob joined");
  alice.say("hi bob");
  const m1 = await bob.next("message");
  const m1a = await alice.next("message");
  check(m1 && m1.from === "alice" && m1.text === "hi bob" && m1.seq === 1, "bob gets alice's message");
  check(m1a && m1a.seq === 1, "the sender gets it too");
  const pres = await (await fetch(`${RT_INVOKE}/s/${GATE}/rooms/${room}/presence`)).json();
  check(pres.users.join() === "alice,bob", "presence from the sockets' tags");
  alice.ws.send("ping");
  check((await alice.next("pong")) !== null, "ping is answered by the runtime (auto-response)");

  console.log("\n== hibernation: no isolate between events");
  const r = await api("PATCH", `/v1/services/${await serviceId(ROOMS)}`, JSON.stringify({ ws: { warm: false } }));
  check(r.status === 200, "rooms: ws.warm = false (a fresh isolate per event)");
  await sleep(1500);
  bob.say("still here?");
  const m2 = await alice.next("message");
  await bob.next("message"); // bob's own copy
  check(m2 && m2.seq === 2 && m2.from === "bob", "the sockets held while the room slept; seq continues from storage");

  console.log("\n== redeploy: sockets stay, the next event runs the new version");
  await redeploy("v2", false);
  alice.say("after deploy");
  const m3 = await bob.next("message");
  check(m3 && m3.version === "v2" && m3.seq === 3, "same sockets, new code (v2), history kept");
  const hist = await (await fetch(`${RT_INVOKE}/s/${GATE}/rooms/${room}/history`)).json();
  check(hist.messages.map((m) => m.seq).join() === "1,2,3", "history in storage: 3 messages");
  const late = new Client(room, "carol");
  await late.opened;
  const w3 = await late.next("welcome");
  check(w3 && w3.history.length === 3 && w3.version === "v2", "a late joiner gets the history");

  console.log("\n== redeploy with close_sockets: 1012");
  await redeploy("v3", true);
  const c1 = await alice.next("closed");
  const c2 = await bob.next("closed");
  check(c1 && c1.code === 1012 && c2 && c2.code === 1012, "every member is closed with 1012");
  const again = new Client(room, "alice");
  await again.opened;
  const w4 = await again.next("welcome");
  check(w4 && w4.version === "v3" && w4.history.length === 3, "reconnected to v3, history intact");
  again.ws.close();
  late.ws.close();
}

main()
  .catch((e) => fail(`error: ${e.message}`))
  .finally(() => process.exit(failed > 0 ? 1 : 0));
