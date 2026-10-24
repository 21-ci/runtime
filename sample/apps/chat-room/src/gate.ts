// chat-room / gate: the public front of the chat. It serves the page, checks what a client asks
// for (room and member names), and forwards each room's requests to that room's durable object in
// the rooms service (`durable.get(room, { service: ROOMS })`): WebSocket upgrades included, so the
// socket ends up bound to the room object, not to the gate.
import { durable } from "std:durable";

const ROOM = /^[a-z0-9][a-z0-9-]{0,31}$/;

function rooms(): string {
  return Deno.env.get("ROOMS_SLUG") ?? "chat-rooms";
}

const PAGE = `<!doctype html><meta charset="utf-8"><title>chat-room</title>
<style>body{font:15px system-ui;max-width:40rem;margin:2rem auto}#log{border:1px solid #ccc;height:20rem;overflow:auto;padding:.5rem}</style>
<p><input id="room" value="lobby"> <input id="name" placeholder="your name"> <button id="join">join</button></p>
<div id="log"></div><p><input id="text" size="50"> <button id="send">send</button></p>
<script>
let ws;
const log = (t) => { const p = document.createElement("div"); p.textContent = t; document.getElementById("log").append(p); };
document.getElementById("join").onclick = () => {
  const room = document.getElementById("room").value, name = document.getElementById("name").value;
  const base = location.pathname.replace(/\\/$/, "");
  ws = new WebSocket(location.origin.replace(/^http/, "ws") + base + "/rooms/" + room + "/ws?name=" + encodeURIComponent(name));
  ws.onmessage = (e) => { const m = JSON.parse(e.data);
    if (m.type === "welcome") { m.history.forEach((h) => log(h.from + ": " + h.text)); log("here: " + m.users.join(", ")); }
    if (m.type === "message") log(m.from + ": " + m.text);
    if (m.type === "join") log(m.name + " joined"); if (m.type === "leave") log(m.name + " left"); };
  ws.onclose = (e) => log("closed " + e.code);
  setInterval(() => ws.readyState === 1 && ws.send("ping"), 25000);
};
document.getElementById("send").onclick = () => ws.send(JSON.stringify({ text: document.getElementById("text").value }));
</script>`;

export default {
  async fetch(req: Request): Promise<Response> {
    const u = new URL(req.url);
    if (u.pathname === "/" || u.pathname === "") {
      return new Response(PAGE, { headers: { "content-type": "text/html; charset=utf-8" } });
    }
    const m = u.pathname.match(/^\/rooms\/([^/]+)\/(ws|history|presence)$/);
    if (!m || !ROOM.test(m[1])) return new Response("not found", { status: 404 });
    if (m[2] === "ws" && req.headers.get("upgrade")?.toLowerCase() !== "websocket") {
      return new Response("upgrade required", { status: 426 });
    }
    return durable.get(m[1], { service: rooms() }).fetch(req);
  },
};
