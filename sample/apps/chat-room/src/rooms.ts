// chat-room / rooms: one durable object per chat room (std:durable + std:sockets).
//
// Deployed as a *durable* service (`"durable": true`): the default export is a class with one
// instance per room, chosen by the gate service with `durable.get(room, { service })`. The room
// keeps its history in its own storage (rows of this service's data.db), knows who is here from
// its sockets' tags (presence), answers `ping` without waking up (auto-response) and prunes its
// history with an alarm. Nothing lives only in memory: the room may be evicted between any two
// messages (or run warm), and it survives deploys and node restarts.
import { type Socket, sockets } from "std:sockets";

const VERSION = "v1"; // the smoke redeploys with another value
const HISTORY = 50; // messages a joining client gets
const KEEP = 200; // messages a room keeps
const PRUNE_AFTER_MS = 60 * 60 * 1000; // the history alarm
const NAME = /^[A-Za-z0-9_.-]{1,32}$/;
const MAX_TEXT = 2000;

interface Message {
  seq: number;
  from: string;
  text: string;
  at: number;
}

const key = (seq: number) => "msg:" + String(seq).padStart(12, "0");

function users(): string[] {
  const names = sockets.list({ limit: 1000 }).sockets.flatMap((s: Socket) => s.tags).filter((t: string) => t.startsWith("user:"));
  return [...new Set(names.map((t: string) => t.slice(5)))].sort();
}

function send(ws: Socket, v: unknown) {
  ws.send(JSON.stringify(v));
}

function broadcast(v: unknown, except?: string) {
  sockets.broadcast(JSON.stringify(v), except ? { except } : undefined);
}

export default class Room {
  state: any;
  room: string;

  constructor(state: any, _env: unknown) {
    this.state = state;
    this.room = state.id.name;
  }

  async history(limit = HISTORY): Promise<Message[]> {
    const page = await this.state.storage.list({ prefix: "msg:", reverse: true, limit });
    return [...page.values()].reverse();
  }

  // Requests come from the gate: the upgrade of a member, and the room's history and presence.
  async fetch(req: Request): Promise<Response> {
    const u = new URL(req.url);
    if (req.headers.get("upgrade")?.toLowerCase() === "websocket") {
      const name = u.searchParams.get("name") ?? "";
      if (!NAME.test(name)) return new Response("bad name", { status: 400 });
      // Heartbeats never wake the room.
      sockets.setAutoResponse({ request: "ping", response: "pong" });
      const res = sockets.accept(req, { tags: [`user:${name}`], attachment: { name, joinedAt: Date.now() } });
      const me = (res as any).socket as Socket;
      send(me, { type: "welcome", room: this.room, version: VERSION, history: await this.history(), users: [...users(), name].sort() });
      broadcast({ type: "join", name }, me.id);
      return res;
    }
    if (u.pathname.endsWith("/history")) return Response.json({ room: this.room, messages: await this.history() });
    if (u.pathname.endsWith("/presence")) return Response.json({ room: this.room, users: users() });
    return new Response("not found", { status: 404 });
  }

  async webSocketMessage(ws: Socket, message: string | ArrayBuffer): Promise<void> {
    if (typeof message !== "string") return;
    let text = "";
    try {
      text = String(JSON.parse(message).text ?? "");
    } catch {
      return send(ws, { type: "error", error: "send JSON: {\"text\": \"...\"}" });
    }
    if (text.length === 0 || text.length > MAX_TEXT) return send(ws, { type: "error", error: "text is 1 to 2000 characters" });
    const { name } = ws.attachment as { name: string };
    const s = this.state.storage;
    const seq = ((await s.get("seq")) ?? 0) + 1;
    const msg: Message = { seq, from: name, text, at: Date.now() };
    await s.put({ seq, [key(seq)]: msg });
    if ((await s.getAlarm()) === null) await s.setAlarm(Date.now() + PRUNE_AFTER_MS);
    // Sent after the writes commit: nobody sees a message the room did not keep.
    broadcast({ type: "message", version: VERSION, ...msg });
  }

  async webSocketClose(ws: Socket, _code: number, _reason: string, _wasClean: boolean): Promise<void> {
    const { name } = ws.attachment as { name: string };
    if (!users().includes(name)) broadcast({ type: "leave", name });
  }

  // Keep the newest KEEP messages; come back while the room is used.
  async alarm(): Promise<void> {
    const s = this.state.storage;
    const seq = (await s.get("seq")) ?? 0;
    const old = await s.list({ prefix: "msg:", end: key(Math.max(0, seq - KEEP + 1)), limit: 1000 });
    if (old.size > 0) await s.delete([...old.keys()]);
    if (sockets.count() > 0) await s.setAlarm(Date.now() + PRUNE_AFTER_MS);
  }
}
