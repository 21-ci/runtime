// Telegram Bot API: update validation (pure) and a small client over fetch.
// https://core.telegram.org/bots/api

import { clip } from "./bot.ts";

export interface IncomingMessage {
  chatId: number;
  chatType: string; // "private" | "group" | "supergroup" | "channel"
  messageId: number;
  fromId: number | null;
  text: string | null; // null for stickers, photos, …
}

export interface ParsedUpdate {
  updateId: number;
  message: IncomingMessage | null; // null for update types the bot ignores
}

const isObj = (v: unknown): v is Record<string, unknown> => typeof v === "object" && v !== null && !Array.isArray(v);

// Only the fields the bot uses are checked; anything malformed is rejected as a whole.
export function parseUpdate(raw: unknown): ParsedUpdate | null {
  if (!isObj(raw)) return null;
  const updateId = raw.update_id;
  if (typeof updateId !== "number" || !Number.isSafeInteger(updateId) || updateId < 0) return null;

  const msg = raw.message; // edited_message, channel_post, … are ignored (setWebhook asks for "message" only)
  if (msg === undefined) return { updateId, message: null };
  if (!isObj(msg) || !isObj(msg.chat)) return null;
  const chatId = msg.chat.id;
  const chatType = msg.chat.type;
  const messageId = msg.message_id;
  if (typeof chatId !== "number" || !Number.isSafeInteger(chatId)) return null;
  if (typeof chatType !== "string" || typeof messageId !== "number" || !Number.isSafeInteger(messageId)) return null;
  const fromId = isObj(msg.from) && typeof msg.from.id === "number" && Number.isSafeInteger(msg.from.id) ? msg.from.id : null;
  const text = typeof msg.text === "string" ? msg.text.slice(0, 4096) : null;
  return { updateId, message: { chatId, chatType, messageId, fromId, text } };
}

// Telegram allows 1-256 characters A-Z, a-z, 0-9, _ and - for secret_token; this bot also wants
// at least 16, so the secret cannot be guessed.
export function isValidSecretToken(s: string | undefined): s is string {
  return typeof s === "string" && /^[A-Za-z0-9_-]{16,256}$/.test(s);
}

export class TelegramError extends Error {
  status: number;
  errorCode: number;
  retryAfter: number | null;

  constructor(status: number, errorCode: number, description: string, retryAfter: number | null) {
    super(`telegram ${errorCode}: ${description}`);
    this.name = "TelegramError";
    this.status = status;
    this.errorCode = errorCode;
    this.retryAfter = retryAfter;
  }

  // The chat cannot be reached and retrying will not help: the user blocked the bot, the chat was
  // deleted, the bot was removed from the group.
  get permanent(): boolean {
    return this.errorCode === 403 || (this.errorCode === 400 && /chat not found|user is deactivated|bot was kicked/i.test(this.message));
  }
}

export interface OutboxEntry {
  method: string;
  params: Record<string, unknown>;
}

export interface BotApiOptions {
  token: string;
  dryRun?: boolean; // record calls in `outbox` instead of sending them (smoke tests, local runs)
  outbox?: OutboxEntry[];
  base?: string;
  timeoutMs?: number;
  fetchFn?: typeof fetch;
}

export class BotApi {
  #token: string;
  #dryRun: boolean;
  #outbox: OutboxEntry[];
  #base: string;
  #timeoutMs: number;
  #fetch: typeof fetch;

  constructor(o: BotApiOptions) {
    this.#token = o.token;
    this.#dryRun = o.dryRun ?? false;
    this.#outbox = o.outbox ?? [];
    this.#base = o.base ?? "https://api.telegram.org";
    this.#timeoutMs = o.timeoutMs ?? 10_000;
    this.#fetch = o.fetchFn ?? fetch;
  }

  async call<T = unknown>(method: string, params: Record<string, unknown> = {}): Promise<T> {
    if (this.#dryRun) {
      this.#outbox.push({ method, params });
      return dryRunResult(method) as T;
    }
    const ac = new AbortController();
    const timer = setTimeout(() => ac.abort(), this.#timeoutMs);
    let res: Response;
    try {
      // The token is part of the URL: never log the URL or the request.
      res = await this.#fetch(`${this.#base}/bot${this.#token}/${method}`, {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify(params),
        signal: ac.signal,
      });
    } finally {
      clearTimeout(timer);
    }
    let data: Record<string, unknown> | null = null;
    try {
      const parsed = await res.json();
      data = isObj(parsed) ? parsed : null;
    } catch {
      // not JSON (proxy error page, …)
    }
    if (res.ok && data?.ok === true) return data.result as T;
    const p = isObj(data?.parameters) ? data!.parameters as Record<string, unknown> : {};
    throw new TelegramError(
      res.status,
      typeof data?.error_code === "number" ? data.error_code : res.status,
      typeof data?.description === "string" ? data.description : `HTTP ${res.status}`,
      typeof p.retry_after === "number" ? p.retry_after : null,
    );
  }

  sendMessage(chatId: number, text: string): Promise<unknown> {
    return this.call("sendMessage", { chat_id: chatId, text: clip(text), link_preview_options: { is_disabled: true } });
  }
}

function dryRunResult(method: string): unknown {
  switch (method) {
    case "getMe":
      return { id: 0, is_bot: true, first_name: "Dry run", username: "dry_run_bot" };
    case "sendMessage":
      return { message_id: 0, date: 0, chat: { id: 0, type: "private" } };
    case "getWebhookInfo":
      return { url: "", has_custom_certificate: false, pending_update_count: 0 };
    default:
      return true;
  }
}
