import { test } from "node:test";
import assert from "node:assert/strict";
import { BotApi, isValidSecretToken, type OutboxEntry, parseUpdate, TelegramError } from "../src/telegram.ts";
import { bearerToken, checkBearer, readJson, safeEqual } from "../src/http.ts";

test("parseUpdate: text message", () => {
  const u = parseUpdate({
    update_id: 10,
    message: { message_id: 5, date: 1, chat: { id: -1001234567890, type: "supergroup" }, from: { id: 42 }, text: "/notes" },
  });
  assert.deepEqual(u, {
    updateId: 10,
    message: { chatId: -1001234567890, chatType: "supergroup", messageId: 5, fromId: 42, text: "/notes" },
  });
});

test("parseUpdate: ignored and malformed updates", () => {
  assert.deepEqual(parseUpdate({ update_id: 11, edited_message: {} }), { updateId: 11, message: null });
  assert.equal(parseUpdate({ update_id: -1 }), null);
  assert.equal(parseUpdate({ update_id: 1.5 }), null);
  assert.equal(parseUpdate({ update_id: 1, message: { chat: { id: "x", type: "private" }, message_id: 1 } }), null);
  assert.equal(parseUpdate([]), null);
  const photo = parseUpdate({ update_id: 2, message: { message_id: 1, chat: { id: 1, type: "private" } } });
  assert.equal(photo?.message?.text, null);
});

test("secret token rules", () => {
  assert.equal(isValidSecretToken("a".repeat(16)), true);
  assert.equal(isValidSecretToken("short"), false);
  assert.equal(isValidSecretToken("has space in it 123"), false);
  assert.equal(isValidSecretToken(undefined), false);
});

test("BotApi: dry run records calls", async () => {
  const outbox: OutboxEntry[] = [];
  const bot = new BotApi({ token: "", dryRun: true, outbox });
  const me = await bot.call<{ username: string }>("getMe");
  assert.equal(me.username, "dry_run_bot");
  await bot.sendMessage(7, "hi");
  assert.deepEqual(outbox[1], { method: "sendMessage", params: { chat_id: 7, text: "hi", link_preview_options: { is_disabled: true } } });
});

test("BotApi: errors carry retry_after and the permanent flag", async () => {
  const reply = (status: number, body: unknown) => () => Promise.resolve(new Response(JSON.stringify(body), { status }));
  const limited = new BotApi({
    token: "t",
    fetchFn: reply(429, { ok: false, error_code: 429, description: "Too Many Requests", parameters: { retry_after: 7 } }) as typeof fetch,
  });
  await assert.rejects(limited.sendMessage(1, "x"), (e: unknown) => e instanceof TelegramError && e.retryAfter === 7 && !e.permanent);

  const blocked = new BotApi({
    token: "t",
    fetchFn: reply(403, { ok: false, error_code: 403, description: "Forbidden: bot was blocked by the user" }) as typeof fetch,
  });
  await assert.rejects(blocked.sendMessage(1, "x"), (e: unknown) => e instanceof TelegramError && e.permanent);

  const okApi = new BotApi({ token: "t", fetchFn: reply(200, { ok: true, result: { message_id: 3 } }) as typeof fetch });
  assert.deepEqual(await okApi.sendMessage(1, "x"), { message_id: 3 });
});

test("safeEqual and bearer tokens", async () => {
  assert.equal(await safeEqual("abc", "abc"), true);
  assert.equal(await safeEqual("abc", "abd"), false);
  assert.equal(await safeEqual("abc", "abcd"), false);
  const req = (h?: string) => new Request("http://x/", h ? { headers: { authorization: h } } : {});
  assert.equal(bearerToken(req("Bearer tok_123")), "tok_123");
  assert.equal(bearerToken(req("Basic abc")), null);
  assert.equal(await checkBearer(req("Bearer " + "s".repeat(20)), "s".repeat(20)), "ok");
  assert.equal(await checkBearer(req("Bearer nope"), "s".repeat(20)), "denied");
  assert.equal(await checkBearer(req("Bearer x"), "short"), "unconfigured");
});

test("readJson enforces the size limit and JSON syntax", async () => {
  const post = (body: string) => new Request("http://x/", { method: "POST", body });
  assert.deepEqual(await readJson(post('{"a":1}'), 100), { ok: true, value: { a: 1 } });
  const big = await readJson(post("x".repeat(101)), 100);
  assert.equal(big.ok ? 0 : big.response.status, 413);
  const bad = await readJson(post("{"), 100);
  assert.equal(bad.ok ? 0 : bad.response.status, 400);
  assert.deepEqual(await readJson(post(""), 100, true), { ok: true, value: null });
});
