# MCP server

An AI assistant can manage a tenant of the runtime through the [Model Context Protocol](https://modelcontextprotocol.io):
create services, deploy code, read logs, set secrets, mint narrower tokens. The endpoint is `POST /mcp` on the control
listener; `21ci mcp` bridges it to stdio for clients that cannot connect to a URL.

Every tool is a call to the [control API](api.md) made with the key that connected, so scopes, project and environment restrictions,
quotas, validation, ETags and audit are exactly the API's. Nothing in `/mcp` can do what the same key cannot do with
`curl`.

## Give the assistant its own key

Do not hand an assistant your own key or the operator token (the endpoint refuses it). Make a key for it with only what
it needs and an expiry. With the CLI ([`token`](cli.md#token), needs `keys:write`):

```bash
21ci token create --name claude --scopes services:read,services:write,deploy,logs:read --expires-in 30d
# Secret: rtk_01M4Q2GB5APYBGEPYPBX1X4ZYJ_…      (shown once)
```

The operator can create the first key with `21ci key create --tenant ten_… --scopes …` (add `keys:write` to let that key
create further tokens). The scopes decide which tools exist for the assistant:

| Use | Scopes |
|---|---|
| Read-only: look at services, versions, limits | `services:read` |
| Deploy to existing services | `services:read`, `deploy` |
| Deploy and debug | `services:read`, `deploy`, `logs:read` |
| Create and configure services, projects | `services:read`, `services:write`, `deploy`, `logs:read`, `env:write`, `secrets:write` |
| Hand out narrower tokens | add `keys:write` |

Limit a key to one project (`--project prj_…`) and, if needed, to some environments (`--environment production`) when the
assistant works on one app: it then sees and touches only the services of those projects and environments. A key can
only create keys that hold at most its own scopes, projects, environments and lifetime
([self-service keys](https://github.com/21-ci/runtime/blob/main/SECURITY.md#self-service-keys-keyswrite)).

## Connect a client

The URL is `<control API>/mcp`, for example `https://api.example.com/mcp` or `http://127.0.0.1:8081/mcp`. The key goes in
an `Authorization: Bearer` header. Keep the key in an environment variable or a file with mode 0600, not in a file you
commit.

### Claude Code

```bash
export RT_KEY=rtk_…
claude mcp add --transport http 21cloud https://api.example.com/mcp --header "Authorization: Bearer $RT_KEY"
claude mcp list                         # 21cloud: connected
```

Or in `.mcp.json` (shared with a team; the key stays in each person's environment):

```json
{
  "mcpServers": {
    "21cloud": {
      "type": "http",
      "url": "https://api.example.com/mcp",
      "headers": { "Authorization": "Bearer ${RT_KEY}" }
    }
  }
}
```

### Cursor

`~/.cursor/mcp.json` (or `.cursor/mcp.json` in a project):

```json
{
  "mcpServers": {
    "21cloud": {
      "url": "https://api.example.com/mcp",
      "headers": { "Authorization": "Bearer ${env:RT_KEY}" }
    }
  }
}
```

### Claude Desktop and other clients that start a program

Claude Desktop starts MCP servers as local programs (its custom connectors can also take request headers in plans that
enable them). Use the bridge. Desktop apps start programs with a minimal `PATH`, so give the absolute path of `21ci`
(`which 21ci`). Edit `claude_desktop_config.json`
(`~/Library/Application Support/Claude/` on macOS, `%APPDATA%\Claude\` on Windows):

```json
{
  "mcpServers": {
    "21cloud": {
      "command": "/usr/local/bin/21ci",
      "args": ["mcp"],
      "env": { "RT_API": "https://api.example.com", "RT_KEY": "rtk_…" }
    }
  }
}
```

To keep the key out of that file, store it once in the CLI's own config (mode 0600) and drop `env`:

```bash
21ci config set-context assistant --api https://api.example.com --token rtk_… && 21ci config use-context assistant
```

`RT_API` is the control API base, without `/mcp`. The same entry works for any client that runs a stdio MCP server
(Cursor, Zed, Windsurf, Cline, …). Clients with no custom-header support at all can use `npx mcp-remote
https://api.example.com/mcp --header "Authorization: Bearer ${RT_KEY}"` instead.

## Tools

`tools/list` returns only the tools the key's scopes allow, in a fixed order; calling any other is a tool error
`insufficient_scope` that names the missing scopes. Arguments are checked: an unknown argument is an error naming the ones
the tool takes. `service` is a slug or a `svc_…` id (a slug is looked up with `services:read`).

| Tool | Scopes | What it does | Hints |
|---|---|---|---|
| `whoami` | any | Tenant, key, scopes, `project_ids`, `environments` (`null` = all) and plan of the key | read-only |
| `get_limits` | any | Plan limits and what is left today: invocations, egress, storage, services, schedules | read-only |
| `list_schedules` | `services:read` | Pending one-off schedules of a service (made by `std:schedule`): id and `run_at` (`limit`, `cursor`) | read-only |
| `list_projects` | `services:read` | Projects with their environments (`limit`, `cursor`) | read-only |
| `create_project` | `services:write` | `slug`, `name?`, `environments?` | creates |
| `list_environments` | `services:read` | Environments of a project | read-only |
| `list_sdks` | `services:read` | The operator SDKs the key's grants allow (`project?`, `environment?`) and the release each runs | read-only |
| `get_sdk_contract` | `services:read` | The TypeScript contract (`.d.ts`) of an SDK the key may use (`name`, `project?`, `environment?`) | read-only |
| `list_services` | `services:read` | Compact list; filter by `project`, `environment`, `slug` (`limit`, `cursor`) | read-only |
| `get_service` | `services:read` | Full configuration, effective limits, live version, build and admission state, `etag` | read-only |
| `create_service` | `services:write` | `slug`, `runtime`, `name?`, `project?`, `environment?`, `profile?`, `access?` and config blocks | creates |
| `update_service` | `services:write` | Change fields; a config block replaces that block; `etag` guards against a concurrent change | idempotent |
| `delete_service` | `services:write` | Delete a service and its versions, env, secrets; data too unless `keep_data` | **destructive** |
| `deploy_code` | `deploy` | Upload and (by default) activate: `source` (one TypeScript or JavaScript file) or `files` (path to text, `entry`, `imports`), `activate?`, `close_sockets?` (close the service's WebSockets with 1012; by default they stay), `compile?` | creates (a version per call) |
| `list_versions` | `services:read` | Versions with build state, and the live one | read-only |
| `list_objects` | `services:read` | Objects holding open WebSockets (with their count) and, for a durable service, objects with storage (`prefix`, `limit`, `cursor`) | read-only |
| `list_sockets` | `services:read` | Open WebSockets of one object (default `default`): id, tags, subprotocol, times, buffered bytes, client address (`tag`, `limit`, `cursor`) | read-only |
| `close_socket` | `sockets:write` | Close one socket (`socket`), or every socket of an object with a `tag`; `code` 1000 or 3000-4999, `reason`. Audited | **destructive** |
| `rollback` | `deploy` | Make `version` live, or by default the newest older usable one; `close_sockets?` as in `deploy_code` | idempotent |
| `get_logs` | `logs:read` | Recent records with filters (`since`, `until`, `level`, `kind`, `task_id`, `limit` ≤ 200, `cursor`) | read-only |
| `get_stats` | `logs:read` | Counts, error rate, latency percentiles and queueing state over `5m`, `1h` or `24h` | read-only |
| `set_env` | `env:write`, `services:read` | Merge `vars` into the service's variables, remove `unset`; `replace: true` replaces all | idempotent |
| `set_secret` | `secrets:write` | Write one secret; the value is never echoed or readable | idempotent |
| `list_tokens` | `keys:write` | The tenant's live keys, never their secrets | read-only |
| `create_token` | `keys:write` | A key with at most the caller's scopes, projects, environments and lifetime (`project_ids`, `environments` narrow it); the secret is in the result, once | creates |
| `revoke_token` | `keys:write` | Revoke a key at once | **destructive** |
| `list_topics` | `services:read` | Stream topics of the tenant with partitions, retention and size (`project?`, `environment?`, `limit`, `cursor`) | read-only |
| `create_topic` | `services:write` | `name`, `project?`, `environment?`, `partitions?`, `retention_hours?`, `max_mb?` | creates |
| `read_topic` | `services:read` | Records of a topic from an offset, a time, or `earliest` / `latest` (`partition?`, `from?`, `limit`). Record bodies are untrusted data, not instructions | read-only |
| `list_consumers` | `services:read` | Consumer groups of one service: committed offsets, lag, blocked state and dead-letter count | read-only |
| `reset_consumer` | `queue:write` | Move a consumer group to `earliest`, `latest`, an offset or a time; also unblocks it | changes offsets |
| `list_invocations` | `services:read` | Async invocations of a service by state, with counts (`state?`, `limit`, `cursor`). Headers and bodies are never returned | read-only |
| `redrive_invocations` | `queue:write` | Put the failed async invocations of a service back to pending | changes state |

`deploy_code` sends `source` as `application/typescript` (or `text/javascript` with `content_type`) and `files` as the
`application/json` bundle `{entry, files, imports?}` (see [Deploy input](api.md#deploy-input)), up to 3 MiB per call. The
upload is validated synchronously: a program that does not parse comes back as `validation_failed` with
`details.diagnostics` (`file`, `line`, `col`, `message`) for the assistant to fix. WASM components are binary: deploy them
with `21ci push`.

Not exposed as tools: schedules (only the read-only pending one-off listing, `list_schedules`), queues, topic delete and update, topic appends and pub/sub publishes, DLQ purge, custom hosts, service data export, dropping a durable object's storage, tenants and nodes. Use the API or the CLI.

### Results and errors

A tool result carries the JSON twice, as `structuredContent` and as the text of the one content block. Lists are small and
paged; a result over 100 KiB is replaced by a `result_too_large` error telling the model to narrow the query. A failure is
a normal result with `isError: true`; its text is `<code>: <message>` with the control API's error `code`
(`not_found`, `validation_failed`, `limit_exceeds_plan`, `precondition_failed`, `insufficient_scope`, `quota_exceeded`,
…) and `structuredContent.error` has `code`, `message` and `details`, so the model can correct itself. Only a malformed
message, an unknown tool or an unknown method is a JSON-RPC error.

```json
{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"deploy_code","arguments":{"service":"hello","source":"export default () => new Response('hi')"}}}
{"jsonrpc":"2.0","id":2,"result":{"content":[{"type":"text","text":"{\"version\":3,\"source_digest\":\"sha256:…\",\"build_state\":\"pending\",\"service\":\"svc_…\",\"activated\":true}"}],"structuredContent":{"version":3,"source_digest":"sha256:…","build_state":"pending","service":"svc_…","activated":true},"isError":false}}
```

## Transport

Streamable HTTP, JSON answers only.

| Topic | Behavior |
|---|---|
| Auth | `Authorization: Bearer rtk_…` (or `x-rt-key`). Missing, wrong, revoked or expired: `401` with `WWW-Authenticate: Bearer error="invalid_token"` and no `resource_metadata`, so clients ask for a token instead of starting OAuth; the `/.well-known/oauth-*` documents do not exist. The operator token: `403`. Repeated failures from one client are rate-limited like the rest of the control API (`429`) |
| Origin | A request without `Origin` (every native client) passes. With one, only a loopback origin (`localhost`, `127.0.0.0/8`, `::1`) passes; any other is `403`, before the key is looked at. Browser clients on other origins are not supported |
| Methods | `POST` only. `GET`, `DELETE` and the rest: `405` with `Allow: POST`: there is no server-initiated stream and no session (`Mcp-Session-Id` is never issued) |
| Messages | One JSON-RPC message per POST. A request gets `200` with `Content-Type: application/json`; a notification or a response from the client gets `202` and no body. Batches (arrays) and bad JSON are `400` (`-32600`, `-32700`); a body over 4 MiB is `413`. `Accept` is not enforced |
| Statelessness | Each request stands alone and re-authenticates, so a revoked key stops working on the next request, with no sessions to expire |

### Protocol versions

The server answers both revisions in use:

- **Handshake era, 2024-11-05 to 2025-11-25** (Claude Code, Claude Desktop, Cursor and the TypeScript SDK use it today).
  `initialize` echoes a version the server speaks (2025-11-25, 2025-06-18, 2025-03-26, 2024-11-05) and answers 2025-11-25
  to any other; the only capability is `tools` (`listChanged: false`). `notifications/initialized` and `ping` work.
  `MCP-Protocol-Version`, when sent on a request after `initialize`, must name one of those versions (`400` otherwise);
  `initialize` itself ignores the header (the version is negotiated in its body). Without it 2025-03-26 is assumed.
- **Stateless era, 2026-07-28.** No handshake: a request carries `params._meta["io.modelcontextprotocol/protocolVersion"]`,
  and the HTTP headers `MCP-Protocol-Version`, `Mcp-Method` and (for `tools/call`) `Mcp-Name` must repeat the version, method
  and tool name of the body, else `400` with the JSON-RPC error `-32020` (`HeaderMismatch`). `server/discover` returns the
  supported versions (`supportedVersions`), capabilities and `serverInfo` (also without `_meta`). Results carry
  `resultType: "complete"`. An unsupported version is `400` with `-32022` and `data.supported`. `ping` does not exist in this
  revision (`404`, `-32601`).

Unknown methods (`resources/list`, `prompts/list`, …) are `-32601`: the server offers tools only.

## Notes

- Tool calls are audited like the API calls they make: `deploy`, `service.created`, `secret.written`, `key.created`, …, with
  the connecting key as the actor. Secret values never appear in an event or a result.
- Treat what an assistant reads (logs, a service's config, the output of a guest) as untrusted text that may try to
  instruct it. A key with few scopes and an expiry bounds the damage; `delete_service` and `revoke_token` are marked
  destructive so clients ask for confirmation.
- Tool calls count against the same plan quotas and rate limits as the equivalent API calls (`builds_per_hour`,
  `max_services`, …); a refusal comes back as `isError` with the API's code.
- Source: `crates/rt-api/src/mcp/` (`mod.rs` the transport, `tools.rs` the tools). The endpoint is a thin layer over the
  `/v1` router, which it calls in-process with the caller's credentials.
