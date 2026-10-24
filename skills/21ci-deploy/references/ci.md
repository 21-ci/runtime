# Contexts, CI, exit codes, MCP

## Configuration precedence (highest first)

| Setting | Order |
|---|---|
| control API | `RT_API`, context `api`, `http://127.0.0.1:8081` |
| invoke listener | `RT_INVOKE`, context `invoke`, `http://127.0.0.1:8080` |
| token | `RT_KEY`, `RT_ROOT_TOKEN`, context `token` |
| tenant (`x-rt-tenant`, operator token only) | `--as-tenant` / `RT_TENANT`, context `tenant` |
| context | `--context` / `RT_CONTEXT`, `current_context` |

Other variables: `RT_INVOKE_KEY` (key `invoke` sends to `access=key` services; the control token is never sent to the
invoke listener), `RT_RETRY` (`--retry`), `XDG_CONFIG_HOME`. An empty variable counts as unset. The config file is
`~/.config/21ci/config.toml` (or `$XDG_CONFIG_HOME/21ci/config.toml`, or `--config FILE`), written with mode 0600:

```toml
current_context = "demo"

[contexts.demo]
api = "http://127.0.0.1:8081"
invoke = "http://127.0.0.1:8080"
token = "rtk_…"
# tenant = "ten_…"     # operator token acting for a tenant
```

The root token cannot create services by itself (services belong to a tenant): use a tenant key, or
`--as-tenant ten_…` with the operator token.

## CI job

No config file needed:

```bash
export RT_API=https://rt.example.com:8081
export RT_INVOKE=https://fn.example.com
export RT_KEY="$DEPLOY_KEY"      # rtk_… with deploy + services:read (+ services:write for --create)
export RT_RETRY=3                # ride out short busy periods (429/503 with Retry-After)

21ci doctor                                                        # fail early on bad config or token
21ci push ./service --service payments --wait --wait-timeout 5m    # fails the job if the build fails
21ci invoke payments /healthz --fail --body-only                   # smoke test, fails on >= 400
```

- Mint a narrow, expiring key for CI: `21ci token create --scopes services:read,deploy --name ci --expires-in 30d`
  (needs `keys:write`; the secret is printed once).
- `push --wait` is the whole deploy step: exit 1 on a failed build or timeout.
- Roll back from a job: `21ci rollback payments` or `21ci rollback payments --to N`.
- Collect errors: `21ci logs payments --since 10m --level error --json | jq -r '.items[].msg'`.
- Idempotence: `push` always creates a version; `service create` of an existing slug is `409 slug_taken`, so use
  `push --create` for "create if missing".
- Keep the token in the CI secret store and in `RT_KEY`; `config set-context --token` puts it on a command line.

## Exit codes

| Code | Meaning |
|---|---|
| 0 | success |
| 1 | the API answered with an error, or the command failed after talking to it (build failed, `--wait` timed out, nothing to roll back to, not found, a `doctor` check failed, `invoke --fail` got >= 400) |
| 2 | usage error: bad flags or arguments, invalid slug/host/JSON/file/block field, unknown context, unreadable config; nothing was sent |
| 3 | network error: unreachable or timed out |
| 4 | busy: `429` or `503` **with** `Retry-After` (rate limit, quota, `queue_timeout`, `overloaded`, `route_stale`, too many log streams), after any `--retry` attempts |

Errors print as `error: <code>: <message> (HTTP <status>[, retry after Ns])` with `details` indented below. With `--json`
the error is one stderr line: `{"error": {"code", "message", "details"?, "status"?, "retry_after"?}}` (`code` is
`usage`, `network_error` or `failed` for errors the CLI raises itself; `http_<status>` or the `x-rt-reason` for non-JSON
bodies).

`--retry N` sleeps for `Retry-After` and resends, at most N times (a `Retry-After` above 60 s is reported at once).
A `503` without `Retry-After` (`control_disabled`, `draining`, `restarting`) is never retried.

## Common control API errors

| Code | HTTP | Fix |
|---|---|---|
| `unauthorized` | 401 | wrong, revoked or expired token |
| `insufficient_scope` | 403 | key lacks the scope (see commands.md "Scopes") |
| `quota_exceeded`, `trigger_not_allowed`, `host_not_allowed` | 403 | plan limit or disabled trigger |
| `not_found` | 404 | wrong slug, or the key's project/environment restriction hides it |
| `slug_taken`, `version_not_ready`, `host_taken`, `environment_in_use`, `project_not_empty` | 409 | |
| `precondition_failed` | 412 | concurrent change: re-run the command |
| `too_large` | 413 | upload or body too large |
| `validation_failed`, `invalid_artifact`, `missing_export`, `limit_exceeds_plan`, `schedule_too_frequent`, `no_rpc_surface` | 422 | read `details` |
| `rate_limited`, `build_quota`, `queue_full` | 429 | wait for `Retry-After` |

## MCP (assistant access without the CLI)

The control API serves an MCP endpoint at `POST <api>/mcp` (tenant keys only; the operator token is refused). Tools
cover services, `deploy_code` (one TS/JS file or a files bundle, up to 3 MiB; WASM must go through `21ci push`),
versions, `rollback`, logs, stats, env, secrets, projects, SDKs, sockets and tokens; schedules (except listing pending
one-offs), queues, hosts and data export are not exposed. Tools visible = the key's scopes.

```bash
21ci token create --name claude --scopes services:read,services:write,deploy,logs:read --expires-in 30d
claude mcp add --transport http 21cloud https://api.example.com/mcp --header "Authorization: Bearer $RT_KEY"
# clients that start a program instead: command "21ci", args ["mcp"], env RT_API / RT_KEY
```

Details: [docs/mcp.md](https://github.com/21-ci/runtime/blob/main/docs/mcp.md).
