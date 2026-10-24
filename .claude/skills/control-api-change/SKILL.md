---
name: control-api-change
description: Use when adding or changing a control API route under /v1 (crates/rt-api handlers, DTOs, error codes, scopes), an MCP tool (crates/rt-api/src/mcp/tools.rs), or the 21ci command that drives it (crates/rt-cli). Covers auth/scope/tenant/project/environment checks, idempotency, audit, OpenAPI regeneration, CLI and MCP parity, docs/api.md, docs/cli.md, docs/mcp.md and the rt-server api tests.
---

# Changing the control API, MCP or CLI

A route is a contract in four places: **REST** (`/v1`), **OpenAPI** (`openapi/openapi.json`), **MCP** (where it makes
sense for an assistant) and **`21ci`**. Change all four together. Commits look like this: `feat(api): ...` (handler,
OpenAPI, tests), then `feat(cli): ...`, then `docs(...)`. See `c374ecf` and `9e7464d`.
Read the "Authentication" and "Tenancy" sections of SECURITY.md, and use the `security-review` skill for anything
that touches auth.

## 1. Handler (`crates/rt-api`)

- **Route.** Add a `.route("/v1/...", get(h).post(..))` in `control_router` (`crates/rt-api/src/control.rs`).
  Handlers live in `control.rs`, or in a split module (`projects.rs`, `tokens.rs`, `sdks.rs`, `sockets.rs`,
  `limits.rs`, `hosts.rs`); a new module also goes in `lib.rs`. `rt-api` must not depend on executors or `rt-host`.
- **Signature.** `(State(s): State<ApiState>, a: Auth, h: HeaderMap, ApiJson(b): ApiJson<Body>)`. `ApiJson` and
  `ApiQuery` (`extract.rs`) turn rejections into `422 validation_failed`, with bodies capped by `MAX_JSON_BODY`
  (1 MiB). Return `R<T>` or `Created<T>`.
- **Checks, in the order `services_create` uses them:**
  1. `a.require_scope(Scope::X)?`, or `a.require_operator()?` for operator-only routes. A new scope means editing
     `Scope`, `Scope::ALL` and `as_str` in `crates/rt-types/src/model.rs`, plus `ALL_SCOPES` in
     `crates/rt-server/tests/common/mod.rs`, plus the scope tables in docs.
  2. Get the tenant with `a.tenant()?`. The operator needs `x-rt-tenant`; for `sys`, writes are refused in
     `auth.rs`. Use `not_reserved(..)` on operator routes that name a tenant.
  3. Load objects with `load_service(&s, &a, id)` or `scoped_service_ids`. Anything the caller may not see is
     **404**, not 403.
  4. Check project and environment with `a.principal.can_touch(project, &env)`, `check_environment_in_project`
     (`projects.rs`) and `restricted(&a.principal)` for moves. A failure is `insufficient_scope`.
  5. Check plan and quotas with `check_limits(&plan, ..)` and `s.quotas.check(tenant, QuotaOp::..)`. Re-check
     them on PATCH and under races (`e5e53d7`).
- **Idempotency** (POSTs that create things):
  - Call `fingerprint(..)` and then `idem_replay(&s, &a, &h, fp)` before doing work, and `idem_store(..)` after it.
  - A key reused with a different body gives `422 idempotency_mismatch`.
  - The store is bounded (`IDEMPOTENCY_*` in `state.rs`).
- **Updates.** `if_match(&h)?` gives the ETag check (`412`).
- **Audit.** Call `audit(&s, &a, "<noun>.<verb>", tenant, service, data)` for every change. `data` must not hold
  secret values (names only). Add new actions to the audit table in SECURITY.md.
- **Errors.**
  - Use `ApiError::new("<code>", msg)`. A new code needs a row in `status_for_code` (`crates/rt-api/src/error.rs`),
    which is the single code-to-HTTP table.
  - Store errors convert through `From<StoreError>`, which is sanitized.
  - Codes are stable strings; never put driver text or paths in them.
- **Lists.** Take `PageQuery` (`dto.rs`) through `page_req()` (default 50, max 200) and return `{ items, next_cursor }`.
- **DTOs.** Put them in `crates/rt-api/src/dto.rs`, with `ToSchema` on bodies and views, and
  `IntoParams` + `#[into_params(parameter_in = Query)]` on queries. Optional fields must not be required in the
  schema.
- **State that grows** (per key, per service): give it a cap or eviction (SECURITY.md "Long-lived maps").

## 2. OpenAPI

- Add `#[utoipa::path(..)]` on the handler, and add it to `paths(...)` / `components(schemas(...))` of `ApiDoc`
  (`control.rs`).
- Regenerate with `RT_UPDATE_OPENAPI=1 cargo test -p rt-api the_committed_openapi_document_matches_the_code`. This
  is light, because rt-api links no V8 or wasmtime. Or run `make openapi` (`scripts/openapi.sh`, which builds
  `rt`). `scripts/openapi.sh --check` reports drift. Commit `openapi/openapi.json`.
- Unit checks in `control.rs`: `query_parameters_are_declared_in_query_and_optional`,
  `optional_body_fields_are_not_required`, `the_new_operations_are_documented`.
- Contract tests in `crates/rt-server/tests/api/openapi.rs`:
  - add the route to `ROUTE_MAP` and new codes to `ERROR_CODES` in `crates/rt-server/tests/common/openapi.rs`;
  - `ac_api_01_every_response_validates_against_the_openapi_schemas` checks real responses.

## 3. MCP tool (`crates/rt-api/src/mcp/tools.rs`)

Add an MCP tool when an assistant managing services would use the route. Operator-only routes have no tool, since
MCP refuses the root token.
- **Definition.** Add a `Def { name, title, description, scopes, kind, schema }` to `DEFS`.
  - `kind` (`Read`/`Create`/`Update`/`Destroy`) drives the annotations.
  - `scopes` must be **the same scopes the REST route requires**, or `tools/list` will advertise a tool that then
    fails.
  - Reuse the `service_prop!`, `page_props!` and `config_blocks!` schema macros.
- **Implementation.** Add a `"name" => ...` arm in `Tools::run`. Parse arguments with the `Args` helpers
  (`req_str`, `opt_str`, `only`, ...). Implement the tool by **replaying `/v1`** through `send` / `get` /
  `send_json`, so that scopes, quotas, ETags, idempotency and audit all come from the handler. Do not call
  the store directly.
- **Service arguments** must be a well-formed id or slug, never a path (`04df211`).
- **Tests.**
  - Unit tests in `tools.rs`: `every_tool_has_a_valid_object_schema_and_a_unique_name` and
    `annotations_follow_the_kind_of_tool`.
  - Integration tests in `crates/rt-server/tests/api/mcp.rs` (`post_mcp`, `request` helpers). Include a
    scope-denied case like `ac_sec_61_tools_are_limited_by_the_scopes_of_the_key`. Edge cases go in `edge_mcp.rs`.
- **No test enforces parity** between REST, MCP and the docs. Check by hand that the tool table in `docs/mcp.md`
  and the MCP scope table in SECURITY.md list the tool with the same scopes. The SECURITY.md table currently lags
  `DEFS`.

## 4. CLI (`crates/rt-cli`, binary `21ci`)

- `rt-cli` depends on no runtime crate: it has its own serde types and talks HTTP with a blocking `reqwest`. Keep
  it that way.
- Add the subcommand to the noun's `#[derive(Subcommand)]` enum in `src/cli.rs` (e.g. `ServiceCmd`, `ProjectCmd`).
  Implement it in `src/cmd/<noun>.rs` (`pub fn run(app, cmd)`). A new noun is dispatched from `src/main.rs`.
- Use the helpers in `src/cmd/mod.rs`: `resolve` (slug or id), `fetch_page` and `print_page`, `parse_json_arg`.
  `src/client.rs` has `get/post/put/patch/delete` and `parse_api_error`, which maps the error envelope to exit codes
  in `src/error.rs`. `--retry` uses `send_retrying`.
- Tests go in `crates/rt-cli/tests/it/<noun>.rs`. They use an httpmock control API plus `assert_cmd`
  (`Ctx::new()`, `ctx.server.mock(..)`, `ctx.run(&[..])`); see `project.rs`
  `create_posts_the_slug_name_and_environments`. Assert the request the CLI sends and the output and exit code.

## 5. End-to-end tests (`crates/rt-server/tests/`)

- API tests go in `tests/api/<area>.rs`; a new file is registered in `tests/api.rs`. Use the `node_test!` macro
  pattern with `start_node()`, `new_tenant(&node, "pro")`, `new_tenant_scopes`, `get/post/patch/delete`,
  `root_for(&node, tenant, ..)` (operator plus `x-rt-tenant`) and `call`. Helpers are in `tests/common/mod.rs`.
- Cover:
  - the happy path;
  - each refusal (missing scope, other tenant gets 404, restricted key outside its project/env, operator as `sys`
    gets `403 reserved`, quota);
  - idempotent replay and mismatch;
  - audit event written (`GET /v1/services/{s}/events` for service events);
  - OpenAPI validation of the response.
- Edge and boundary cases go in `tests/api/edge_control.rs` / `edge_keys.rs`.
- Run with `cargo test -p rt-server --test api <module>::`, and `cargo test -p rt-cli --test it <noun>::`.

## 6. Docs (same PR)

| File | What to change |
|---|---|
| `docs/api.md` | The "Routes" subsection for the noun, "Control error codes" for new codes, "Plans" if a plan limit changed |
| `docs/cli.md` | The `### <noun>` section under "Commands", "Scopes" if one was added, and **"Control API coverage"** (the `\| Operation \| Command \|` table must list every OpenAPI operation) |
| `docs/mcp.md` | The "Tools" table (`\| Tool \| Scopes \| What it does \| Hints \|`) or the "Not exposed as tools" line |
| `SECURITY.md` | Scopes, MCP scope table, audit events, caps |
| `skills/21ci-deploy/references/commands.md` | When a user-facing command changes |
| `docs/cli.md`, `docs/api.md` | the CLI and control API guides; update them in the same change. `make docs-check` builds the site from them |

## Done when
- [ ] Each check in section 1 is present or deliberately not needed, and the refusals are tested.
- [ ] `openapi/openapi.json` is regenerated and committed; `ROUTE_MAP` / `ERROR_CODES` are updated.
- [ ] The MCP tool (or the decision not to add one) is documented, with scopes equal to the REST route.
- [ ] The `21ci` command, its tests and the `docs/cli.md` coverage table are done.
- [ ] `make lint` passes; `make coverage MODULES="api cli"` covers the route's success, auth/scope refusals and error codes.
