# Agent skills for the 21 Cloud runtime

Skills that teach a coding agent (Claude Code, or any agent that reads Markdown instructions) to write, deploy and debug
services for the [21 Cloud Functions Runtime](https://github.com/21-ci/runtime): a self-hosted, multi-tenant functions
runtime that runs TypeScript/JavaScript in Deno isolates and WASM components (WASI P2 `wasi:http`) in wasmtime.

| Skill | Use it for |
|---|---|
| [`21cloud-deno-service`](21cloud-deno-service/SKILL.md) | Writing or debugging a TypeScript/JavaScript service: handler forms, `ctx`, triggers, the `std:*` modules (KV, SQL, blob, queue, schedule, locks, sockets, durable objects, service calls), limits and error codes |
| [`21cloud-wasm-service`](21cloud-wasm-service/SKILL.md) | Writing or debugging a WASM component service in Rust (`wasm32-wasip2`) or JavaScript (`jco componentize`): the `wasi:http` contract, the `std:host` WIT interfaces, RPC and socket exports |
| [`21ci-deploy`](21ci-deploy/SKILL.md) | Deploying and operating services with the `21ci` CLI: contexts, `push`, config blocks, env and secrets, schedules, queues, logs, rollbacks, CI usage and exit codes |

Each skill is a directory with a `SKILL.md` (YAML frontmatter `name` + `description`, then instructions) and a
`references/` directory the instructions link to. Every API, limit, flag and error code in them was checked against
the source of this repository.

## Install

Claude Code loads skills from `~/.claude/skills/` (all projects) or `.claude/skills/` (one project). Copy the
directories you want:

```bash
git clone https://github.com/21-ci/runtime.git
mkdir -p ~/.claude/skills
cp -R runtime/skills/21cloud-deno-service runtime/skills/21cloud-wasm-service runtime/skills/21ci-deploy ~/.claude/skills/

# or only for one project
mkdir -p .claude/skills && cp -R runtime/skills/21cloud-deno-service .claude/skills/
```

Keep each directory whole: `SKILL.md` links to the files in its `references/`. The skills link to repository files
(samples, WIT) by absolute GitHub URL, so they work outside a checkout.

Other agents (Cursor, Codex, Aider, custom agents) have no skill loader: point them at the `SKILL.md` file directly,
for example from your project's `AGENTS.md`:

```markdown
When writing code for the 21 Cloud runtime, follow skills/21cloud-deno-service/SKILL.md
(and its references/) from https://github.com/21-ci/runtime.
```

## Also useful

- [`llms.txt`](../llms.txt): an index of the repository for LLMs.
- The MCP server (`POST /mcp` on the control API, or `21ci mcp` for stdio) lets an assistant deploy and read logs
  directly: see [docs/mcp.md](../docs/mcp.md). The `21ci-deploy` skill covers both paths.
- Docs: https://21-ci.github.io/runtime/
