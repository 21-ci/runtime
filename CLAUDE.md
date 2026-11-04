# CLAUDE.md

Read [AGENTS.md](AGENTS.md) first. It holds the layout, build and test commands, conventions and security rules for this repository. This file only adds the workflow rules below.

## Workflow

### CodeGraph

This repo has a `.codegraph/` index. Before grep or reading files to understand or locate code, use
`codegraph_explore` (MCP tool) or `codegraph explore "<symbols or question>"` (shell). This holds when delegating to
subagents too: tell them to use CodeGraph first.

### TDD

Write the unit and integration tests before the code. Run them only when a big module is finished, not after every
edit. Then run the narrowest suite that covers the module (see AGENTS.md, `scripts/test.sh --fast -- -p <crate>`), and
`make lint` plus the module's coverage check before the change is done.
