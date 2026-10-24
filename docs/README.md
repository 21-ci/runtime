# Documentation

The documentation site (https://21-ci.github.io/runtime/) is built from this directory with [`mkdocs.yml`](../mkdocs.yml)
at the repository root. The pages are Markdown files here; the navigation is in `mkdocs.yml`.

Six pages are read on GitHub as well as on the site. They are the reference, not copies: the site links to them.

| Page | Topic |
|---|---|
| [api.md](api.md) | Control and invoke API: authentication, scopes, every route, error codes, invoke reasons |
| [cli.md](cli.md) | `21ci` command line client: install, contexts, every command, CI usage, exit codes |
| [config.md](config.md) | `rt.toml`: profiles, live vs restart keys, `make menuconfig` and `defconfig` |
| [deploy.md](deploy.md) | Running a node: Compose, Kubernetes, packages, environment, ports, metrics, backup, upgrade, security checklist |
| [handlers.md](handlers.md) | Guest code: Deno handler forms, `ctx`, `std:*` modules, limits, WASM component contract |
| [mcp.md](mcp.md) | MCP server for AI assistants: connecting clients, tools, transport |

Everything else under `docs/` is a site page (concepts, guides, tutorials, reference, FAQ, about). The `docs/assets/`
folder holds the site's logo, stylesheet and the API explorer script; `docs/assets/openapi.json` is generated on every
build from [`openapi/openapi.json`](../openapi/openapi.json) by [`scripts/docs_hooks.py`](../scripts/docs_hooks.py) and
is not committed.

## Build

```bash
make docs-build    # strict build into target/site: any warning (broken link or anchor, missing snippet) fails
make docs-serve    # live preview at http://127.0.0.1:8000
make docs-check    # the strict build, plus a check that the site mentions no internal notes
```

Snippets in the pages (`--8<-- "sample/deno/hello/main.ts"`) include the real files of the repository, relative to its
root. A change to a sample, to `wit/rt-host.wit` or to a defconfig shows up in the site on the next build; check the
line ranges of included code after editing those files.

## Editing

- Every page has a level-1 heading, which becomes its title in the navigation.
- Link to other pages with relative `.md` paths. Link to files outside this directory (samples, `SECURITY.md`,
  `deploy/`) with their GitHub URL, because the site only contains `docs/`.
- Do not link to design notes or handoffs: they are not part of this repository.
- The tables in `api.md`, `deploy.md` and `config.md` are parsed by tests in `crates/`. Keep their columns and row
  names when you edit them, and run the crate tests that read them after the change.
