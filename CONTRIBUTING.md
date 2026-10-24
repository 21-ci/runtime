# Contributing

Thanks for helping. This guide covers the dev setup, the checks a change must pass, and how a change gets merged.
Questions go to [Discussions](https://github.com/21-ci/runtime/discussions); security bugs go through
[SECURITY.md](SECURITY.md), never a public issue.

## Ground rules

- Open an issue (or a discussion) before a large change, so the design is agreed before the code.
- One logical change per PR. Small PRs are reviewed faster.
- Every change covers **its main logic** with tests (request paths, rules, limits, user-visible errors, auth checks); CI fails a module below **90 % lines**. A line that cannot run in a test carries
  `// LCOV_EXCL_LINE: <reason>` (or a `// LCOV_EXCL_START: <reason>` ... `// LCOV_EXCL_STOP` block); the reason is
  mandatory and is reviewed like code.
- Behaviour visible to users (API, `std:*` modules, CLI, config keys, limits) changes together with its docs, the
  OpenAPI document (`make openapi`) and, when it touches a trust boundary, [SECURITY.md](SECURITY.md).
- No CLA. Commits are signed off under the [DCO](#developer-certificate-of-origin).

## Dev setup

| Need | Why |
|---|---|
| Rust 1.95 (installed from `rust-toolchain.toml` by rustup, with `clippy`, `rustfmt`, `llvm-tools`, target `wasm32-wasip2`) | the workspace and the WASM fixtures |
| Python 3.10+ | config tooling (`tools/`), the docs site |
| Docker with Compose v2 and BuildKit, `curl`, `jq` | images and the black-box e2e suite |
| `cargo-llvm-cov` (`cargo install cargo-llvm-cov`) | coverage |
| optional: `oha` (benchmarks), `cargo-deny`, `shellcheck`, `actionlint` | what CI runs |

```bash
git clone https://github.com/21-ci/runtime && cd runtime
make fixtures        # WASM guest fixtures (fixtures/wasm/out), needed by the executor tests
make build           # cargo build --workspace (first build downloads the prebuilt V8: allow 10+ min)
make dev             # local node: control :8081 (token dev-root-token), invoke :8080, metrics :9090
```

V8 and wasmtime make the target directory large (20+ GB for a full debug build plus tests). Test crate by crate when
disk is tight, and keep one shared `target/`.

## Make targets

`make help` lists all of them. The ones you need daily:

| Target | Does |
|---|---|
| `make build` / `make check` | `cargo build --workspace` / `cargo check --workspace --all-targets` |
| `make fmt` / `make lint` | `cargo fmt --all` / `fmt --check` + `clippy --workspace --all-targets -D warnings` |
| `make test` | WASM fixtures + `cargo test --workspace` + Python tool tests (`scripts/test.sh --fast` skips fixtures, doc-tests and Python) |
| `make coverage` | tests under coverage: per-module table + `target/coverage/lcov.info` |
| `make e2e` | builds the image, starts a compose project with fresh secrets, runs `tests/e2e/run_all.sh` |
| `make openapi` | regenerates `openapi/openapi.json` (a test fails when it drifts) |
| `make fixtures` | rebuilds the WASM fixtures (after any change in `wit/` or `fixtures/wasm/`) |
| `make dev` / `make run` | a local node on `config/defconfigs/dev.toml` (`WATCH=1` reloads) |

## Tests

```bash
cargo test -p rt-sched                         # one crate (preferred while iterating)
cargo test -p rt-server --test flows websockets # one module of a crate's integration binary
make test                                      # everything CI runs in the test job
sample/apps/run-smokes.sh target/debug/rt      # the sample-app smokes against a real node
```

- **Unit tests** live next to the code (`#[cfg(test)] mod tests`).
- **Integration tests**: crates with many of them have one test binary with a module tree (`crates/<crate>/tests/it/`,
  or `crates/rt-server/tests/{api,flows,process}/`). Add a module there; do not add a new test binary, each one links
  V8 or wasmtime again.
- **Edge-case suites**: the should-pass / should-fail cases of each module are `edge_*` modules in the crate's
  integration tests (for example `crates/rt-server/tests/api/edge_control.rs` and
  `crates/rt-server/tests/flows/edge_invoke.rs`). They run in the normal `cargo test`; add a case there when you fix
  a bug at a boundary.
- **Black-box e2e**: `tests/e2e/*.sh` against the Docker image (`make e2e`).
- **Perf tests** are `#[ignore]`d and run in release mode: `make test-perf`.
- A test that fails only under load (for example a p99 latency assertion) is rerun alone before it is called a
  failure; a real flake gets an issue, not a retry loop.

### Coverage

```bash
make coverage                                  # whole workspace, per-module table
make coverage MODULES=sched ARGS=--missing     # one module, uncovered line ranges
scripts/coverage.sh --fail-under 90 --floor tools=45 --exclusions    # what CI runs: fails a module below 90 %, prints every exclusion
```

The report is `target/coverage/lcov.info` (plus `--html`). CI uploads it to Codecov.

## Commit style

[Conventional Commits](https://www.conventionalcommits.org/): `type(scope): summary` in the imperative, at most 72
characters, then a short body that says why.

- Types: `feat`, `fix`, `perf`, `refactor`, `test`, `docs`, `build`, `ci`, `chore`, `revert`.
- Scopes are the area: `sched`, `store`, `api`, `cli`, `deno`, `wasm`, `build`, `host`, `sec`, `config`, `docs`, ...
- `feat!:` or a `BREAKING CHANGE:` footer marks a breaking change. release-please builds `CHANGELOG.md` and the
  version from these messages, so they matter.

```
fix(store): a deleted service's data never comes back

Deleting a service now removes its data.db before the row, so a crash in between
leaves an orphan file that the startup sweep removes, never a live row without data.

Signed-off-by: Ada Lovelace <ada@example.com>
```

## Developer Certificate of Origin

Every commit carries a `Signed-off-by:` line (`git commit -s`), which certifies the
[Developer Certificate of Origin 1.1](https://developercertificate.org/): you wrote the change or have the right to
submit it under the project's license (Apache-2.0). The DCO check fails a PR with an unsigned-off commit; fix it with
`git rebase --signoff main` and a force push to your branch.

`main` also requires signed commits (GPG or SSH,
[GitHub docs](https://docs.github.com/en/authentication/managing-commit-signature-verification)).

## Pull requests

1. Fork, branch from `main` (`fix/…`, `feat/…`), push, open a PR against `main`. Backports to a `release/x.y` branch
   are separate PRs (see [.github/BRANCHING.md](.github/BRANCHING.md)).
2. Fill in the PR template. CI must be green: fmt, clippy, tests, coverage, fixtures, smokes, docs build and the
   security checks.
3. One approving review from a code owner. `main` is linear: PRs are squash- or rebase-merged.

## Docs

The docs site is built from this repository: `mkdocs.yml` at the root, pages in `docs/` (the guides `api.md`, `cli.md`,
`config.md`, `deploy.md`, `handlers.md` and `mcp.md` are the reference pages). It is published to GitHub Pages by
`.github/workflows/docs.yml`. Build it locally with `make docs-build` (a venv in `target/docs-venv`, strict: any warning
fails), preview with `make docs-serve`, and check with `make docs-check`. Internal design notes are not in this
repository, so do not link to them. Agent skills for service authors live in [skills/](skills/).

## Coding agents

Claude Code skills for contributors live in `.claude/skills/` (security review, performance, adding a guest API,
control API changes, testing, config changes); [AGENTS.md](AGENTS.md) is the entry point for any coding agent. Agent-written
changes follow the same rules as everyone else's: tests that cover the logic, docs, DCO sign-off by the human who submits them.

## License

By contributing you agree that your contributions are licensed under the [Apache License 2.0](LICENSE).

## Releases

Maintainers publish versions as described in [RELEASING.md](RELEASING.md): one `vX.Y.Z` tag builds, signs and
publishes binaries, images, packages, Homebrew formulae and the docs site.
