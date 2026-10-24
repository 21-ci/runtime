<!-- Title: a Conventional Commit, e.g. `fix(store): a deleted service's data never comes back`. -->

## What and why

<!-- What changes, and why. Link the issue: Fixes #123. -->

## How it was tested

<!-- Commands you ran, new test modules, manual checks. -->

## Checklist

- [ ] Tests added or updated (unit, the crate's `tests/it` modules, `edge_*` cases for boundaries); `make test` passes
- [ ] The changed logic is covered and each module stays at or above 90 % lines (`make coverage MODULES=<crate> ARGS=--missing`); every exclusion has
      `// LCOV_EXCL_LINE: <reason>`
- [ ] `make lint` passes (fmt + clippy `-D warnings`)
- [ ] Docs updated for user-visible changes (docs site, README module table, `make openapi`, `config/schema.json`)
- [ ] SECURITY.md updated if a trust boundary, limit or default changed
- [ ] Commits follow Conventional Commits and are signed off (`git commit -s`, DCO)
- [ ] Breaking change? Marked with `!` / `BREAKING CHANGE:` and described above
