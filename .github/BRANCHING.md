# Branches, tags and releases

Trunk-based: everything lands on `main` through pull requests; releases are tags; `release/x.y` branches exist only
to carry patch backports.

## Branches

| Branch | Purpose | Rules (ruleset in `.github/rulesets/`) |
|---|---|---|
| `main` | trunk; always releasable | PRs only, 1 approving review (code owner), stale approvals dismissed, conversations resolved, required check `CI result`, linear history (squash or rebase merge), signed commits, no force push, no deletion |
| `release/x.y` | patch backports for the `x.y` line | same as `main`; created from the `vx.y.0` tag when the first backport is needed |
| `feat/…`, `fix/…`, … | work branches in forks or the repo | none; deleted after merge |

## Tags

- `vX.Y.Z` (semver, `v` prefix) marks a release. Pushing it runs `.github/workflows/release.yml`.
- Pre-releases: `vX.Y.Z-rc.N`; the GitHub Release is marked as a pre-release and the Homebrew tap, the package
  repositories and the `latest` image tags are not updated.
- Tags are protected (`rulesets/tags.json`): only maintainers create them, nobody moves or deletes them.

## How a release happens

1. Conventional Commits on `main` feed **release-please** (`.github/workflows/release-please.yml`), which keeps an open
   release PR that bumps the workspace version in `Cargo.toml` and updates `CHANGELOG.md`.
2. A maintainer merges the release PR. release-please creates the `vX.Y.Z` tag and a draft GitHub Release.
3. The tag runs `release.yml`: cross builds, tarballs + checksums, SBOMs, cosign signatures and SLSA provenance,
   deb/rpm/apk packages and their repositories on GitHub Pages, multi-arch GHCR images, scans that block on
   HIGH/CRITICAL findings, the Homebrew tap bump, and a final job that verifies every signature.

## Backporting a fix

```bash
git switch -c release/1.4 v1.4.0          # once per line, then push it (a maintainer)
git switch -c backport/1234-to-1.4 release/1.4
git cherry-pick -x <sha-on-main>          # keep the original Conventional Commit message
git push -u origin backport/1234-to-1.4   # PR against release/1.4
```

release-please runs on `release/*` branches too and opens the `1.4.x` release PR there.

## Applying the rulesets

The JSON files in [`rulesets/`](rulesets/) use the GitHub rulesets REST schema. Import them under
**Settings → Rules → Rulesets → New ruleset → Import a ruleset**, or with the API:

```bash
for f in .github/rulesets/*.json; do
  gh api -X POST repos/21-ci/runtime/rulesets --input "$f"
done
```

Create the `@21-ci/maintainers` and `@21-ci/security` teams first (CODEOWNERS). `tags.json` has an empty
bypass list, so it imports into any organization (actor IDs differ between accounts). After importing it, open the
ruleset → **Bypass list → Add bypass** and add **Repository admin** (so maintainers can push a tag by hand) and the
release GitHub App that release-please acts as (`RELEASE_APP_ID`; it creates the release tags, and its tag push starts
`release.yml`). An App only shows up there once it is installed on the repository. The required check name `CI result` is the
final job of `.github/workflows/ci.yml`; rename both together.
