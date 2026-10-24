# Releasing

How a version of `rt` and `21ci` gets built, signed and published to GitHub Releases, GHCR, Homebrew, the
apt/rpm/apk repositories and the docs site — and the one-time setup that makes it work.

One tag does everything: pushing `vX.Y.Z` runs [`release.yml`](.github/workflows/release.yml), which builds,
scans, signs, publishes and then verifies what it published. Branch rules are in [`.github/BRANCHING.md`](.github/BRANCHING.md);
signing keys and the packaging files are described in [`packaging/README.md`](packaging/README.md).

| Channel | Where | Updated by |
|---|---|---|
| Binaries, SBOMs, checksums | GitHub Releases of `21-ci/runtime` | `release.yml` |
| Container images | `ghcr.io/21-ci/rt`, `ghcr.io/21-ci/21ci` (`X.Y.Z`, `X.Y`, `latest`) | `release.yml` |
| Homebrew | `21-ci/homebrew-tap` (`rt`, `twentyone-ci` + alias `21ci`) | `release.yml`, stable releases only |
| apt, rpm, apk | `https://21-ci.github.io/runtime/packages/` (last three stable releases) | `docs.yml`, dispatched by `release.yml` |
| Installer | `https://21-ci.github.io/runtime/install.sh` and each release | `docs.yml` / `release.yml` |
| Docs | `https://21-ci.github.io/runtime/` | `docs.yml` on every docs change to `main` |

## Publish a version (the normal path)

1. Merge work into `main` through PRs with [Conventional Commit](https://www.conventionalcommits.org) titles
   (`feat:`, `fix:`, `feat!:` for a breaking change).
2. release-please keeps a PR titled **`chore: release X.Y.Z`** open. It bumps the version in `Cargo.toml` and
   `.release-please-manifest.json` and writes `CHANGELOG.md` from the commit titles.
3. Review and merge that PR. The release App tags the merge commit `vX.Y.Z`, and the tag starts `release.yml`.
4. Watch it:

   ```bash
   gh run watch --repo 21-ci/runtime
   ```

5. When the run is green, check the result (see [Verify a release](#verify-a-release)). Nothing else to do: the
   Homebrew formulae, images, packages and Pages repositories are all updated by the run.

What `release.yml` does, in order (a failing step stops everything after it):

| Job | Does |
|---|---|
| `plan` | reads the version from the tag and fails if it differs from `Cargo.toml`; `vX.Y.Z-*` is a pre-release |
| `build` | `rt` + `21ci` for `x86_64`/`aarch64-unknown-linux-gnu` and `x86_64`/`aarch64-apple-darwin`; `21ci` also for `*-linux-musl`; reproducible tarballs with the dependency list embedded |
| `artifacts` | deb + rpm (GPG-signed) of both, apk of `21ci` (RSA-signed), SPDX SBOMs; trivy + grype scan every unpacked tarball, package and SBOM — a fixable HIGH/CRITICAL fails the release |
| `images` | multi-arch images built from the release binaries, pushed by digest, scanned per platform, only then tagged, cosign-signed (keyless) and attested |
| `release` | `SHA256SUMS`, its cosign bundle, SLSA provenance, the GitHub Release with every file |
| `homebrew` | renders `packaging/homebrew/*.tmpl` with the new checksums and pushes them to the tap (stable only) |
| `pages` | dispatches `docs.yml`, which rebuilds and signs the apt/rpm/apk repositories and redeploys the site |
| `verify` | downloads what was published and checks every checksum, signature and attestation |

## Other kinds of release

**Release by hand** (no release-please PR, e.g. the very first release): the version in `Cargo.toml` must
already match the tag.

```bash
git tag -s v0.1.0 -m "v0.1.0"
```

```bash
git push origin v0.1.0
```

Only admins and the release automation may create `v*` tags (`.github/rulesets/tags.json`).

**Pre-release** (`v1.2.0-rc.1`): set the version in `Cargo.toml` to `1.2.0-rc.1`, merge, tag `v1.2.0-rc.1`. It
gets a GitHub pre-release and images tagged `1.2.0-rc.1` and `1.2`, but no `latest` tag, no Homebrew update and no
place in the apt/rpm/apk repositories.

**Patch an older line** (`release/x.y`): cherry-pick the fix onto `release/x.y` through a PR (rules in
[`.github/BRANCHING.md`](.github/BRANCHING.md)). release-please also watches `release/**` and opens a
`chore: release x.y.(z+1)` PR there; merging it tags and releases as above. Note: `release.yml` moves the
`latest` image tag and the Homebrew formulae on every stable release, so a patch of an older line points them at
that older version. Release the newest line again afterwards (or tag the older patch as a pre-release) until the
workflow learns to skip them for older lines.

**Docs only**: merge to `main`. `docs.yml` runs on changes under `docs/`, `mkdocs.yml`, `sample/`, `openapi/`,
`config/`, `wit/`, `packaging/install.sh`, `packaging/repo/` and a few more (see its `paths:`). Redeploy by hand:

```bash
gh workflow run docs.yml --repo 21-ci/runtime
```

**Rebuild the package repositories** (e.g. after changing `packaging/repo/*.sh`): run `docs.yml` as above; it
re-downloads the packages of the last three stable releases and re-signs the repositories.

## When something goes wrong

**A job failed** (flaky runner, network): rerun only the failed jobs. Check the GitHub Release, GHCR and the tap
afterwards: a job that failed half-way may have published part of its output.

```bash
gh run rerun <run-id> --failed --repo 21-ci/runtime
```

**Run the whole release again for an existing tag:**

```bash
gh workflow run release.yml --ref v0.1.0 --repo 21-ci/runtime
```

**A scan blocked the release:** bump or patch the dependency in a PR, merge, and release the next patch version.
Only when a finding does not apply, add it to [`.trivyignore`](.trivyignore) with the reason and an expiry, in a
reviewed PR. Never re-tag the same version with different contents.

**A bad version went out:** do not delete or move the tag (checksums, signatures and caches point at it).

1. Fix forward with the next patch version (the normal path above).
2. Mark the broken release so people stop installing it:

   ```bash
   gh release edit v0.1.1 --repo 21-ci/runtime --prerelease --notes "Broken: use v0.1.2. <reason>"
   ```

3. If it is a security issue, also publish an advisory (see [SECURITY.md](SECURITY.md)).

## Verify a release

Images (keyless signature from this repository's workflow):

```bash
cosign verify ghcr.io/21-ci/rt:0.1.0 --certificate-oidc-issuer https://token.actions.githubusercontent.com --certificate-identity-regexp '^https://github.com/21-ci/runtime/'
```

Release files (checksums, their signature, build provenance):

```bash
gh release download v0.1.0 --repo 21-ci/runtime --dir /tmp/rt-0.1.0
```

```bash
cd /tmp/rt-0.1.0 && sha256sum -c SHA256SUMS --ignore-missing
```

```bash
cosign verify-blob SHA256SUMS --bundle SHA256SUMS.sigstore.json --certificate-oidc-issuer https://token.actions.githubusercontent.com --certificate-identity-regexp '^https://github.com/21-ci/runtime/'
```

```bash
gh attestation verify rt-0.1.0-x86_64-unknown-linux-gnu.tar.gz --repo 21-ci/runtime
```

Each channel installs the new version:

| Channel | Command |
|---|---|
| Homebrew | `brew update && brew upgrade 21-ci/tap/rt 21-ci/tap/21ci` |
| curl | `curl -fsSL https://21-ci.github.io/runtime/install.sh \| sh` |
| apt | `sudo apt update && sudo apt install --only-upgrade rt 21ci` |
| dnf | `sudo dnf upgrade rt 21ci` |
| apk | `apk update && apk upgrade 21ci` |
| Docker | `docker pull ghcr.io/21-ci/rt:0.1.0` |

## One-time setup

Done once per organization. Needs `gh` (signed in), `gpg` and `openssl`.

### 1. Repositories

Create the organization **21-ci**, then the repository and the Homebrew tap (the tap needs one commit, so
`--add-readme`):

```bash
gh repo create 21-ci/runtime --public --description "Self-hosted serverless runtime for TypeScript and WebAssembly"
```

```bash
gh repo create 21-ci/homebrew-tap --public --add-readme --description "Homebrew formulae for rt and 21ci"
```

Push the code before importing the rulesets (step 6), which then allow only signed commits through PRs:

```bash
git push origin main
```

### 2. Signing keys

Images and release files are signed keyless (GitHub OIDC): no key. The package repositories need two keys. Keep
offline backups of both private keys (for example in a password manager) and delete the local copies once they are
stored as secrets: losing them forces every user to trust a new key.

GPG key for apt and rpm (choose a passphrase):

```bash
gpg --quick-gen-key "21 Cloud Packages <packages@YOUR-DOMAIN>" rsa4096 sign 3y
```

```bash
gpg --armor --export-secret-keys "21 Cloud Packages" > pkg-gpg-private.asc
```

Its fingerprint goes into [SECURITY.md](SECURITY.md#verifying-releases) (`REPLACE-WITH-THE-PACKAGE-KEY-FINGERPRINT`):

```bash
gpg --fingerprint "21 Cloud Packages"
```

RSA key for apk (unencrypted: `abuild-sign` cannot prompt; the public half is derived during the build):

```bash
openssl genrsa -out 21cloud.rsa 4096
```

### 3. Actions secrets

```bash
gh secret set PKG_GPG_PRIVATE_KEY --repo 21-ci/runtime < pkg-gpg-private.asc
```

```bash
gh secret set PKG_GPG_PASSPHRASE --repo 21-ci/runtime
```

```bash
gh secret set APK_RSA_PRIVATE_KEY --repo 21-ci/runtime < 21cloud.rsa
```

`HOMEBREW_TAP_TOKEN`: GitHub → Settings → Developer settings → Fine-grained tokens → New; resource owner
**21-ci**, repository **homebrew-tap** only, permission **Contents: Read and write**.

```bash
gh secret set HOMEBREW_TAP_TOKEN --repo 21-ci/runtime
```

`RELEASE_APP_ID` and `RELEASE_APP_PRIVATE_KEY`: organization Settings → Developer settings → GitHub Apps → New.
No webhook; permissions **Contents** and **Pull requests**: Read and write. Create it, generate a private key,
install it on `21-ci/runtime`. release-please acts as this App so that its PRs run CI and its tags start
`release.yml` (pushes made with the default `GITHUB_TOKEN` trigger no workflows).

```bash
gh secret set RELEASE_APP_ID --repo 21-ci/runtime
```

```bash
gh secret set RELEASE_APP_PRIVATE_KEY --repo 21-ci/runtime < release-app.private-key.pem
```

Optional, for the coverage upload in `ci.yml` (token from codecov.io):

```bash
gh secret set CODECOV_TOKEN --repo 21-ci/runtime
```

### 4. GitHub Pages

Repository → Settings → Pages → **Source: GitHub Actions** (this creates the `github-pages` environment), then:

```bash
gh workflow run docs.yml --repo 21-ci/runtime
```

The docs appear at `https://21-ci.github.io/runtime/`; `packages/` fills in with the first stable release.

### 5. Placeholders

Replace `REPLACE-WITH-PROJECT-DOMAIN` (in `SECURITY.md`, `CODE_OF_CONDUCT.md`, `packaging/nfpm/*.yaml`) with the
project's domain or contact addresses, and the key fingerprint in `SECURITY.md`:

```bash
git grep -n "REPLACE-WITH-"
```

### 6. Branch and tag rules

Repository → Settings → Rules → Rulesets → **New ruleset → Import a ruleset**, once for each of
`.github/rulesets/main.json`, `release-branches.json` and `tags.json`.

### 7. After the first release

GHCR images of an organization start private. Make both public once: github.com/orgs/21-ci/packages → **rt** →
Package settings → **Change visibility → Public**; the same for **21ci**.

## Keys: rotation and expiry

The GPG key above expires after three years. Before it does, either extend it (`gpg --quick-set-expire <fingerprint> 3y`,
export again, update `PKG_GPG_PRIVATE_KEY`, run `docs.yml`) or rotate: generate a new key, publish its fingerprint
in `SECURITY.md` and the release notes, update the secret, run `docs.yml`. Users re-download `packages/gpg.key`.
Rotating the apk key works the same way with `APK_RSA_PRIVATE_KEY`; Alpine users fetch the new
`packages/apk/21cloud.rsa.pub`.
