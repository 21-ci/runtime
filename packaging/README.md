# packaging/

Everything that turns a release build into installable artifacts. `.github/workflows/release.yml` drives it; each
piece also runs by hand.

| Path | What | Used by |
|---|---|---|
| [`install.sh`](install.sh) | POSIX `curl \| sh` installer: detects OS/arch/libc, downloads from GitHub Releases, verifies `SHA256SUMS` (and its cosign signature when `cosign` is installed) | users; served at `https://21-ci.github.io/runtime/install.sh` and attached to each release |
| [`nfpm/rt.yaml`](nfpm/rt.yaml) | deb + rpm of `rt`: binary, systemd unit, `/etc/rt/rt.toml`, `/etc/default/rt` or `/etc/sysconfig/rt`, `rt` user, `/var/lib/rt` | release.yml (`nfpm package`) |
| [`nfpm/21ci.yaml`](nfpm/21ci.yaml) | deb + rpm + apk of `21ci` (static musl binary, shell completions) | release.yml |
| [`scripts/`](scripts/) | deb/rpm maintainer scripts of `rt` (create the user, reload systemd, never delete data) | nfpm |
| [`systemd/rt.service`](systemd/rt.service), [`systemd/rt.env`](systemd/rt.env) | hardened unit and its environment file (secrets) | rt deb/rpm |
| [`launchd/io.github.21-ci.rt.plist`](launchd/io.github.21-ci.rt.plist) | launchd agent for macOS installs without Homebrew | users |
| [`homebrew/`](homebrew/) | formula templates `rt.rb.tmpl` (with a `service do` block) and `twentyone-ci.rb.tmpl`, `render.sh` | release.yml → `21-ci/homebrew-tap` |
| [`repo/`](repo/) | `apt.sh`, `rpm.sh`, `apk.sh`: build and sign the package repositories; `index.html` for the `packages/` page | docs.yml (Pages) |
| [`docker/healthcheck.rs`](docker/healthcheck.rs) | the image's HEALTHCHECK probe (std-only Rust, no curl in a distroless image) | Dockerfile |

## Release artifacts

Every `vX.Y.Z` release carries:

| File | Content |
|---|---|
| `rt-X.Y.Z-<target>.tar.gz` | `rt-X.Y.Z-<target>/{rt, LICENSE, NOTICE, README.md, defconfigs/}` for `x86_64`/`aarch64-unknown-linux-gnu`, `x86_64`/`aarch64-apple-darwin` |
| `21ci-X.Y.Z-<target>.tar.gz` | `21ci-X.Y.Z-<target>/{21ci, LICENSE, NOTICE, README.md}` for the four targets above plus `x86_64`/`aarch64-unknown-linux-musl` |
| `*.deb`, `*.rpm`, `*.apk` | the packages (rpm and apk signed) |
| `*.spdx.json` | SPDX SBOM of each tarball (syft) |
| `SHA256SUMS`, `SHA256SUMS.sigstore.json` | checksums of every file above, and their keyless cosign signature bundle |
| `install.sh` | the installer of that release |

Build provenance (SLSA, `actions/attest-build-provenance`) and SBOM attestations are stored with GitHub
attestations for every tarball, package and image: `gh attestation verify <file> --repo 21-ci/runtime`.

### Why no musl `rt`

`rt` links the prebuilt V8 static library of `rusty_v8`, which is published for glibc targets only. `rt` therefore
ships for `*-linux-gnu` (built on Ubuntu 22.04: glibc 2.35 or newer), and there is no `rt` apk. On Alpine, run the
container image. `21ci` has no V8 and ships as a static musl binary.

## Package repositories (GitHub Pages)

One Pages site serves both the docs and the repositories, so `docs.yml` builds both on every deploy: it builds the
docs, downloads the packages of the last three releases, rebuilds and signs the repositories, and publishes:

| URL | Content |
|---|---|
| `https://21-ci.github.io/runtime/packages/gpg.key` | ASCII-armored public key for apt and rpm |
| `https://21-ci.github.io/runtime/packages/apt` | `deb [signed-by=/etc/apt/keyrings/21cloud.gpg] https://21-ci.github.io/runtime/packages/apt stable main` |
| `https://21-ci.github.io/runtime/packages/rpm/21cloud.repo` | dnf/yum repo file (`gpgcheck=1`, `repo_gpgcheck=1`) |
| `https://21-ci.github.io/runtime/packages/apk` | Alpine repository (`21ci`), key `apk/21cloud.rsa.pub` |

Pages sites are limited to 1 GB, hence the three-release window; older packages stay on their GitHub Releases.

## Signing keys and secrets

| Actions secret | What | Create it with |
|---|---|---|
| `PKG_GPG_PRIVATE_KEY` | ASCII-armored private key that signs rpms, `InRelease`/`Release.gpg` and `repomd.xml` | `gpg --quick-gen-key "21 Cloud Packages <packages@…>" rsa4096 sign 3y`, then `gpg --armor --export-secret-keys <id>` |
| `PKG_GPG_PASSPHRASE` | its passphrase (may be empty) | |
| `APK_RSA_PRIVATE_KEY` | unencrypted PEM RSA key that signs apks and `APKINDEX.tar.gz` (`abuild-sign` cannot prompt) | `openssl genrsa -out 21cloud.rsa 4096` |
| `HOMEBREW_TAP_TOKEN` | fine-grained token with `contents: write` on `21-ci/homebrew-tap` only | GitHub → Settings → Developer settings |
| `RELEASE_APP_ID`, `RELEASE_APP_PRIVATE_KEY` | a GitHub App (contents + pull requests: write) that release-please acts as, so its PRs run CI and its tags run release.yml | GitHub → Organization settings → GitHub Apps |
| `CODECOV_TOKEN` | coverage upload (ci.yml) | codecov.io |

Images and release files are signed keyless with cosign (GitHub OIDC): no key to store. Keep an offline backup of
both private keys and publish the GPG fingerprint in [SECURITY.md](../SECURITY.md#verifying-releases).

## By hand

```bash
# packages of a local build (needs nfpm)
VERSION=0.1.0 ARCH=arm64 BIN_DIR=target/release NFPM_GPG_KEY_FILE= \
  nfpm package -f packaging/nfpm/rt.yaml -p deb -t dist/pkg/

# Homebrew formulae for a release
packaging/homebrew/render.sh 0.1.0 SHA256SUMS ../homebrew-tap

# installer from a fork or mirror
RT_INSTALL_REPO=me/runtime sh packaging/install.sh --only 21ci
```
