# syntax=docker/dockerfile:1.7
#
# 21 Cloud Functions Runtime: the node `rt` (one process, --role=all) on a distroless base.
#
#   make docker-build                     # docker build -t rt:dev .            (compiles from source)
#   docker build --build-arg BIN_STAGE=prebuilt -t rt:x.y.z .                 (release: binaries from dist/docker/)
#
# Stages:
#   builder    rust:1.95-trixie            cargo build --release -p rt-server (registry + target in BuildKit caches)
#   prebuilt   scratch                     release binaries copied from dist/docker/<arch>/ (.github/workflows/release.yml)
#   fs         debian:trixie-slim          an empty /data owned by uid 10001 (no shell in the final image)
#   runtime    distroless/cc-debian13      glibc + libgcc + CA certificates; rusty_v8 needs glibc, not musl
#
# The final image has no shell and no package manager, runs as uid 10001, works with a read-only root filesystem
# (`docker run --read-only`, only /data is written) and carries its own HEALTHCHECK probe (packaging/docker/healthcheck.rs).
# Base images are pinned by digest; dependabot (docker ecosystem) bumps them.

ARG RUST_IMAGE=rust:1.95-trixie@sha256:f49565f188ee00bc2a18dd418183f2c5f23ef7d6e691890517ed341a598f67c3
ARG FS_IMAGE=debian:trixie-slim@sha256:62bd423aeee912639ca8ee681c2b762cb3e25137e0a92b20be455898a9f034ad
ARG RUNTIME_IMAGE=gcr.io/distroless/cc-debian13:nonroot@sha256:83b9b50586253821cf63ffcec87d3d94265a093ad30da232039af962dd8f4edf
# builder (default: compile here) or prebuilt (copy dist/docker/<arch>/{rt,rt-healthcheck}).
ARG BIN_STAGE=builder

# ---------------------------------------------------------------------------------------------
# Binaries from source
# ---------------------------------------------------------------------------------------------
FROM ${RUST_IMAGE} AS builder

WORKDIR /src

# The image already ships Rust 1.95; rust-toolchain.toml would make rustup fetch another copy
# (plus the wasm32-wasip2 target) on the first cargo call. The server build needs neither.
ENV CARGO_INCREMENTAL=0 \
    CARGO_NET_RETRY=5 \
    CARGO_TERM_COLOR=never

COPY . .
RUN rm -f rust-toolchain.toml

# The target dir lives in a cache mount, so it is NOT part of the layer: copy the binary out in the
# same RUN. build.rs of rusty_v8 downloads the prebuilt V8 static library, so this stage needs network.
RUN --mount=type=cache,target=/usr/local/cargo/registry,sharing=locked \
    --mount=type=cache,target=/usr/local/cargo/git,sharing=locked \
    --mount=type=cache,target=/src/target,sharing=locked \
    cargo build --release --locked -p rt-server --bin rt \
 && install -D -m 0755 target/release/rt /out/rt \
 && rustc --edition 2021 -C opt-level=s -C strip=symbols -o /out/rt-healthcheck packaging/docker/healthcheck.rs

# ---------------------------------------------------------------------------------------------
# Binaries from a release build (CI): dist/docker/amd64/rt, dist/docker/arm64/rt, ...
# ---------------------------------------------------------------------------------------------
FROM scratch AS prebuilt
ARG TARGETARCH
COPY --chmod=0755 dist/docker/${TARGETARCH}/rt dist/docker/${TARGETARCH}/rt-healthcheck /out/

FROM ${BIN_STAGE} AS bin

# ---------------------------------------------------------------------------------------------
# Writable data directory (built on the build platform: no emulation needed)
# ---------------------------------------------------------------------------------------------
FROM --platform=$BUILDPLATFORM ${FS_IMAGE} AS fs
RUN install -d -o 10001 -g 10001 -m 0750 /out/data \
 && install -d -m 0755 /out/etc-rt

# ---------------------------------------------------------------------------------------------
# Runtime
# ---------------------------------------------------------------------------------------------
FROM ${RUNTIME_IMAGE} AS runtime

ARG VERSION=dev
ARG REVISION=unknown
LABEL org.opencontainers.image.title="rt" \
      org.opencontainers.image.description="21 Cloud Functions Runtime node: multi-tenant Deno isolates and WASM components" \
      org.opencontainers.image.source="https://github.com/21-ci/runtime" \
      org.opencontainers.image.documentation="https://21-ci.github.io/runtime/" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.vendor="21 Cloud" \
      org.opencontainers.image.version="${VERSION}" \
      org.opencontainers.image.revision="${REVISION}"

COPY --from=bin /out/rt /usr/local/bin/rt
COPY --from=bin /out/rt-healthcheck /usr/local/bin/rt-healthcheck
COPY --from=fs --chown=10001:10001 /out/data /data
COPY --from=fs /out/etc-rt /etc/rt
# Reference configs (balanced, max_density, min_latency, edge-1gb, dev) and the production default.
COPY config/defconfigs /etc/rt/defconfigs
COPY deploy/rt.toml /etc/rt/defconfigs/production.toml

# Config file: rt reads $RT_DATA/rt.toml when it exists, so mounting a file at /etc/rt/rt.toml (read-only) is enough;
# without it the node runs on the built-in defaults (profile `balanced`). `--config` / RT_CONFIG still win.
#
# Inside a container every listener binds 0.0.0.0, otherwise `-p` mappings cannot reach it (the built-in defaults bind
# control and metrics to 127.0.0.1). The RT__ overlay wins over rt.toml, so these hold even with a mounted file; publish
# 8081 and 9090 on loopback only (deploy/compose does).
#
# The prebuilt V8 has no sandbox, and the built-in `security.require_v8_sandbox=true` would refuse to start
# (SECURITY.md, "Known limitations"). Drop RT__SECURITY__REQUIRE_V8_SANDBOX when a sandboxed V8 ships.
ENV RT_DATA=/etc/rt \
    HOME=/data \
    RT__NODE__DATA_DIR=/data \
    RT__LISTENERS__PUBLIC=0.0.0.0:8080 \
    RT__LISTENERS__INTERNAL=unix:/data/rt.sock \
    RT__LISTENERS__CONTROL=0.0.0.0:8081 \
    RT__LISTENERS__METRICS=0.0.0.0:9090 \
    RT__SECURITY__REQUIRE_V8_SANDBOX=false \
    RT_HEALTHCHECK_ADDR=127.0.0.1:9090 \
    RT_LOG=info

# Secrets are never baked in: pass RT_ROOT_TOKEN and RT_MASTER_KEY (64 hex chars, `openssl rand -hex 32`) at run
# time, or RT_ROOT_TOKEN_FILE / RT_MASTER_KEY_FILE pointing at mounted files (Docker/Kubernetes secrets).

# meta.db, queue.db, artifacts/, services/, blobs/, logs/, cache/, deps/. A bind mount must be writable by uid 10001.
VOLUME ["/data"]
WORKDIR /data

# 8080 public invoke, 8081 control API (+ /docs), 9090 metrics (/metrics /healthz /readyz).
EXPOSE 8080 8081 9090

# Liveness: the sysmon heartbeat. /readyz is the load-balancer probe; it is not used here so that a slow start or a
# drain does not get the container restarted.
HEALTHCHECK --interval=10s --timeout=3s --start-period=30s --retries=3 CMD ["/usr/local/bin/rt-healthcheck"]

USER 10001:10001
STOPSIGNAL SIGTERM
ENTRYPOINT ["/usr/local/bin/rt"]
CMD ["serve"]
