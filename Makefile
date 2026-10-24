# 21 Cloud Functions Runtime: developer entry points. `make` (or `make help`) lists them.
#
# Most targets are thin wrappers: the logic lives in scripts/*.sh (bash, `--help` on each) so that
# CI and humans run exactly the same thing. Configuration tooling (menuconfig, defconfig,
# validate-config, ...) lives in mk/config.mk.

.DEFAULT_GOAL := help

CARGO     ?= cargo
DATA_DIR  ?= data
IMAGE     ?= rt:dev
CONTAINER ?= rt-dev

.PHONY: help build release cli cli-install check lint fmt test test-perf coverage ci-local ci-fast fixtures run dev \
        docker-build docker-run docker-stop docker-logs e2e bench openapi docs docs-venv docs-build docs-serve docs-preview \
        docs-check clean-data

help:
	@echo "Runtime targets:"
	@echo ""
	@echo "Build and check"
	@echo "  make build         cargo build --workspace (debug)"
	@echo "  make release       cargo build --release -p rt-server (target/release/rt)"
	@echo "  make cli           cargo build -p rt-cli (target/debug/21ci; RELEASE=1 for target/release/21ci)"
	@echo "  make cli-install   cargo install --path crates/rt-cli (puts 21ci in ~/.cargo/bin; docs/cli.md)"
	@echo "  make check         cargo check --workspace --all-targets"
	@echo "  make lint          cargo fmt --check + clippy -D warnings (what CI runs)"
	@echo "  make fmt           cargo fmt --all"
	@echo ""
	@echo "Test"
	@echo "  make test          WASM fixtures + cargo test --workspace + python tool tests (scripts/test.sh)"
	@echo "                     fast variant: scripts/test.sh --fast"
	@echo "  make test-perf     the perf-ignored tests in release mode (scripts/test.sh --perf-only)"
	@echo "  make coverage      tests under coverage + lcov report (MODULES=\"sched store\" ARGS=\"--html --missing\")"
	@echo "  make ci-fast       before a push: fmt, clippy, every security scanner, docs (a few minutes)"
	@echo "  make ci-local      the whole CI locally: + all tests under coverage and the sample smokes (STAGES=\"...\")"
	@echo "  make fixtures      build fixtures/wasm/out/*.wasm (scripts/build-fixtures.sh)"
	@echo "  make e2e           docker build + compose up + tests/e2e/run_all.sh + down (scripts/e2e.sh)"
	@echo "  make bench         list the PRF load scenarios; run one: scripts/bench.sh prf-01 (needs oha)"
	@echo ""
	@echo "Run"
	@echo "  make run           local node on CONFIG if it exists, else the dev defconfig (scripts/dev.sh)"
	@echo "  make dev           local dev node: dev defconfig, RT_LOG, generated dev master key; WATCH=1 reloads"
	@echo "  make docker-build  build the image IMAGE (default rt:dev)"
	@echo "  make docker-run    run it as CONTAINER (default rt-dev) on 127.0.0.1:8080/8081/9090"
	@echo "  make docker-logs   follow the container logs"
	@echo "  make docker-stop   stop and remove the container (data volume kept)"
	@echo "  docker compose up -d --build   the same through docker-compose.yml (secrets in .env, see .env.example)"
	@echo ""
	@echo "Docs and API"
	@echo "  make openapi       regenerate openapi/openapi.json from the code (rt openapi)"
	@echo "  make docs          print where the specs, guides and samples live"
	@echo "  make docs-build    strict MkDocs build of the site into target/site (venv in target/docs-venv)"
	@echo "  make docs-serve    live preview of the site at http://127.0.0.1:8000 (DOCS_ADDR to change)"
	@echo "  make docs-preview  build once, serve the static site at http://127.0.0.1:8000 (fast page loads)"
	@echo "  make docs-check    strict build, then fail if the site mentions internal notes"
	@echo "  make clean-data    remove DATA_DIR (default ./data, local dev data only)"
	@echo ""
	@echo "Configuration (mk/config.mk, options CONFIG=rt.toml PROFILE=balanced)"
	@echo "  make menuconfig    curses TUI           make config          line-by-line prompts"
	@echo "  make defconfig     print a profile      make oldconfig       fill missing keys"
	@echo "  make savedefconfig minimal diff         make listprofiles    list profiles"
	@echo "  make validate-config                    make test-config     tooling unit tests"
	@echo "  make help-config   details of the above"
	@echo ""
	@echo "Variables: CARGO=$(CARGO)  DATA_DIR=$(DATA_DIR)  IMAGE=$(IMAGE)  CONTAINER=$(CONTAINER)"

build:
	$(CARGO) build --workspace

release:
	$(CARGO) build --release -p rt-server --bin rt

# The `21ci` command line client (docs/cli.md). Light: no V8/wasmtime in its dependency tree.
cli:
	$(CARGO) build $(if $(RELEASE),--release) -p rt-cli --bin 21ci

cli-install:
	$(CARGO) install --path crates/rt-cli --locked

check:
	$(CARGO) check --workspace --all-targets

lint:
	$(CARGO) fmt --all -- --check
	$(CARGO) clippy --workspace --all-targets -- -D warnings

fmt:
	$(CARGO) fmt --all

test:
	scripts/test.sh

test-perf:
	scripts/test.sh --perf-only

coverage:
	scripts/coverage.sh $(MODULES) $(ARGS)

ci-local:
	scripts/ci-local.sh $(STAGES)

ci-fast:
	scripts/ci-local.sh --fast

fixtures:
	scripts/build-fixtures.sh

run:
	@if [ -f "$(CONFIG)" ]; then \
	  DATA_DIR="$(DATA_DIR)" exec scripts/dev.sh --config "$(CONFIG)"; \
	else \
	  echo "run: $(CONFIG) not found, using config/defconfigs/dev.toml (create one: make defconfig PROFILE=balanced > $(CONFIG))" >&2; \
	  DATA_DIR="$(DATA_DIR)" exec scripts/dev.sh; \
	fi

dev:
	DATA_DIR="$(DATA_DIR)" scripts/dev.sh $(if $(WATCH),--watch,)

docker-build:
	IMAGE="$(IMAGE)" scripts/docker-build.sh

docker-run:
	IMAGE="$(IMAGE)" CONTAINER="$(CONTAINER)" scripts/docker-run.sh

docker-stop:
	CONTAINER="$(CONTAINER)" scripts/docker-stop.sh

docker-logs:
	docker logs --follow --tail 200 "$(CONTAINER)"

e2e:
	scripts/e2e.sh

bench:
	scripts/bench.sh list

openapi:
	CARGO="$(CARGO)" scripts/openapi.sh

docs:
	@echo "Docs site:            https://21-ci.github.io/runtime/"
	@echo "Control API:          docs/api.md           generated: openapi/openapi.json (make openapi)"
	@echo "                      live: GET /v1/openapi.json and /docs on the control listener (:8081)"
	@echo "CLI:                  docs/cli.md           handlers and std:* modules: docs/handlers.md"
	@echo "Config and profiles:  docs/config.md, config/defconfigs/, config/schema.json"
	@echo "Deployment:           docs/deploy.md, deploy/, packaging/   security: SECURITY.md"
	@echo "Samples and scripts:  sample/README.md, sample/scripts/{deploy,invoke}.sh"
	@echo "Benchmarks:           bench/RESULTS.md, scripts/bench.sh"
	@echo "Docker:               Dockerfile, Dockerfile.21ci, docker-compose.yml (dev), deploy/compose (production)"

clean-data:
	@test -n "$(DATA_DIR)" && test "$(DATA_DIR)" != "/" && test "$(DATA_DIR)" != "." || { echo "refusing to remove DATA_DIR='$(DATA_DIR)'" >&2; exit 1; }
	rm -rf "$(DATA_DIR)"
	@echo "removed $(DATA_DIR). The docker data volume is separate: scripts/docker-stop.sh --purge"

-include mk/config.mk

# The documentation site (mkdocs.yml, docs/). The venv is created once from requirements.txt and rebuilt when it changes.
DOCS_VENV ?= target/docs-venv
DOCS_PY   := $(DOCS_VENV)/bin/python
DOCS_SITE ?= target/site
DOCS_ADDR ?= 127.0.0.1:8000
# Material for MkDocs prints a banner about MkDocs 2.0 on every run; the pins in requirements.txt stay below 2.0.
export NO_MKDOCS_2_WARNING := 1

$(DOCS_PY): requirements.txt
	python3 -m venv $(DOCS_VENV)
	$(DOCS_PY) -m pip install --quiet -r requirements.txt
	@touch $(DOCS_PY)

docs-venv: $(DOCS_PY)

docs-build: $(DOCS_PY)
	$(DOCS_PY) -m mkdocs build --strict -d $(DOCS_SITE)

docs-serve: $(DOCS_PY)
	$(DOCS_PY) -m mkdocs serve --dirty --dev-addr $(DOCS_ADDR)

# Build once, then serve the static files: instant page loads, no rebuild on every edit.
docs-preview: docs-build
	@echo "docs: http://$(DOCS_ADDR)/"
	python3 -m http.server --bind $(firstword $(subst :, ,$(DOCS_ADDR))) --directory $(DOCS_SITE) $(lastword $(subst :, ,$(DOCS_ADDR)))

docs-check: docs-build
	@if grep -rlE 'docs/plan|runtime-internal|runtime-docs' $(DOCS_SITE) >/dev/null; then \
		echo "the site mentions internal notes:" >&2; grep -rlE 'docs/plan|runtime-internal|runtime-docs' $(DOCS_SITE) >&2; exit 1; \
	fi
	@echo "docs: strict build ok, no internal references in $(DOCS_SITE)"
