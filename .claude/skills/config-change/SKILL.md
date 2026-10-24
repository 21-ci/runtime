---
name: config-change
description: Use when adding, renaming, removing or changing the default/bounds of a node config key (rt.toml / RT__ env), a profile-controlled knob, a plan limit ([plans.<name>]), or a service-scoped scaling key in this runtime. Covers rt-config types and validation, config/schema.json, the five defconfigs, live vs restart vs reload keys, the Python menuconfig tooling and its tests, and docs/config.md.
---

# Changing configuration

Unknown keys fail startup, and so does a key that is missing from the schema or the defconfigs, in tests. So one
key touches about eight files, and they all land in one commit. The reference commit is `996936c`
(`feat(ws): ... [ws] config`): its docs followed in `3fa2587`.

## Files to touch

| # | File | What |
|---|---|---|
| 1 | `crates/rt-config/src/sections.rs` | The field on the section struct (the `section!` macro gives `#[serde(deny_unknown_fields, default)]`) **and** its value in the hand-written `impl Default`. Durations use `serde_duration`. A **profile-controlled** knob is `Option<T>`, where `None` means the profile fills it. |
| 2 | `crates/rt-config/src/lib.rs` | Only for a new section: the field on `Config` **and** in `impl Default for Config`. A new profile knob needs its `get_or_insert` line in `Config::print` (`--print-config`). Live keys: see below. |
| 3 | `crates/rt-config/src/validate.rs` | Bounds in `problems(cfg)` (helpers `at_least`, `fraction`, `listener_problem`, `cidr_problem`). The message must name the dotted key. Bounds must equal the schema's `min`/`max`/`enum`. |
| 4 | `crates/rt-config/src/profile.rs` / `resolve.rs` | Profile knobs: the per-profile values in `node_knobs`, `service_knobs`, `scaling_knobs` or `warm_share`; resolution with provenance in `resolve_node` / `resolve_service`. |
| 5 | `config/schema.json` | `"<section>": {"help", "keys": {"<key>": {"type", "default", "min"/"max"/"enum", "help", "profile_scoped": false}}}`. A profile knob uses `"profile_scoped": "node"` plus `"profile_defaults": {"max_density", "balanced", "min_latency"}` (see `sched.spin_us`). Nested tables go under `"tables"`, plans under `"named"`, bindings under `"rows"`, and `scaling` has `"scope": "service"`. |
| 6 | `config/defconfigs/{balanced,dev,edge-1gb,max_density,min_latency}.toml` | **Every** defconfig states every schema key, and nothing else. A profile defconfig states its profile's value. |
| 7 | The consumer | Wire the value where it is used, usually `crates/rt-server/src/wiring.rs` or `specs.rs`, then into the crate's own config type (e.g. `DenoExecutorConfig`, `SchedConfig`). `rt-config` stays L1: it must not depend on the consumers. |
| 8 | `docs/config.md` | The row in its "Configuration Sections" subsection, "Profile Differences at a Glance" for a profile knob, "Live-Updatable vs Restart-Required" for a live key, and "Schema Reference" if the format changed. Add `SECURITY.md` "Hardening settings" if the key affects security. |

**Plan limits** live on `rt_types::Plan` (`crates/rt-types/src/plan.rs`), under `"plans"."named"` in the schema.
Also update `docs/api.md` "Plans", the plan table in SECURITY.md "Tenancy, plans and quotas" if it is a free-plan
default, and the quota check in `crates/rt-sec/src/quota.rs` if it is enforced there.

**Removing or renaming a key.** Do not drop it silently. Make the loader refuse it with a hint, via
`check_removed` in `crates/rt-config/src/loader.rs` (see the `[[sdk]]` entry, which gives `ConfigError::Removed`),
and remove it from the schema, the defconfigs and the docs.

## Live, reload or restart?

- **Restart-only** is the default: nothing more to do. The node config `PUT` answers `pending_restart` for such keys.
- **Live** (changed by `PUT /v1/node/config` and SIGHUP):
  1. add the key to `Config::LIVE_KEYS` and copy it in `apply_live_keys` (`crates/rt-config/src/lib.rs`). They must
     mirror each other: `crates/rt-config/tests/ac_cfg_load.rs`
     `apply_live_keys_copies_every_live_key_and_nothing_else` checks this;
  2. apply it at run time in `apply_live_knobs` / `apply_live_config` (`crates/rt-server/src/wiring.rs`);
  3. test it in `crates/rt-server/tests/api/control.rs` (CFG-05).
- **Reload** (`RELOAD_KEYS = ["plans"]`): SIGHUP re-reads plans through `reload_config` in `wiring.rs`. A plan
  change must re-run the warm-floor sync (`68ac0f7`).

`docs/config.md` is the configuration guide. Update it in the same change as `config/schema.json` and `LIVE_KEYS` /
`RELOAD_KEYS`.

## Tests that will catch you

- **Rust** (`cargo test -p rt-config`; light, no V8): `crates/rt-config/tests/ac_cfg_schema.rs`. It includes the
  schema with `include_str!` and checks:
  - every config section is in the schema and the reverse;
  - every schema key exists in the struct, and an `Option` means profile-controlled;
  - schema defaults equal the built-in defaults;
  - schema bounds are enforced, naming the key;
  - schema profile defaults equal the Rust profile tables;
  - every shipped defconfig loads and validates;
  - profile defconfigs state their profile's values.

  There are also `ac_cfg_load.rs`, `ac_cfg_profiles.rs`, `ac_cfg_duration.rs` and others.
- **Python** (`make test-config`, i.e. `python3 -m unittest discover -s tools -p 'test_*.py'`):
  `tools/test_config.py`, including `test_defconfigs_state_every_schema_key_and_nothing_else`.
  `TestSchemaMatchesRustConfig` and `TestRustLoaderAgrees` run the real `rt` binary (`RT_BIN`, else
  `target/{debug,release}/rt`) and are skipped when it is not built.
- Add a validation test for the new bound (accepted at the edge, refused one past it with the key named), and a
  consumer test that shows the value takes effect.

## Tooling

- The Python tools read `config/schema.json`, so a correct schema entry makes the key appear in `make menuconfig`
  (`tools/menuconfig.py`) and `make config`, `defconfig`, `oldconfig`, `savedefconfig` and `validate-config`
  (`tools/config_cli.py`, `tools/rtconfig.py`). These targets are in `mk/config.mk`.
- Change `tools/rtconfig.py` only for a new value **type** or schema shape, and add a test for it in
  `tools/test_config.py`.
- `RT__SECTION__KEY` env overrides work automatically (`ENV_PREFIX`, `apply_env` in `loader.rs`).

## Done when
- [ ] Struct, `Default`, validation, schema, all five defconfigs and the docs agree on name, type, default and bounds.
- [ ] `cargo test -p rt-config`, `make test-config` and the consumer crate's tests pass.
- [ ] `make coverage MODULES="config tools"` covers the new key's parsing, validation and defaults.
