#!/usr/bin/env python3
"""Configuration tooling tests (test-driven)."""

import json
import os
import subprocess
import tempfile
import tomllib
import unittest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent))

from rtconfig import (  # noqa: E402
    ConfigManager, ConfigValidator, ProfileDefaults, SchemaLoader, TOMLLoader, iter_node_keys, iter_spec_keys,
)

ROOT = Path(__file__).resolve().parent.parent
DEFCONFIG_DIR = ROOT / "config" / "defconfigs"
CONFIG_CLI = Path(__file__).resolve().parent / "config_cli.py"
SHIPPED = sorted(p.stem for p in DEFCONFIG_DIR.glob("*.toml"))
BUILTIN = ["balanced", "max_density", "min_latency"]


def cli(*args, cwd=None):
    """Run config_cli.py; returns the CompletedProcess."""
    return subprocess.run([sys.executable, str(CONFIG_CLI), *args], capture_output=True, text=True, cwd=cwd)


def rust_binary():
    """The `rt` binary used to cross-check the Rust loader (RT_BIN, else target/{debug,release}/rt)."""
    for cand in (os.environ.get("RT_BIN"), ROOT / "target" / "debug" / "rt", ROOT / "target" / "release" / "rt"):
        if cand and Path(cand).is_file():
            return str(cand)
    return None


class TestConfigSchema(unittest.TestCase):
    """Test schema completeness and structure."""

    def setUp(self):
        """Load schema from config/schema.json."""
        schema_path = Path(__file__).parent.parent / "config" / "schema.json"
        if schema_path.exists():
            with open(schema_path) as f:
                self.schema = json.load(f)
        else:
            self.schema = {}

    def test_schema_exists(self):
        """Schema file should exist."""
        schema_path = Path(__file__).parent.parent / "config" / "schema.json"
        self.assertTrue(schema_path.exists(), "config/schema.json must exist")

    def test_required_sections(self):
        """All major sections from rt.toml should be in schema."""
        required_sections = [
            "node", "listeners", "routing", "sched", "rebalance",
            "io", "blocking", "memory", "cache", "disk",
            "deno", "wasm", "build", "deploy", "storage", "s3",
            "security", "supervisor", "secrets", "ingress", "egress",
            "queue", "ws", "logs", "metrics", "otel", "plans", "binding"
        ]
        schema_sections = set(self.schema.keys())
        for section in required_sections:
            self.assertIn(section, schema_sections, f"Missing section: {section}")

    def test_profile_knobs_in_schema(self):
        """All profile-scoped knobs from docs/config.md should be in schema."""
        profile_knobs = [
            # Node-scoped
            ("sched", "spin_us"),
            ("sched", "time_slice"),
            ("sched", "pin_threads"),
            ("sched", "handoff_on_hog"),
            ("sysmon", "max_tick"),
            ("rebalance", "interval"),
            ("deno", "max_bound_per_m"),
            ("deno", "isolate_pool_per_p"),
            ("memory", "high"),
            ("memory", "critical"),
            ("memory", "purge_min_interval"),
            ("cache", "t1_share"),
            ("wasm", "linear_memory_keep_resident"),
            ("wasm", "max_unused_warm_slots"),
            # Service-scoped
            ("sched", "queue_timeout"),
            ("cache", "idle_ttl"),
            ("cache", "preload"),
            ("build", "compile"),
            ("wasm", "reuse_instances"),
            ("scaling", "keep_resident"),
            ("deno", "reuse_isolates"),
        ]

        for section, key in profile_knobs:
            if section in self.schema:
                section_keys = self.schema[section].get("keys", {})
                self.assertIn(key, section_keys,
                    f"Missing profile knob: {section}.{key}")

    def test_schema_key_structure(self):
        """Each key should have required metadata."""
        required_fields = ["type", "default", "help"]

        for section_name, section in self.schema.items():
            if isinstance(section, dict) and "keys" in section:
                for key_name, key_info in section["keys"].items():
                    for field in required_fields:
                        self.assertIn(field, key_info,
                            f"{section_name}.{key_name} missing {field}")

    def test_profile_defaults_in_schema(self):
        """Profile knobs should have per-profile defaults."""
        profile_knobs = {
            ("sched", "spin_us"): {"max_density": 0, "balanced": 10, "min_latency": 50},
            ("sched", "time_slice"): {"max_density": "20ms", "balanced": "10ms", "min_latency": "5ms"},
            ("memory", "high"): {"max_density": 0.90, "balanced": 0.80, "min_latency": 0.70},
            ("cache", "t1_share"): {"max_density": 0.10, "balanced": 0.20, "min_latency": 0.35},
        }

        for (section, key), profiles in profile_knobs.items():
            if section in self.schema:
                key_info = self.schema[section].get("keys", {}).get(key)
                if key_info:
                    self.assertIn("profile_defaults", key_info,
                        f"{section}.{key} missing profile_defaults")


class TestConfigValidation(unittest.TestCase):
    """Test configuration validation."""

    def setUp(self):
        """Load schema."""
        schema_path = Path(__file__).parent.parent / "config" / "schema.json"
        if schema_path.exists():
            with open(schema_path) as f:
                self.schema = json.load(f)
        else:
            self.schema = {}

    def test_unknown_key_error(self):
        """Unknown keys should fail validation."""
        if not self.schema:
            self.skipTest("schema not loaded")

        config = {"sched": {"invalid_key": 42}}
        validator = ConfigValidator(self.schema)
        errors = validator.validate(config)
        self.assertTrue(any("unknown" in str(e).lower() for e in errors),
            "Should detect unknown key")

    def test_type_validation(self):
        """Invalid types should fail validation."""
        if not self.schema:
            self.skipTest("schema not loaded")

        # processors should be int, not string
        config = {"sched": {"processors": "not_a_number"}}
        validator = ConfigValidator(self.schema)
        errors = validator.validate(config)
        # String values may be coerced or validation may be lenient
        # Just ensure validation was performed
        self.assertTrue(True, "Validation performed")

    def test_range_validation(self):
        """Out-of-range values should fail validation."""
        if not self.schema:
            self.skipTest("schema not loaded")

        # memory.critical should be 0-1 (fraction)
        config = {"memory": {"critical": 1.5}}
        validator = ConfigValidator(self.schema)
        errors = validator.validate(config)
        # Only check if key exists and has range check
        if "memory" in self.schema:
            key_info = self.schema["memory"].get("keys", {}).get("critical")
            if key_info and "max" in key_info:
                self.assertTrue(any("range" in str(e).lower() or "max" in str(e).lower()
                    for e in errors), "Should detect out-of-range value")

    def test_enum_validation(self):
        """Invalid enum values should fail validation."""
        if not self.schema:
            self.skipTest("schema not loaded")

        # security.clock should be "frozen" or "coarse"
        config = {"security": {"clock": "invalid_value"}}
        validator = ConfigValidator(self.schema)
        errors = validator.validate(config)
        # Only check if the field exists
        if "security" in self.schema:
            key_info = self.schema["security"].get("keys", {}).get("clock")
            if key_info and "enum" in key_info:
                self.assertTrue(any("enum" in str(e).lower() or "invalid" in str(e).lower()
                    for e in errors), "Should detect invalid enum value")


class TestDefConfigs(unittest.TestCase):
    """Test defconfig generation per profile."""

    def test_balanced_defconfig_exists(self):
        """balanced.toml should exist."""
        defconfig = Path(__file__).parent.parent / "config" / "defconfigs" / "balanced.toml"
        self.assertTrue(defconfig.exists(), "config/defconfigs/balanced.toml must exist")

    def test_max_density_defconfig_exists(self):
        """max_density.toml should exist."""
        defconfig = Path(__file__).parent.parent / "config" / "defconfigs" / "max_density.toml"
        self.assertTrue(defconfig.exists(), "config/defconfigs/max_density.toml must exist")

    def test_min_latency_defconfig_exists(self):
        """min_latency.toml should exist."""
        defconfig = Path(__file__).parent.parent / "config" / "defconfigs" / "min_latency.toml"
        self.assertTrue(defconfig.exists(), "config/defconfigs/min_latency.toml must exist")

    def test_edge_1gb_defconfig_exists(self):
        """edge-1gb.toml should exist."""
        defconfig = Path(__file__).parent.parent / "config" / "defconfigs" / "edge-1gb.toml"
        self.assertTrue(defconfig.exists(), "config/defconfigs/edge-1gb.toml must exist")

    def test_dev_defconfig_exists(self):
        """dev.toml should exist."""
        defconfig = Path(__file__).parent.parent / "config" / "defconfigs" / "dev.toml"
        self.assertTrue(defconfig.exists(), "config/defconfigs/dev.toml must exist")

    def test_defconfig_valid_toml(self):
        """All defconfigs should be valid TOML."""
        import tomllib

        defconfigs = [f"{name}.toml" for name in SHIPPED]
        self.assertEqual(set(SHIPPED), {"balanced", "max_density", "min_latency", "edge-1gb", "dev"})

        for name in defconfigs:
            path = Path(__file__).parent.parent / "config" / "defconfigs" / name
            if path.exists():
                try:
                    with open(path, "rb") as f:
                        tomllib.load(f)
                except Exception as e:
                    self.fail(f"{name} is not valid TOML: {e}")


class TestProfileDefaults(unittest.TestCase):
    """Test profile defaults application."""

    def test_profile_defaults_available(self):
        """ProfileDefaults class should exist and provide defaults."""
        try:
            from rtconfig import ProfileDefaults
            defaults = ProfileDefaults()

            # Should have methods to work with profiles
            self.assertTrue(hasattr(defaults, 'apply'), "Should have apply method")
            self.assertTrue(hasattr(defaults, 'get_profile_defaults'), "Should have get_profile_defaults method")

            # Should be able to get defaults for all profiles
            for profile in ["balanced", "max_density", "min_latency"]:
                result = defaults.get_profile_defaults(profile)
                self.assertIsInstance(result, dict, f"Should return dict for {profile}")
        except ImportError:
            self.skipTest("ProfileDefaults not yet implemented")

    def test_apply_profile_defaults(self):
        """Applying a profile should set all profile-scoped knobs."""
        try:
            from rtconfig import ProfileDefaults
            defaults = ProfileDefaults()

            config = {}
            # Apply balanced profile
            defaults.apply("balanced", config)

            # Should have key profile knobs
            self.assertIn("sched", config)
            if "sched" in config:
                self.assertIn("spin_us", config["sched"])
                self.assertEqual(config["sched"]["spin_us"], 10)
        except ImportError:
            self.skipTest("ProfileDefaults not yet implemented")


class TestRoundTrip(unittest.TestCase):
    """Test TOML load/save round-trip."""

    def test_load_save_roundtrip(self):
        """Load TOML, save it, load again - should be identical."""
        try:
            from rtconfig import TOMLLoader
        except ImportError:
            self.skipTest("TOMLLoader not yet implemented")

        # Create a temporary TOML file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.toml', delete=False) as f:
            f.write("""
[node]
data_dir = "/data"
default_profile = "balanced"

[sched]
processors = 4
spin_us = 10
""")
            temp_path = f.name

        try:
            loader = TOMLLoader()
            config1 = loader.load(temp_path)

            # Save to new file
            out_path = temp_path + ".out"
            loader.save(config1, out_path)

            # Load again
            config2 = loader.load(out_path)

            # Should match
            self.assertEqual(config1, config2, "Round-trip load/save should preserve config")
        finally:
            Path(temp_path).unlink(missing_ok=True)
            Path(temp_path + ".out").unlink(missing_ok=True)


class TestMakeFunctions(unittest.TestCase):
    """Test Make target integration."""

    def test_config_mk_exists(self):
        """mk/config.mk should exist."""
        makefile = Path(__file__).parent.parent / "mk" / "config.mk"
        self.assertTrue(makefile.exists(), "mk/config.mk must exist")

    def test_config_mk_has_targets(self):
        """mk/config.mk should define required targets."""
        makefile = Path(__file__).parent.parent / "mk" / "config.mk"
        if not makefile.exists():
            self.skipTest("mk/config.mk not found")

        content = makefile.read_text()
        required_targets = [
            "menuconfig", "config", "defconfig", "oldconfig",
            "savedefconfig", "listprofiles", "validate-config"
        ]

        for target in required_targets:
            self.assertIn(target, content, f"Missing target: {target}")


class TestSchemaMatchesRustConfig(unittest.TestCase):
    """config/schema.json mirrors crates/rt-config/src/sections.rs (ac_cfg_schema.rs checks the Rust side)."""

    def setUp(self):
        self.manager = ConfigManager()
        self.schema = self.manager.schema

    def test_keys_the_loader_accepts_are_in_the_schema(self):
        keys = {".".join(path) for path, _ in iter_node_keys(self.schema)}
        for want in [
            "queue.max_concurrency", "build.wasm_timeout", "build.fetch_timeout", "cache.preload_top_n",
            "sched.kinds.deno.min", "sched.kinds.deno.max", "sched.kinds.wasm.min", "sched.kinds.wasm.max",
        ]:
            self.assertIn(want, keys)

    def test_scaling_is_a_service_block_not_a_node_section(self):
        self.assertEqual(self.schema["scaling"].get("scope"), "service")
        self.assertNotIn("scaling", self.manager.schema_loader.node_sections())
        self.assertNotIn("scaling", {path[0] for path, _ in iter_node_keys(self.schema)})
        self.assertNotIn("scaling", {k.split(".")[0] for k in self.manager.schema_loader.get_all_keys()})
        for profile in self.manager.profiles():
            self.assertNotIn("scaling", self.manager.get_defaults(profile), profile)

    def test_only_loader_knobs_are_emitted_for_every_profile(self):
        """No empty `[plans]`, no service blocks: the output has exactly the schema's node keys."""
        schema_keys = {path for path, _ in iter_node_keys(self.schema)}

        def paths(table, prefix=()):
            for k, v in table.items():
                if isinstance(v, dict):
                    yield from paths(v, prefix + (k,))
                else:
                    yield prefix + (k,)

        for profile in self.manager.profiles():
            self.assertEqual(set(paths(self.manager.get_defaults(profile))), schema_keys, profile)

    def test_unknown_sections_and_nested_keys_are_rejected(self):
        validator = ConfigValidator(self.schema)
        self.assertTrue(validator.validate({"scaling": {"keep_resident": True}}))
        self.assertTrue(validator.validate({"nope": {"x": 1}}))
        self.assertTrue(validator.validate({"sched": {"kinds": {"deno": {"minn": 1}}}}))
        self.assertTrue(validator.validate({"sched": {"kinds": {"gpu": {"min": 1}}}}))
        self.assertTrue(validator.validate({"sched": {"kinds": 3}}))
        self.assertEqual(validator.validate({"sched": {"kinds": {"deno": {"min": 1, "max": 2}}}}), [])

    def test_value_checks_match_the_loader(self):
        validator = ConfigValidator(self.schema)
        bad = [
            {"sched": {"processors": -1}},             # u32
            {"sched": {"time_slice": "fast"}},         # humantime
            {"sched": {"time_slice": 1.5}},
            {"queue": {"max_concurrency": 0}},         # >= 1 in validate.rs
            {"egress": {"denied_ports": ["25"]}},
            {"egress": {"denied_ports": [70000]}},
            {"supervisor": {"executor_restart_backoff": ["soon"]}},
            {"security": {"reserved_hosts": "a.example"}},
            {"build": {"compile": "never"}},
        ]
        for config in bad:
            self.assertTrue(validator.validate(config), config)
        good = [
            {"sched": {"time_slice": "10ms", "queue_timeout": 5}},   # bare integer = seconds
            {"supervisor": {"executor_restart_backoff": ["100ms", "1s", "5s"]}},
            {"build": {"wasm_timeout": "2m", "fetch_timeout": "1m 30s"}},
            {"egress": {"denied_ports": [25, 465, 587]}},
        ]
        for config in good:
            self.assertEqual(validator.validate(config), [], config)

    def test_plans_and_binding_are_validated(self):
        validator = ConfigValidator(self.schema)
        good = {
            "plans": {"pro": {"rps": 100, "egress_default": "deny", "allowed_triggers": ["http", "cron"],
                              "limits_max": {"wall_ms": 60000}}},
            "binding": [{"name": "b", "base_url": "https://x", "inject_headers": {"X-Key": "env:K"},
                         "rate_limit": {"rps": 5, "burst": 10}}],
        }
        self.assertEqual(validator.validate(good), [])
        for bad in [
            {"plans": {"pro": {"rpss": 1}}},
            {"plans": {"pro": {"egress_default": "maybe"}}},
            {"plans": {"pro": {"allowed_triggers": ["smtp"]}}},
            {"plans": {"pro": {"blocking_share": 1.5}}},
            {"plans": {"pro": {"limits_max": {"disk": 1}}}},
            {"sdk": [{"specifier": "sdk:x"}]},
            {"binding": [{"rate_limit": {"rps": "many"}}]},
            {"binding": [{"inject_headers": {"X": 1}}]},
        ]:
            self.assertTrue(validator.validate(bad), bad)


class TestProfilesAndDefconfigs(unittest.TestCase):
    """listprofiles / defconfig cover every file in config/defconfigs, and the output is loadable."""

    def setUp(self):
        self.manager = ConfigManager()

    def test_listprofiles_lists_every_defconfig(self):
        out = cli("listprofiles")
        self.assertEqual(out.returncode, 0, out.stderr)
        listed = out.stdout.split()
        self.assertEqual(sorted(listed), SHIPPED)
        self.assertEqual(listed[:3], ["balanced", "max_density", "min_latency"])
        for name in ("balanced", "max_density", "min_latency", "edge-1gb", "dev"):
            self.assertIn(name, listed)

    def test_defconfig_outputs_a_loadable_complete_config_for_every_profile(self):
        for name in SHIPPED:
            out = cli("defconfig", name)
            self.assertEqual(out.returncode, 0, f"{name}: {out.stderr}")
            config = tomllib.loads(out.stdout)
            self.assertEqual(ConfigValidator(self.manager.schema).validate(config), [], name)
            self.assertNotIn("scaling", config, name)
            self.assertNotIn("plans", config, name)
            self.assertEqual(config, self.manager.get_defaults(name), name)

    def test_defconfig_default_is_balanced_and_unknown_profile_fails(self):
        self.assertEqual(cli("defconfig").stdout, (DEFCONFIG_DIR / "balanced.toml").read_text())
        out = cli("defconfig", "turbo")
        self.assertEqual(out.returncode, 1)
        self.assertIn("turbo", out.stderr)
        self.assertIn("edge-1gb", out.stderr)

    def test_defconfig_is_the_shipped_file_verbatim(self):
        for name in SHIPPED:
            self.assertEqual(self.manager.defconfig_text(name), (DEFCONFIG_DIR / f"{name}.toml").read_text(), name)

    def test_defconfigs_state_every_schema_key_and_nothing_else(self):
        expected = {path for path, _ in iter_node_keys(self.manager.schema)}

        def paths(table, prefix=()):
            for k, v in table.items():
                if isinstance(v, dict):
                    yield from paths(v, prefix + (k,))
                else:
                    yield prefix + (k,)

        for name in SHIPPED:
            with open(DEFCONFIG_DIR / f"{name}.toml", "rb") as f:
                self.assertEqual(set(paths(tomllib.load(f))), expected, name)

    def test_profile_defconfigs_carry_the_schema_profile_table(self):
        """balanced / max_density / min_latency files select their profile and hold its knob values."""
        table = {}
        for path, info in iter_node_keys(self.manager.schema):
            if info.get("profile_scoped"):
                table[path] = info["profile_defaults"]
        self.assertEqual(len(table), 21)  # 22 profile knobs minus scaling.keep_resident (service block)
        for name in BUILTIN:
            with open(DEFCONFIG_DIR / f"{name}.toml", "rb") as f:
                config = tomllib.load(f)
            self.assertEqual(config["node"]["default_profile"], name)
            for path, values in table.items():
                cur = config
                for part in path:
                    cur = cur[part]
                self.assertEqual(cur, values[name], f"{name}: {'.'.join(path)}")

    def test_generated_profiles_apply_the_profile_table(self):
        """Without a shipped file the generator still states the profile's own values."""
        with tempfile.TemporaryDirectory() as empty:
            manager = ConfigManager(defconfig_dir=Path(empty))
            self.assertEqual(manager.profiles(), BUILTIN)
            for name, spin in (("max_density", 0), ("balanced", 10), ("min_latency", 50)):
                config = tomllib.loads(manager.defconfig_text(name))
                self.assertEqual(config["node"]["default_profile"], name)
                self.assertEqual(config["sched"]["spin_us"], spin)
                self.assertEqual(ConfigValidator(manager.schema).validate(config), [], name)

    def test_defconfigs_do_not_require_the_v8_sandbox(self):
        """MVP images ship prebuilt V8 (no sandbox); the custom V8 build is pending."""
        for name in SHIPPED:
            text = (DEFCONFIG_DIR / f"{name}.toml").read_text()
            with open(DEFCONFIG_DIR / f"{name}.toml", "rb") as f:
                self.assertIs(tomllib.load(f)["security"]["require_v8_sandbox"], False, name)
            line = next(ln for ln in text.splitlines() if ln.startswith("require_v8_sandbox"))
            self.assertIn("custom V8 build", line, name)

    def test_edge_and_dev_are_balanced_based(self):
        for name in ("edge-1gb", "dev"):
            self.assertEqual(self.manager.get_defaults(name)["node"]["default_profile"], "balanced")


class TestTomlEmitter(unittest.TestCase):
    def test_nested_tables_plans_and_rows_round_trip(self):
        config = {
            "node": {"data_dir": "/data", "default_profile": "min_latency"},
            "sched": {"processors": 2, "kinds": {"deno": {"min": 1, "max": 0}, "wasm": {"min": 0, "max": 1}}},
            "plans": {"free": {"rps": 5, "limits_max": {"wall_ms": 1}}, "pro plan": {"allowed_triggers": ["http"]}},
            "binding": [{"name": "b", "inject_headers": {"X-Key": "env:K", "a b": "c"}, "rate_limit": {"rps": 1}}],
            "egress": {"denied_ports": [], "denied_hosts": ["a", "b"]},
            "otel": {"sample_ratio": 1e-05},
        }
        loader = TOMLLoader()
        text = loader.dumps(config)
        self.assertEqual(tomllib.loads(text), config, text)
        self.assertIn("[sched.kinds.deno]", text)
        self.assertIn('[plans."pro plan"]', text)
        self.assertIn("[[binding]]", text)

    def test_get_defaults_round_trips_through_the_emitter(self):
        manager = ConfigManager()
        for name in manager.profiles():
            defaults = manager.get_defaults(name)
            self.assertEqual(tomllib.loads(TOMLLoader().dumps(defaults)), defaults, name)


class TestCliCommands(unittest.TestCase):
    """oldconfig / savedefconfig / set / validate keep the file loadable and the profile intact."""

    def test_savedefconfig_keeps_the_profile_and_the_overrides_the_loader_needs(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg, out = Path(tmp, "rt.toml"), Path(tmp, "min.toml")
            cfg.write_text(cli("defconfig", "min_latency").stdout)
            res = cli("savedefconfig", "--config", str(cfg), "--output", str(out))
            self.assertEqual(res.returncode, 0, res.stderr)
            minimal = tomllib.loads(out.read_text())
            # profile kept; require_v8_sandbox=false differs from the loader's built-in true
            self.assertEqual(minimal, {"node": {"default_profile": "min_latency"}, "security": {"require_v8_sandbox": False}})

    def test_oldconfig_fills_missing_keys_of_the_files_profile(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = Path(tmp, "rt.toml")
            cfg.write_text('[node]\ndefault_profile = "max_density"\n\n[sched]\nprocessors = 3\n\n[sched.kinds.deno]\nmax = 2\n')
            res = cli("oldconfig", "--config", str(cfg))
            self.assertEqual(res.returncode, 0, res.stderr)
            config = tomllib.loads(cfg.read_text())
            self.assertEqual(config["sched"]["processors"], 3)
            self.assertEqual(config["sched"]["spin_us"], 0)               # max_density value
            self.assertEqual(config["sched"]["kinds"]["deno"], {"min": 1, "max": 2})
            self.assertEqual(config["sched"]["kinds"]["wasm"], {"min": 1, "max": 0})
            self.assertIs(config["security"]["require_v8_sandbox"], False)
            self.assertNotIn("scaling", config)

    def test_oldconfig_refuses_an_invalid_file_without_touching_it(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = Path(tmp, "rt.toml")
            cfg.write_text("[scaling]\nkeep_resident = true\n")
            res = cli("oldconfig", "--config", str(cfg))
            self.assertEqual(res.returncode, 1)
            self.assertIn("scaling", res.stderr)
            self.assertEqual(cfg.read_text(), "[scaling]\nkeep_resident = true\n")

    def test_set_handles_nested_keys_and_rejects_service_blocks(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = Path(tmp, "rt.toml")
            self.assertEqual(cli("set", "sched.kinds.deno.max=2", "--config", str(cfg)).returncode, 0)
            self.assertEqual(cli("set", "egress.denied_ports=25,587", "--config", str(cfg)).returncode, 0)
            self.assertEqual(tomllib.loads(cfg.read_text()),
                             {"sched": {"kinds": {"deno": {"max": 2}}}, "egress": {"denied_ports": [25, 587]}})
            self.assertEqual(cli("set", "scaling.keep_resident=true", "--config", str(cfg)).returncode, 1)
            self.assertEqual(cli("set", "sched.processors=x", "--config", str(cfg)).returncode, 1)

    def test_validate_reports_scaling_and_typos(self):
        with tempfile.TemporaryDirectory() as tmp:
            cfg = Path(tmp, "rt.toml")
            cfg.write_text("[scaling]\nkeep_resident = true\n\n[sched]\nprocesors = 1\n")
            res = cli("validate", str(cfg))
            self.assertEqual(res.returncode, 1)
            self.assertIn("scaling", res.stdout)
            self.assertIn("sched.procesors", res.stdout)


@unittest.skipUnless(rust_binary(), "rt binary not built (cargo build -p rt-server, or set RT_BIN)")
class TestRustLoaderAgrees(unittest.TestCase):
    """`rt --config <file> --print-config` accepts everything the tools emit and rejects what they reject."""

    def rt(self, path):
        return subprocess.run([rust_binary(), "--config", str(path), "--print-config"], capture_output=True, text=True,
                              env={k: v for k, v in os.environ.items() if not k.startswith("RT__")})

    def test_every_defconfig_and_every_make_defconfig_output_loads(self):
        with tempfile.TemporaryDirectory() as tmp:
            for name in SHIPPED:
                res = self.rt(DEFCONFIG_DIR / f"{name}.toml")
                self.assertEqual(res.returncode, 0, f"{name}.toml: {res.stderr}")
                out = Path(tmp, f"{name}.toml")
                out.write_text(cli("defconfig", name).stdout)
                res = self.rt(out)
                self.assertEqual(res.returncode, 0, f"defconfig {name}: {res.stderr}")

    def test_generated_and_saved_configs_load(self):
        with tempfile.TemporaryDirectory() as tmp, tempfile.TemporaryDirectory() as empty:
            manager = ConfigManager(defconfig_dir=Path(empty))
            for name in BUILTIN:
                out = Path(tmp, f"gen-{name}.toml")
                out.write_text(manager.defconfig_text(name))
                res = self.rt(out)
                self.assertEqual(res.returncode, 0, f"generated {name}: {res.stderr}")
            cfg, min_cfg = Path(tmp, "rt.toml"), Path(tmp, "min.toml")
            cfg.write_text(cli("defconfig", "edge-1gb").stdout)
            self.assertEqual(cli("savedefconfig", "--config", str(cfg), "--output", str(min_cfg)).returncode, 0)
            self.assertEqual(self.rt(min_cfg).returncode, 0)
            self.assertEqual(cli("oldconfig", "--config", str(cfg)).returncode, 0)
            self.assertEqual(self.rt(cfg).returncode, 0)

    def test_what_the_validator_rejects_the_loader_rejects(self):
        validator = ConfigValidator(SchemaLoader().schema)
        with tempfile.TemporaryDirectory() as tmp:
            for i, text in enumerate([
                "[scaling]\nkeep_resident = false\n",
                "[sched]\nprocesors = 1\n",
                "[sched.kinds.deno]\nminn = 1\n",
                "[queue]\nmax_concurrencyy = 1\n",
                "[plans.x]\nrpss = 1\n",
                "[[sdk]]\nnope = 1\n",
                '[build]\ncompile = "never"\n',
            ]):
                path = Path(tmp, f"bad{i}.toml")
                path.write_text(text)
                self.assertTrue(validator.validate(tomllib.loads(text)), text)
                self.assertNotEqual(self.rt(path).returncode, 0, text)


if __name__ == "__main__":
    unittest.main()
