#!/usr/bin/env python3
"""Configuration loader, validator and manager for the runtime (`rt.toml`).

`config/schema.json` describes exactly what the Rust loader (crates/rt-config) accepts:

* a section is `{"help", "keys": {...}}`; nested tables live under `"tables"` (e.g.
  `sched.kinds.deno`), `[plans.<name>]` entries under `"named"`, `[[binding]]` rows
  under `"rows"`;
* `"scope": "service"` marks a per-service block (`scaling`). It is **not** an `rt.toml` section:
  the node loader rejects it (`unknown key scaling`), so these tools never emit or accept it in a
  node config.

Shipped defconfigs (`config/defconfigs/*.toml`) are the profile files: `balanced`, `max_density`
and `min_latency` are the built-in profiles (`node.default_profile`); `edge-1gb` and `dev` are
reference configs on top of `balanced`.
"""

import copy
import json
import math
import re
import tomllib
from pathlib import Path
from typing import Any, Dict, Iterator, List, Optional, Tuple, Union

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_SCHEMA = ROOT / "config" / "schema.json"
DEFAULT_DEFCONFIG_DIR = ROOT / "config" / "defconfigs"

# Rust built-in default of `node.default_profile`.
BUILTIN_DEFAULT_PROFILE = "balanced"

Path_ = Tuple[str, ...]
_MISSING = object()


# ---------------------------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------------------------

def is_node_section(info: Any) -> bool:
    """True for a section that lives in `rt.toml` (not a per-service block like `scaling`)."""
    return isinstance(info, dict) and info.get("scope") != "service"


def node_section_names(schema: Dict[str, Any]) -> List[str]:
    return [name for name, info in schema.items() if is_node_section(info)]


def iter_spec_keys(spec: Dict[str, Any], prefix: Path_ = ()) -> Iterator[Tuple[Path_, Dict[str, Any]]]:
    """Yield `(path, key_info)` for the fixed keys of a table spec and its nested tables."""
    for name, info in spec.get("keys", {}).items():
        yield prefix + (name,), info
    for name, sub in spec.get("tables", {}).items():
        yield from iter_spec_keys(sub, prefix + (name,))


def iter_node_keys(schema: Dict[str, Any]) -> Iterator[Tuple[Path_, Dict[str, Any]]]:
    """Every fixed key of every node section: `(("sched", "kinds", "deno", "min"), info)`."""
    for section in node_section_names(schema):
        yield from iter_spec_keys(schema[section], (section,))


def get_path(data: Dict[str, Any], path: Path_, default: Any = None) -> Any:
    cur: Any = data
    for part in path:
        if not isinstance(cur, dict) or part not in cur:
            return default
        cur = cur[part]
    return cur


def set_path(data: Dict[str, Any], path: Path_, value: Any) -> None:
    cur = data
    for part in path[:-1]:
        nxt = cur.get(part)
        if not isinstance(nxt, dict):
            nxt = cur[part] = {}
        cur = nxt
    cur[path[-1]] = value


def deep_merge(base: Dict[str, Any], over: Dict[str, Any]) -> Dict[str, Any]:
    """`over` on top of `base`, recursing into tables (lists and scalars are replaced)."""
    out = copy.deepcopy(base)
    for key, value in over.items():
        if isinstance(value, dict) and isinstance(out.get(key), dict):
            out[key] = deep_merge(out[key], value)
        else:
            out[key] = copy.deepcopy(value)
    return out


def deep_diff(config: Dict[str, Any], base: Dict[str, Any]) -> Dict[str, Any]:
    """The part of `config` that differs from `base`."""
    out: Dict[str, Any] = {}
    for key, value in config.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            sub = deep_diff(value, base[key])
            if sub:
                out[key] = sub
        elif key not in base or value != base[key]:
            out[key] = value
    return out


class SchemaLoader:
    """Load and provide access to the configuration schema."""

    def __init__(self, schema_path: Optional[Path] = None):
        """Initialize with schema file."""
        self.path = Path(schema_path) if schema_path is not None else DEFAULT_SCHEMA
        self.schema = self._load_schema()

    def _load_schema(self) -> Dict[str, Any]:
        """Load schema from JSON file."""
        if not self.path.exists():
            raise FileNotFoundError(f"Schema not found: {self.path}")

        with open(self.path) as f:
            return json.load(f)

    def node_sections(self) -> List[str]:
        """Sections of `rt.toml`, in schema order (no per-service blocks)."""
        return node_section_names(self.schema)

    def builtin_profiles(self) -> List[str]:
        """Profiles the Rust loader knows (`node.default_profile`)."""
        return list(self.schema["node"]["keys"]["default_profile"]["enum"])

    def get_all_keys(self) -> Dict[str, tuple]:
        """Fixed node keys as `"section.rest.of.path" -> (section, "rest.of.path", info)`."""
        result = {}
        for path, info in iter_node_keys(self.schema):
            result[".".join(path)] = (path[0], ".".join(path[1:]), info)
        return result


# ---------------------------------------------------------------------------------------------
# TOML
# ---------------------------------------------------------------------------------------------

_BARE_KEY = re.compile(r"^[A-Za-z0-9_-]+$")


class TOMLLoader:
    """Load and save TOML configuration files."""

    def load(self, path: Union[str, Path]) -> Dict[str, Any]:
        """Load TOML file and return config dict."""
        with open(path, "rb") as f:
            return tomllib.load(f)

    def save(self, config: Dict[str, Any], path: Union[str, Path]) -> None:
        """Save config dict as TOML file."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w") as f:
            f.write(self.dumps(config))

    def dumps(self, data: Dict[str, Any]) -> str:
        """Config dict -> TOML text (scalars first, then `[a.b]` tables, then `[[rows]]`)."""
        lines: List[str] = []
        self._emit(lines, data, [], False)
        return "\n".join(lines) + "\n"

    # kept for callers that used the old private name
    _to_toml = dumps

    @staticmethod
    def _is_array_of_tables(value: Any) -> bool:
        return isinstance(value, list) and len(value) > 0 and all(isinstance(v, dict) for v in value)

    def _emit(self, lines: List[str], table: Dict[str, Any], path: List[str], array_item: bool) -> None:
        scalars = [(k, v) for k, v in table.items() if not isinstance(v, dict) and not self._is_array_of_tables(v)]
        tables = [(k, v) for k, v in table.items() if isinstance(v, dict)]
        rows = [(k, v) for k, v in table.items() if self._is_array_of_tables(v)]

        if path:
            # A table that only holds sub-tables needs no header of its own.
            if scalars or array_item or not (tables or rows):
                if lines:
                    lines.append("")
                dotted = ".".join(path)
                lines.append(f"[[{dotted}]]" if array_item else f"[{dotted}]")
        for key, value in scalars:
            lines.append(f"{self._format_key(key)} = {self._format_value(value)}")
        for key, value in tables:
            self._emit(lines, value, path + [self._format_key(key)], False)
        for key, value in rows:
            for item in value:
                self._emit(lines, item, path + [self._format_key(key)], True)

    @staticmethod
    def _format_str(value: str) -> str:
        escapes = {'"': '\\"', "\\": "\\\\", "\n": "\\n", "\t": "\\t", "\r": "\\r", "\b": "\\b", "\f": "\\f"}
        out = []
        for ch in value:
            if ch in escapes:
                out.append(escapes[ch])
            elif ord(ch) < 0x20 or ord(ch) == 0x7F:
                out.append(f"\\u{ord(ch):04x}")
            else:
                out.append(ch)
        return '"' + "".join(out) + '"'

    def _format_key(self, key: str) -> str:
        return key if _BARE_KEY.match(key) else self._format_str(key)

    def _format_value(self, value: Any) -> str:
        """Format a value for TOML output."""
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, str):
            return self._format_str(value)
        if isinstance(value, int):
            return str(value)
        if isinstance(value, float):
            if math.isnan(value):
                return "nan"
            if math.isinf(value):
                return "inf" if value > 0 else "-inf"
            return repr(value)
        if isinstance(value, list):
            return "[" + ", ".join(self._format_value(v) for v in value) + "]"
        if isinstance(value, dict):
            inner = ", ".join(f"{self._format_key(k)} = {self._format_value(v)}" for k, v in value.items())
            return "{ " + inner + " }" if inner else "{}"
        if hasattr(value, "isoformat"):
            return value.isoformat()
        return str(value)


# ---------------------------------------------------------------------------------------------
# validation
# ---------------------------------------------------------------------------------------------

# humantime (the Rust parser): `<integer><unit>` groups, e.g. `250ms`, `1h 30m`.
_DURATION_RE = re.compile(
    r"^\s*(\d+\s*(nsec|ns|usec|us|µs|msec|ms|seconds?|secs?|s|minutes?|min|m|hours?|hr|h|days?|d|weeks?|w|months?|M|years?|y)\s*)+$"
)


def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _is_num(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def _type_name(value: Any) -> str:
    return type(value).__name__


class ConfigValidator:
    """Validate configuration against the schema (same strictness as the Rust loader)."""

    def __init__(self, schema: Dict[str, Any]):
        """Initialize with schema."""
        self.schema = schema

    def validate(self, config: Dict[str, Any]) -> List[str]:
        """Validate config and return list of errors."""
        errors: List[str] = []
        for name, value in config.items():
            info = self.schema.get(name)
            if info is None:
                errors.append(f"unknown key: {name}")
            elif not is_node_section(info):
                errors.append(f"unknown key: {name} (per-service block, not an rt.toml section)")
            elif "rows" in info:
                errors.extend(self._validate_rows(info["rows"], value, name))
            elif "named" in info:
                if not isinstance(value, dict):
                    errors.append(f"{name}: expected table of [{name}.<name>] tables, got {_type_name(value)}")
                else:
                    for entry_name, entry in value.items():
                        path = f"{name}.{entry_name}"
                        if isinstance(entry, dict):
                            errors.extend(self._validate_table(info["named"], entry, path))
                        else:
                            errors.append(f"{path}: expected table, got {_type_name(entry)}")
            elif isinstance(value, dict):
                errors.extend(self._validate_table(info, value, name))
            else:
                errors.append(f"{name}: expected table, got {_type_name(value)}")
        return errors

    def _validate_rows(self, spec: Dict[str, Any], value: Any, path: str) -> List[str]:
        if not isinstance(value, list) or not all(isinstance(r, dict) for r in value):
            return [f"{path}: expected array of tables ([[{path}]])"]
        errors: List[str] = []
        for i, row in enumerate(value):
            errors.extend(self._validate_table(spec, row, f"{path}[{i}]"))
        return errors

    def _validate_table(self, spec: Dict[str, Any], table: Dict[str, Any], path: str) -> List[str]:
        errors: List[str] = []
        keys = spec.get("keys", {})
        tables = spec.get("tables", {})
        for key, value in table.items():
            full = f"{path}.{key}"
            if key in keys:
                errors.extend(self._validate_value(full, value, keys[key]))
            elif key in tables:
                if isinstance(value, dict):
                    errors.extend(self._validate_table(tables[key], value, full))
                else:
                    errors.append(f"{full}: expected table, got {_type_name(value)}")
            else:
                errors.append(f"unknown key: {full}")
        return errors

    def _validate_value(self, key: str, value: Any, key_info: Dict[str, Any]) -> List[str]:
        """Validate a single value against its schema."""
        errors: List[str] = []
        expected_type = key_info.get("type")

        if expected_type == "bool":
            if not isinstance(value, bool):
                errors.append(f"{key}: expected bool, got {_type_name(value)}")
        elif expected_type == "int":
            if not _is_int(value):
                errors.append(f"{key}: expected int, got {_type_name(value)}")
            else:
                errors.extend(self._check_range(key, value, key_info, default_min=0))
        elif expected_type == "float":
            if not _is_num(value):
                errors.append(f"{key}: expected float, got {_type_name(value)}")
            else:
                errors.extend(self._check_range(key, value, key_info))
        elif expected_type == "string":
            if not isinstance(value, str):
                errors.append(f"{key}: expected string, got {_type_name(value)}")
        elif expected_type == "duration":
            errors.extend(self._check_duration(key, value))
        elif expected_type == "enum":
            allowed = key_info.get("enum", [])
            if value not in allowed:
                errors.append(f"{key}: invalid enum value '{value}', must be one of {allowed}")
        elif expected_type == "list":
            if not isinstance(value, list):
                errors.append(f"{key}: expected list, got {_type_name(value)}")
            else:
                for i, item in enumerate(value):
                    errors.extend(self._validate_item(f"{key}[{i}]", item, key_info))
        elif expected_type == "map":
            if not isinstance(value, dict):
                errors.append(f"{key}: expected table, got {_type_name(value)}")
            else:
                for k, v in value.items():
                    if not isinstance(v, str):
                        errors.append(f"{key}.{k}: expected string, got {_type_name(v)}")

        return errors

    def _validate_item(self, key: str, item: Any, key_info: Dict[str, Any]) -> List[str]:
        kind = key_info.get("items")
        if kind == "string":
            return [] if isinstance(item, str) else [f"{key}: expected string, got {_type_name(item)}"]
        if kind == "int":
            if not _is_int(item):
                return [f"{key}: expected int, got {_type_name(item)}"]
            return self._check_range(
                key, item, {"min": key_info.get("item_min"), "max": key_info.get("item_max")}, default_min=0
            )
        if kind == "duration":
            return self._check_duration(key, item)
        if kind == "enum":
            allowed = key_info.get("item_enum", [])
            return [] if item in allowed else [f"{key}: invalid value '{item}', must be one of {allowed}"]
        return []

    @staticmethod
    def _check_duration(key: str, value: Any) -> List[str]:
        # humantime string (`"250ms"`) or a bare non-negative integer (seconds)
        if _is_int(value):
            return [] if value >= 0 else [f"{key}: duration must not be negative"]
        if isinstance(value, str):
            return [] if _DURATION_RE.match(value) else [f"{key}: invalid duration '{value}' (e.g. \"250ms\", \"10s\", \"1h\")"]
        return [f"{key}: expected duration string, got {_type_name(value)}"]

    @staticmethod
    def _check_range(key: str, value: Any, info: Dict[str, Any], default_min: Optional[float] = None) -> List[str]:
        errors = []
        lo = info.get("min")
        if lo is None:
            lo = default_min
        hi = info.get("max")
        if lo is not None and value < lo:
            errors.append(f"{key}: value {value} below minimum {lo}")
        if hi is not None and value > hi:
            errors.append(f"{key}: value {value} above maximum {hi}")
        return errors


# ---------------------------------------------------------------------------------------------
# profiles
# ---------------------------------------------------------------------------------------------

class ProfileDefaults:
    """Apply and manage profile defaults (the schema's `profile_defaults`)."""

    def __init__(self, schema: Optional[Dict[str, Any]] = None):
        """Initialize with optional schema."""
        self.schema = schema or SchemaLoader().schema

    def profiles(self) -> List[str]:
        return list(self.schema["node"]["keys"]["default_profile"]["enum"])

    def _scoped_values(self, profile: str) -> Iterator[Tuple[Path_, Any]]:
        if profile not in self.profiles():
            raise ValueError(f"Unknown profile: {profile}")
        for path, info in iter_node_keys(self.schema):
            table = info.get("profile_defaults")
            if info.get("profile_scoped") and table and profile in table:
                yield path, table[profile]

    def apply(self, profile: str, config: Dict[str, Any], overwrite: bool = False) -> None:
        """Apply profile defaults to config dict (modifies in place).

        Explicit values are kept unless `overwrite` is set.
        """
        for path, value in self._scoped_values(profile):
            if overwrite or get_path(config, path, _MISSING) is _MISSING:
                set_path(config, path, copy.deepcopy(value))

    def get_profile_defaults(self, profile: str) -> Dict[str, Any]:
        """Get a dict of profile defaults."""
        result: Dict[str, Any] = {}
        for path, value in self._scoped_values(profile):
            set_path(result, path, copy.deepcopy(value))
        return result


# ---------------------------------------------------------------------------------------------
# manager
# ---------------------------------------------------------------------------------------------

class ConfigManager:
    """Main configuration manager."""

    def __init__(self, schema_path: Optional[Path] = None, defconfig_dir: Optional[Path] = None):
        """Initialize manager."""
        self.schema_loader = SchemaLoader(schema_path)
        self.schema = self.schema_loader.schema
        self.defconfig_dir = Path(defconfig_dir) if defconfig_dir is not None else DEFAULT_DEFCONFIG_DIR
        self.toml_loader = TOMLLoader()
        self.validator = ConfigValidator(self.schema)
        self.profile_defaults = ProfileDefaults(self.schema)

    # ----- profiles

    def builtin_profiles(self) -> List[str]:
        """Profiles known to the Rust loader (`node.default_profile`)."""
        return self.schema_loader.builtin_profiles()

    def defconfig_path(self, name: str) -> Optional[Path]:
        path = self.defconfig_dir / f"{name}.toml"
        return path if path.is_file() else None

    def profiles(self) -> List[str]:
        """Everything `defconfig` accepts: built-in profiles, then the other shipped defconfigs."""
        builtin = self.builtin_profiles()
        builtin.sort(key=lambda p: p != BUILTIN_DEFAULT_PROFILE)  # the default first, schema order after it
        extra = sorted(p.stem for p in self.defconfig_dir.glob("*.toml") if p.stem not in builtin)
        return builtin + extra

    def _require_profile(self, name: str) -> None:
        if name not in self.profiles():
            raise ValueError(f"Unknown profile '{name}' (available: {', '.join(self.profiles())})")

    def defconfig_text(self, name: str = BUILTIN_DEFAULT_PROFILE) -> str:
        """The defconfig of `name`: the shipped file verbatim, else generated from the schema."""
        self._require_profile(name)
        path = self.defconfig_path(name)
        if path is not None:
            text = path.read_text()
            return text if text.endswith("\n") else text + "\n"
        return self.toml_loader.dumps(self.get_defaults(name))

    # ----- loading / saving

    def load_config(self, path: Union[str, Path]) -> Dict[str, Any]:
        """Load and validate config from TOML file."""
        config = self.toml_loader.load(path)
        errors = self.validator.validate(config)
        if errors:
            raise ValueError("Config validation failed:\n" + "\n".join(errors))
        return config

    def save_config(self, config: Dict[str, Any], path: Union[str, Path]) -> None:
        """Validate and save config to TOML file."""
        errors = self.validator.validate(config)
        if errors:
            raise ValueError("Config validation failed:\n" + "\n".join(errors))
        self.toml_loader.save(config, path)

    # ----- defaults

    def schema_defaults(self) -> Dict[str, Any]:
        """Every fixed node key at its schema (`balanced`) default."""
        defaults: Dict[str, Any] = {}
        for path, info in iter_node_keys(self.schema):
            if "default" in info:
                set_path(defaults, path, copy.deepcopy(info["default"]))
        return defaults

    def get_defaults(self, profile: str = BUILTIN_DEFAULT_PROFILE, shipped: bool = True) -> Dict[str, Any]:
        """Complete node config of `profile`.

        Schema defaults, then the profile's knob table, then (when `shipped`) the shipped
        `config/defconfigs/<profile>.toml` on top. Only keys the Rust loader accepts are included.
        `edge-1gb` / `dev` are overlays on the profile named by their `node.default_profile`.
        With `shipped=False` the result is what the Rust loader itself resolves for that profile:
        the baseline for a minimal config (`savedefconfig`).
        """
        self._require_profile(profile)
        builtin = self.builtin_profiles()
        overlay: Optional[Dict[str, Any]] = None
        path = self.defconfig_path(profile)
        if path is not None:
            overlay = self.toml_loader.load(path)

        base = profile
        if profile not in builtin:
            base = (overlay or {}).get("node", {}).get("default_profile", BUILTIN_DEFAULT_PROFILE)
            if base not in builtin:
                raise ValueError(f"defconfig '{profile}': node.default_profile '{base}' is not a profile")

        defaults = self.schema_defaults()
        self.profile_defaults.apply(base, defaults, overwrite=True)
        set_path(defaults, ("node", "default_profile"), base)
        if shipped and overlay:
            defaults = deep_merge(defaults, overlay)
        return defaults

    def diff_defaults(self, config: Dict[str, Any], profile: str = BUILTIN_DEFAULT_PROFILE) -> Dict[str, Any]:
        """Return only config values that differ from what the Rust loader resolves for `profile`.

        `node.default_profile` is kept whenever it is not the loader's own default, so the minimal
        file still selects the same profile.
        """
        result = deep_diff(config, self.get_defaults(profile, shipped=False))
        if profile != BUILTIN_DEFAULT_PROFILE:
            node = {"default_profile": profile, **{k: v for k, v in result.get("node", {}).items() if k != "default_profile"}}
            result = {"node": node, **{k: v for k, v in result.items() if k != "node"}}
        return result

    # ----- value parsing (CLI / menuconfig)

    def _parse_value(self, value_str: str, key_info: Dict[str, Any]) -> Any:
        """Parse a string value according to key schema."""
        key_type = key_info.get("type")

        if key_type == "bool":
            if value_str.lower() in ("true", "yes", "1", "on"):
                return True
            if value_str.lower() in ("false", "no", "0", "off"):
                return False
            raise ValueError(f"Invalid bool: {value_str}")
        if key_type == "int":
            try:
                return int(value_str)
            except ValueError:
                raise ValueError(f"Invalid int: {value_str}") from None
        if key_type == "float":
            try:
                return float(value_str)
            except ValueError:
                raise ValueError(f"Invalid float: {value_str}") from None
        if key_type == "enum":
            if value_str not in key_info.get("enum", []):
                raise ValueError(f"Invalid enum: {value_str} (one of {key_info.get('enum', [])})")
            return value_str
        if key_type == "list":
            parts = [s.strip() for s in value_str.split(",") if s.strip()]
            if key_info.get("items") == "int":
                try:
                    return [int(p) for p in parts]
                except ValueError:
                    raise ValueError(f"Invalid int list: {value_str}") from None
            return parts
        if key_type == "map":
            out: Dict[str, str] = {}
            for part in (s.strip() for s in value_str.split(",") if s.strip()):
                if "=" not in part:
                    raise ValueError(f"Invalid map entry '{part}' (use NAME=VALUE,NAME=VALUE)")
                k, v = part.split("=", 1)
                out[k.strip()] = v.strip()
            return out
        # string or duration
        return value_str
