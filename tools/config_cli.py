#!/usr/bin/env python3
"""Configuration CLI tool - non-interactive and line-by-line modes."""

import sys
import argparse
import tomllib
from pathlib import Path

from rtconfig import BUILTIN_DEFAULT_PROFILE, ConfigManager, deep_merge, get_path, set_path


def _load(manager: ConfigManager, path: str):
    """Load a TOML file; print the error and return None when it cannot be read or parsed."""
    try:
        return manager.toml_loader.load(path)
    except (OSError, tomllib.TOMLDecodeError) as e:
        print(f"Error: cannot read {path}: {e}", file=sys.stderr)
        return None


def _profile_of(config) -> str:
    return get_path(config, ("node", "default_profile"), BUILTIN_DEFAULT_PROFILE)


def cmd_defconfig(args):
    """Print the defconfig of a profile (any file in config/defconfigs)."""
    manager = ConfigManager()
    try:
        content = manager.defconfig_text(args.profile or BUILTIN_DEFAULT_PROFILE)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    sys.stdout.write(content)
    return 0


def cmd_oldconfig(args):
    """Fill missing keys with defaults."""
    manager = ConfigManager()

    if not Path(args.config).exists():
        print(f"Error: Config file not found: {args.config}", file=sys.stderr)
        return 1

    config = _load(manager, args.config)
    if config is None:
        return 1
    errors = manager.validator.validate(config)
    if errors:
        print(f"Validation failed for {args.config} (not modified):", file=sys.stderr)
        for error in errors:
            print(f"  - {error}", file=sys.stderr)
        return 1

    # Defaults of the file's profile first, then everything the file sets on top.
    try:
        defaults = manager.get_defaults(_profile_of(config))
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1
    result = deep_merge(defaults, config)

    manager.save_config(result, args.config)
    print(f"Updated {args.config} with missing defaults")
    return 0


def cmd_savedefconfig(args):
    """Save minimal config with only non-default values."""
    manager = ConfigManager()

    if not Path(args.config).exists():
        print(f"Error: Config file not found: {args.config}", file=sys.stderr)
        return 1

    config = _load(manager, args.config)
    if config is None:
        return 1
    errors = manager.validator.validate(config)
    if errors:
        print(f"Validation failed for {args.config}:", file=sys.stderr)
        for error in errors:
            print(f"  - {error}", file=sys.stderr)
        return 1
    try:
        diff = manager.diff_defaults(config, _profile_of(config))
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    out_path = args.output or ".config"
    manager.save_config(diff, out_path)
    print(f"Saved minimal config to {out_path}")
    return 0


def cmd_listprofiles(args):
    """List available profiles (built-in profiles, then the other shipped defconfigs)."""
    for profile in ConfigManager().profiles():
        print(profile)

    return 0


def cmd_validate(args):
    """Validate a config file."""
    manager = ConfigManager()

    config_file = args.file or "rt.toml"
    if not Path(config_file).exists():
        print(f"Error: Config file not found: {config_file}", file=sys.stderr)
        return 1

    config = _load(manager, config_file)
    if config is None:
        return 1
    errors = manager.validator.validate(config)

    if errors:
        print(f"Validation failed for {config_file}:")
        for error in errors:
            print(f"  - {error}")
        return 1
    else:
        print(f"{config_file} is valid")
        return 0


def cmd_config(args):
    """Interactive line-by-line configuration."""
    manager = ConfigManager()

    config_file = args.config or "rt.toml"

    # Load existing config if it exists
    if Path(config_file).exists():
        config = _load(manager, config_file)
        if config is None:
            return 1
    else:
        config = {}

    profile = _profile_of(config)
    defaults = manager.get_defaults(profile)

    for full_key, (section_name, key_name, key_info) in manager.schema_loader.get_all_keys().items():
        path = tuple(full_key.split("."))
        current = get_path(config, path)
        default = get_path(defaults, path, key_info.get("default"))

        shown = current if current is not None else default
        help_text = key_info.get("help", "")
        prompt = f"{full_key}: [{shown}] ({help_text})\n> "

        try:
            value_str = input(prompt).strip()
            if not value_str:
                # Keep the current value, or take the profile default
                if current is None:
                    set_path(config, path, default)
            else:
                set_path(config, path, manager._parse_value(value_str, key_info))
        except KeyboardInterrupt:
            print("\nInterrupted")
            return 1
        except EOFError:
            print("\nInput closed, nothing saved", file=sys.stderr)
            return 1
        except ValueError as e:
            print(f"Error: {e} (keeping {shown})", file=sys.stderr)

        if full_key == "node.default_profile":
            # later prompts show the defaults of the profile just chosen
            profile = _profile_of(config)
            defaults = manager.get_defaults(profile)

    # Validate
    errors = manager.validator.validate(config)
    if errors:
        print("\nValidation errors:")
        for error in errors:
            print(f"  - {error}")
        return 1

    # Save
    manager.toml_loader.save(config, config_file)
    print(f"\nSaved to {config_file}")
    return 0


def cmd_set(args):
    """Set a single config value."""
    manager = ConfigManager()

    config_file = args.config or "rt.toml"

    # Load existing config
    if Path(config_file).exists():
        config = _load(manager, config_file)
        if config is None:
            return 1
    else:
        config = {}

    # Parse key=value
    if "=" not in args.setting:
        print("Error: Use format KEY=VALUE (e.g., sched.processors=4)", file=sys.stderr)
        return 1

    full_key, value_str = args.setting.split("=", 1)
    full_key = full_key.strip()

    # Get key info from schema (fixed keys of node sections; nested tables use dots: sched.kinds.deno.min)
    all_keys = manager.schema_loader.get_all_keys()
    if full_key not in all_keys:
        print(f"Error: Unknown key '{full_key}'", file=sys.stderr)
        return 1

    _, _, key_info = all_keys[full_key]

    # Parse value
    try:
        value = manager._parse_value(value_str, key_info)
    except ValueError as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

    set_path(config, tuple(full_key.split(".")), value)

    # Validate
    errors = manager.validator.validate(config)
    if errors:
        print("Validation failed:")
        for error in errors:
            print(f"  - {error}")
        return 1

    # Save
    manager.toml_loader.save(config, config_file)
    print(f"Set {full_key} = {value} in {config_file}")
    return 0


def main(argv=None):
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(description="Rust runtime configuration tool")
    subparsers = parser.add_subparsers(dest="command", help="Command to run")

    # defconfig command
    sub = subparsers.add_parser("defconfig", help="Print the defconfig of a profile")
    sub.add_argument(
        "profile",
        nargs="?",
        default=BUILTIN_DEFAULT_PROFILE,
        help="Profile name: one of `listprofiles` (max_density, balanced, min_latency, edge-1gb, dev); default balanced",
    )
    sub.set_defaults(func=cmd_defconfig)

    # oldconfig command
    sub = subparsers.add_parser("oldconfig", help="Fill missing keys with defaults")
    sub.add_argument("--config", "-c", default="rt.toml", help="Config file")
    sub.set_defaults(func=cmd_oldconfig)

    # savedefconfig command
    sub = subparsers.add_parser("savedefconfig", help="Save minimal config diff")
    sub.add_argument("--config", "-c", default="rt.toml", help="Config file")
    sub.add_argument("--output", "-o", help="Output file (default: .config)")
    sub.set_defaults(func=cmd_savedefconfig)

    # listprofiles command
    sub = subparsers.add_parser("listprofiles", help="List available profiles")
    sub.set_defaults(func=cmd_listprofiles)

    # validate command
    sub = subparsers.add_parser("validate", help="Validate config file")
    sub.add_argument("file", nargs="?", default="rt.toml", help="Config file to validate")
    sub.set_defaults(func=cmd_validate)

    # config command (interactive)
    sub = subparsers.add_parser("config", help="Interactive line-by-line configuration")
    sub.add_argument("--config", "-c", default="rt.toml", help="Config file")
    sub.set_defaults(func=cmd_config)

    # set command
    sub = subparsers.add_parser("set", help="Set single config value")
    sub.add_argument("setting", help="KEY=VALUE (e.g., sched.processors=4, sched.kinds.deno.max=2)")
    sub.add_argument("--config", "-c", default="rt.toml", help="Config file")
    sub.set_defaults(func=cmd_set)

    args = parser.parse_args(argv)

    if not args.command:
        parser.print_help()
        return 1

    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
