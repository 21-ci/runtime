#!/usr/bin/env python3
"""Interactive curses-based configuration menu (Linux menuconfig style)."""

import sys
import os
import curses
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
import json

from rtconfig import ConfigManager, TOMLLoader, get_path, iter_spec_keys, set_path


class MenuConfig:
    """Interactive menu configuration using curses."""

    def __init__(self, config_path: str = "rt.toml"):
        """Initialize menuconfig."""
        self.config_path = Path(config_path)
        self.manager = ConfigManager()

        # Load existing config or defaults
        if self.config_path.exists():
            self.config = self.manager.toml_loader.load(self.config_path)
        else:
            self.config = {}

        self.profile = self.config.get("node", {}).get("default_profile", "balanced")
        self.defaults = self.manager.get_defaults(self.profile)

        # Build menu structure
        # rt.toml sections that have fixed keys (no per-service blocks like `scaling`)
        self.sections = [s for s in self.manager.schema_loader.node_sections() if list(iter_spec_keys(self.manager.schema[s]))]
        self.current_section = 0
        self.current_key_in_section = {}

        # Search state
        self.search_text = ""
        self.search_results = []
        self.in_search = False

    def get_section_keys(self, section: str) -> List[Tuple[str, Dict]]:
        """Get keys in a section."""
        if section not in self.sections:
            return []

        # nested tables are listed with their dotted path (`kinds.deno.min`)
        return [(".".join(path), info) for path, info in iter_spec_keys(self.manager.schema[section])]

    def get_value(self, section: str, key: str) -> Any:
        """Get current value for a key."""
        path = (section, *key.split("."))
        value = get_path(self.config, path)
        return value if value is not None else get_path(self.defaults, path)

    def set_value(self, section: str, key: str, value: Any) -> None:
        """Set value in config."""
        set_path(self.config, (section, *key.split(".")), value)

    def show_menu(self, stdscr):
        """Main menu loop."""
        curses.curs_set(0)  # Hide cursor
        stdscr.keypad(True)

        while True:
            stdscr.clear()
            height, width = stdscr.getmaxyx()

            if self.in_search:
                self.show_search(stdscr, height, width)
            else:
                self.show_main_menu(stdscr, height, width)

            key = stdscr.getch()

            if not self.handle_input(key, height, width):
                break

        curses.curs_set(1)  # Show cursor

    def show_main_menu(self, stdscr, height: int, width: int) -> None:
        """Display main menu."""
        # Title
        stdscr.addstr(0, 0, "Rust Runtime Configuration", curses.A_BOLD | curses.A_UNDERLINE)
        stdscr.addstr(1, 0, f"Profile: {self.profile} | File: {self.config_path}")

        # Section list
        y = 3
        for i, section in enumerate(self.sections):
            if i == self.current_section:
                stdscr.addstr(y, 2, f"> {section}", curses.A_REVERSE)
            else:
                stdscr.addstr(y, 2, f"  {section}")
            y += 1

            if y > height - 6:
                stdscr.addstr(height - 3, 0, "... more sections ...")
                break

        # Help text
        stdscr.addstr(height - 2, 0, "UP/DOWN: navigate | ENTER: edit | /: search | L: load | S: save | Q: quit | ?: help")

    def show_section_menu(self, stdscr, section: str, height: int, width: int) -> None:
        """Display menu for a section's keys."""
        stdscr.clear()

        # Title
        stdscr.addstr(0, 0, f"[{section}]", curses.A_BOLD | curses.A_UNDERLINE)

        keys = self.get_section_keys(section)
        if not self.current_key_in_section:
            self.current_key_in_section[section] = 0

        current_idx = self.current_key_in_section.get(section, 0)

        y = 2
        for i, (key_name, key_info) in enumerate(keys):
            value = self.get_value(section, key_name)

            # Format value display
            if isinstance(value, list):
                value_str = "[" + ", ".join(str(v) for v in value) + "]"
            elif isinstance(value, bool):
                value_str = "true" if value else "false"
            else:
                value_str = str(value) if value is not None else ""

            help_text = key_info.get("help", "")

            line = f"{key_name}: {value_str} # {help_text}"[:width-4]

            if i == current_idx:
                stdscr.addstr(y, 2, line, curses.A_REVERSE)
            else:
                stdscr.addstr(y, 2, line)

            y += 1
            if y > height - 4:
                stdscr.addstr(height - 2, 0, "... more keys ...")
                break

        # Help text
        stdscr.addstr(height - 1, 0, "UP/DOWN: navigate | ENTER: edit | ?: help | <: back")

    def show_search(self, stdscr, height: int, width: int) -> None:
        """Display search interface."""
        stdscr.clear()
        stdscr.addstr(0, 0, "Search: " + self.search_text, curses.A_BOLD)

        if self.search_text:
            # Find matching keys
            self.search_results = []
            for full_key, (_section, _key, key_info) in self.manager.schema_loader.get_all_keys().items():
                help_text = key_info.get("help", "")
                if self.search_text.lower() in full_key.lower() or \
                   self.search_text.lower() in help_text.lower():
                    self.search_results.append((full_key, key_info, help_text))

            # Display results
            y = 2
            for i, (full_key, key_info, help_text) in enumerate(self.search_results[:height-4]):
                value = get_path(self.config, tuple(full_key.split(".")))
                value_str = str(value) if value is not None else ""

                line = f"{full_key}: {value_str} # {help_text}"[:width-4]
                stdscr.addstr(y, 2, line)
                y += 1

        stdscr.addstr(height - 1, 0, "Type to search, ESC to exit search")

    def edit_key(self, stdscr, section: str, key_name: str) -> None:
        """Edit a single key value."""
        keys = self.get_section_keys(section)
        key_info = None

        for k_name, k_info in keys:
            if k_name == key_name:
                key_info = k_info
                break

        if not key_info:
            return

        current_value = self.get_value(section, key_name)
        key_type = key_info.get("type")

        if key_type == "bool":
            # Toggle bool
            new_value = not (current_value or False)
            self.set_value(section, key_name, new_value)
        elif key_type == "enum":
            # Cycle through enum values
            enum_values = key_info.get("enum", [])
            if enum_values:
                try:
                    idx = enum_values.index(current_value)
                    new_value = enum_values[(idx + 1) % len(enum_values)]
                    self.set_value(section, key_name, new_value)
                except ValueError:
                    self.set_value(section, key_name, enum_values[0])
        else:
            # Text input
            curses.curs_set(1)
            stdscr.clear()
            stdscr.addstr(0, 0, f"Edit {section}.{key_name}:")
            stdscr.addstr(1, 0, f"Type: {key_type}")
            if "help" in key_info:
                stdscr.addstr(2, 0, f"Help: {key_info['help']}")
            if "default" in key_info:
                stdscr.addstr(3, 0, f"Default: {key_info['default']}")

            stdscr.addstr(5, 0, "Value: ")
            value_input = self.get_string_input(stdscr, str(current_value or ""), 7)

            if value_input is not None:
                try:
                    parsed = self.manager._parse_value(value_input, key_info)
                    self.set_value(section, key_name, parsed)
                except ValueError as e:
                    # Error - show message
                    stdscr.addstr(8, 0, f"Error: {e}", curses.A_BOLD)
                    stdscr.getch()

            curses.curs_set(0)

    def get_string_input(self, stdscr, initial: str = "", y: int = 0) -> Optional[str]:
        """Get string input from user."""
        x = 0
        text = list(initial)

        while True:
            stdscr.addstr(y, 0, "Value: " + "".join(text) + " " * 30)
            stdscr.addstr(y, 7 + len(text), "", curses.A_REVERSE)
            stdscr.refresh()

            key = stdscr.getch()

            if key == ord('\n'):
                return "".join(text)
            elif key == ord('\x1b'):  # ESC
                return None
            elif key == curses.KEY_BACKSPACE or key == ord('\x08'):
                if text:
                    text.pop()
            elif 32 <= key <= 126:
                text.append(chr(key))

    def handle_input(self, key: int, height: int, width: int) -> bool:
        """Handle keyboard input. Return False to exit."""
        if self.in_search:
            if key == 27:  # ESC
                self.in_search = False
                self.search_text = ""
            elif key == curses.KEY_BACKSPACE or key == ord('\x08'):
                self.search_text = self.search_text[:-1]
            elif 32 <= key <= 126:
                self.search_text += chr(key)
            return True

        # Main menu
        if key == ord('q') or key == ord('Q'):
            # Save prompt
            return self.prompt_save_and_quit()
        elif key == ord('?'):
            self.show_help()
        elif key == ord('/'):
            self.in_search = True
            self.search_text = ""
        elif key == ord('L'):
            self.load_defconfig()
        elif key == ord('S'):
            self.save_config()
        elif key == curses.KEY_UP:
            self.current_section = max(0, self.current_section - 1)
        elif key == curses.KEY_DOWN:
            self.current_section = min(len(self.sections) - 1, self.current_section + 1)
        elif key == ord('\n'):
            # Enter section
            section = self.sections[self.current_section]
            self.show_section_editor(section)

        return True

    def show_section_editor(self, section: str) -> None:
        """Enter section editor."""
        # This would need full curses handling
        pass

    def prompt_save_and_quit(self) -> bool:
        """Prompt to save before quitting."""
        # Save config
        try:
            errors = self.manager.validator.validate(self.config)
            if errors:
                print(f"\nValidation errors before saving:")
                for error in errors:
                    print(f"  - {error}")
                input("Press Enter to continue...")
                return True

            self.manager.toml_loader.save(self.config, self.config_path)
            print(f"\nSaved configuration to {self.config_path}")
        except Exception as e:
            print(f"\nError saving: {e}")

        return False

    def show_help(self) -> None:
        """Show help information."""
        pass

    def load_defconfig(self) -> None:
        """Load a defconfig."""
        pass

    def save_config(self) -> None:
        """Save configuration."""
        try:
            errors = self.manager.validator.validate(self.config)
            if errors:
                print(f"\nValidation errors:")
                for error in errors:
                    print(f"  - {error}")
                return

            self.manager.toml_loader.save(self.config, self.config_path)
            print(f"\nSaved to {self.config_path}")
        except Exception as e:
            print(f"Error saving: {e}")


def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(description="Interactive runtime configuration menu")
    parser.add_argument("--config", "-c", default="rt.toml", help="Config file path")
    args = parser.parse_args()

    # Check if TTY is available
    if not sys.stdin.isatty():
        print("Error: menuconfig requires a terminal (TTY)", file=sys.stderr)
        print("Use 'make config' or 'python3 config_cli.py' for non-interactive mode", file=sys.stderr)
        sys.exit(2)

    try:
        menuconfig = MenuConfig(args.config)
        curses.wrapper(menuconfig.show_menu)
    except KeyboardInterrupt:
        print("\nInterrupted")
        sys.exit(1)


if __name__ == "__main__":
    main()
