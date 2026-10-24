# Configuration tooling makefile
# Targets for managing Rust runtime configuration

.PHONY: menuconfig config defconfig oldconfig savedefconfig listprofiles validate-config test-config help-config

# Default config file path
CONFIG ?= rt.toml

# Profile for `make defconfig`: any file in config/defconfigs (see `make listprofiles`)
PROFILE ?= balanced

# Python tools directory
TOOLS_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))../tools

help-config:
	@echo "Configuration targets:"
	@echo "  make menuconfig          - Interactive curses menu (Linux menuconfig style)"
	@echo "  make config              - Interactive line-by-line configuration"
	@echo "  make defconfig PROFILE=p - Print the defconfig of profile p (default: balanced)"
	@echo "  make oldconfig           - Fill missing keys with profile defaults"
	@echo "  make savedefconfig       - Save minimal config diff (to .config)"
	@echo "  make listprofiles        - List available profiles (every file in config/defconfigs)"
	@echo "  make test-config         - Run the tools/ unit tests"
	@echo "  make validate-config     - Validate current config"
	@echo ""
	@echo "Options:"
	@echo "  CONFIG=path/to/rt.toml   - Override config file (default: rt.toml)"
	@echo "  PROFILE=max_density      - Profile for defconfig: balanced, max_density, min_latency, edge-1gb, dev"
	@echo ""
	@echo "Profiles available (config/defconfigs/<name>.toml):"
	@echo "  - balanced     : Medium throughput/latency (default)"
	@echo "  - max_density  : High throughput, aggressive caching"
	@echo "  - min_latency  : Low latency, preloading, thread pinning"
	@echo "  - edge-1gb     : 1vCPU/1GB edge device (balanced knobs, small limits)"
	@echo "  - dev          : Local development (loopback, ./data, debug logging)"

menuconfig:
	@python3 $(TOOLS_DIR)/menuconfig.py --config $(CONFIG)

config:
	@python3 $(TOOLS_DIR)/config_cli.py config --config $(CONFIG)

defconfig:
	@python3 $(TOOLS_DIR)/config_cli.py defconfig $(PROFILE)

oldconfig:
	@python3 $(TOOLS_DIR)/config_cli.py oldconfig --config $(CONFIG)

savedefconfig:
	@python3 $(TOOLS_DIR)/config_cli.py savedefconfig --config $(CONFIG)

listprofiles:
	@python3 $(TOOLS_DIR)/config_cli.py listprofiles

validate-config:
	@python3 $(TOOLS_DIR)/config_cli.py validate $(CONFIG)

# Testing target
test-config:
	@python3 -m unittest discover -s $(TOOLS_DIR) -p 'test_*.py' -v

