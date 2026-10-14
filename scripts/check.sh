#!/usr/bin/env bash
# CI's fmt / clippy / test steps, in its order, from any working directory.
# The emulator suite is separate: scripts/test-emulator.sh.
set -euo pipefail
cd "$(dirname "$0")/.."

cargo fmt --check
cargo clippy --all-targets -- -D warnings
# --all-targets skips the emulator target (`test = false`); lint it by name.
cargo clippy -p bq-freighter --test emulator -- -D warnings
cargo test -p bq-freighter
