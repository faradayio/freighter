#!/usr/bin/env bash
# Runs the freighter emulator suite against a throwaway cbtemulator.
#
# The suite HARD FAILS without BIGTABLE_EMULATOR_HOST rather than skipping —
# a silently-skipped Bigtable regression test is worse than no test at all —
# so this script is the only supported way to run it. It is also why the
# target carries `test = false` in Cargo.toml and stays out of a bare
# `cargo test`.
set -euo pipefail
cd "$(dirname "$0")/.."

PORT=8086
EMULATOR_CMD="cbtemulator --host=localhost --port=${PORT}"

if ! gcloud components list --filter=id:bigtable --format='value(state.name)' 2>/dev/null | grep -q Installed; then
  echo "ERROR: gcloud bigtable emulator component missing — run: gcloud components install bigtable" >&2
  exit 1
fi

if ! command -v cbt >/dev/null 2>&1; then
  echo "ERROR: cbt CLI missing — run: gcloud components install cbt" >&2
  echo "       (bigtable_rs has no admin API; the suite creates tables with cbt)" >&2
  exit 1
fi

if nc -z localhost "$PORT" 2>/dev/null; then
  echo "ERROR: localhost:${PORT} is already in use — a stale emulator would serve these" >&2
  echo "       tests stale data. Kill it first: pkill -f '${EMULATOR_CMD}'" >&2
  exit 1
fi

gcloud beta emulators bigtable start --host-port="localhost:${PORT}" &
EMULATOR_PID=$!
# `gcloud emulators start` is a wrapper that execs cbtemulator as a CHILD, and
# killing the wrapper alone leaves that child holding the port. Kill both.
cleanup() {
  kill "$EMULATOR_PID" 2>/dev/null || true
  pkill -f "$EMULATOR_CMD" 2>/dev/null || true
}
trap cleanup EXIT

export BIGTABLE_EMULATOR_HOST="localhost:${PORT}"
for _ in $(seq 1 30); do nc -z localhost "$PORT" && break; sleep 0.5; done
if ! nc -z localhost "$PORT"; then
  echo "ERROR: bigtable emulator never came up on localhost:${PORT}" >&2
  exit 1
fi

# --test-threads=1: the emulator is a single in-memory process; serial
# execution keeps failures readable.
cargo test -p bq-freighter --test emulator -- --test-threads=1 "$@"
