#!/usr/bin/env bash
# Runs the freighter BigQuery suite: prepare -> plan -> move (emulator) ->
# verify against a real BigQuery dataset you own, plus the jobs client
# against the live API.
#
# The suite HARD FAILS without its inputs rather than skipping — a silently
# skipped integration test is worse than none — so this script checks them
# first. It builds a 1.5M-row table in the dataset (expires in a day) and
# drops what it creates.
#
#   FREIGHTER_TEST_DATASET=<project>.<dataset>   a dataset you may write to
#   BIGTABLE_EMULATOR_HOST=localhost:8086          a running cbtemulator
#   Application Default Credentials                gcloud auth application-default login
set -euo pipefail
cd "$(dirname "$0")/.."

: "${FREIGHTER_TEST_DATASET:?set FREIGHTER_TEST_DATASET=<project>.<dataset> (a BigQuery dataset you may write to)}"
: "${BIGTABLE_EMULATOR_HOST:?set BIGTABLE_EMULATOR_HOST (start one: gcloud beta emulators bigtable start --host-port=localhost:8086)}"
if ! command -v cbt >/dev/null 2>&1; then
  echo "ERROR: cbt CLI missing — run: gcloud components install cbt" >&2
  exit 1
fi

# --test-threads=1: the end-to-end test is one sequential story.
cargo test -p bq-freighter --test bigquery -- --test-threads=1 "$@"
