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
gcloud auth application-default print-access-token >/dev/null 2>&1 || {
  echo "ERROR: no Application Default Credentials — run: gcloud auth application-default login" >&2
  exit 1
}

# --test-threads=1: serial, so the echoed freighter stderr of the end-to-end
# story does not interleave with the jobs tests.
cargo test -p bq-freighter --test bigquery -- --test-threads=1 "$@"
