# Quickstart

A whole load, top to bottom, into the Bigtable emulator on your machine:
build a source table, plan the load, run it, and verify it. **BigQuery is not
emulated.** `prepare`, `verify` and the mover's reads all talk to real
BigQuery, so you need a Google Cloud project where you can create a table in a
dataset, and Application Default Credentials for it
(`gcloud auth application-default login`). The example reads a few tens of
megabytes in BigQuery and leaves nothing behind once you clean up.

Below, `my-project.my_dataset` is your project and dataset. Replace it
everywhere. You also need `freighter` on your `PATH` ([Install](../README.md#install)),
the `gcloud` CLI, and `jq`.

## 1. Start the Bigtable emulator

```bash
gcloud components install beta bigtable cbt
gcloud beta emulators bigtable start --host-port=localhost:8086 &
export BIGTABLE_EMULATOR_HOST=localhost:8086
cbt -project example-project -instance example-instance createtable example_table
cbt -project example-project -instance example-instance createfamily example_table data
```

With `BIGTABLE_EMULATOR_HOST` set, freighter writes to the emulator, which
accepts any project, instance and app profile name.

## 2. Write a source query

Any query that yields `key STRING, json STRING` will do. This one makes
100,000 rows:

```bash
cat > query.sql <<'EOF'
SELECT
  FORMAT('k/%07d', n) AS key,
  TO_JSON_STRING(STRUCT(n, REPEAT('x', 200) AS pad)) AS json
FROM UNNEST(GENERATE_ARRAY(1, 100000)) AS n
EOF
```

## 3. Prepare the source table

```bash
freighter prepare --query-file query.sql \
  --table my-project.my_dataset.freighter_quickstart \
  --expires-days 1 --out source.json
```

`prepare` checks the table does not exist and that the query's columns are
STRING, finds quantile cut points over the keys, builds the table partitioned
by bucket and clustered on the key, and measures it. It logs each step:

```
prepare: scanning the query for 4000 quantiles
prepare: 100000 rows, 2 buckets after thinning (asked for 4000); 0 bytes scanned
prepare: building my-project.my_dataset.freighter_quickstart (2 buckets, expires in 1 days)
prepare: built my-project.my_dataset.freighter_quickstart (0 bytes scanned)
prepare: 100000 rows; bytes per row logical 240.9, gzip 63.7, raw 228.7; largest bucket 50001 rows
```

100,000 rows make two buckets, because `prepare` thins the cut to at least
50,000 rows a bucket. The query reads no table, hence 0 bytes scanned. The
table expires in a day. Run the same command again and it refuses, with exit
2, because the table now exists.

`source.json` is the *source record*, everything `plan` needs:

```bash
cat source.json
```

```json
{
  "version": 1,
  "table": "projects/my-project/datasets/my_dataset/tables/freighter_quickstart",
  "billing_project": "my-project",
  "rowkey_column": "key",
  "range_column": "bucket",
  "boundaries": [
    "k/0050000"
  ],
  "buckets": 2,
  "rows": 100000,
  "logical_bytes_per_row": 240.88895,
  "gzip_bytes_per_row": 63.67,
  "raw_bytes_per_row": 228.735,
  "max_rows_per_bucket": 50001,
  "null_json_deletes_row": false
}
```

Your boundary and sizes can differ slightly: the cut points are approximate
quantiles. The fields are described in
[Preparing a source table](preparing-a-source-table.md#the-source-record).

## 4. Plan the load

```bash
freighter plan --source source.json --lanes 1 --memory 2Gi \
  --bt-project example-project --bt-instance example-instance \
  --destination-table example_table --app-profile default \
  --out-dir load
freighter validate file://$PWD/load/manifest.json && echo valid
```

`plan` sizes the shards to fit one lane in 2 GiB and writes
`load/manifest.json`:

```
plan: step 0 — 1 lanes, 1 shards, 62823 cells in flight
```

Both buckets fit in one shard. `validate` runs the mover's own manifest check, prints
nothing and exits 0 when the mover would accept it. Have a look:

```bash
jq '{version, flush_cells, inflight_mutations, cell_timestamp_micros, shards}' load/manifest.json
```

The `cell_timestamp_micros` is the one timestamp every cell of this load will
carry. [Sizing a load](sizing.md) explains every number.

```json
{
  "version": 4,
  "flush_cells": 62823,
  "inflight_mutations": 62823,
  "cell_timestamp_micros": 1792004413880000,
  "shards": [
    {
      "index": 0,
      "range": {
        "kind": "int",
        "lo": 0,
        "hi": 1
      },
      "expect_empty": false,
      "lane": 0
    }
  ]
}
```

`flush_cells` is 4,000,000 target request bytes over 63.67 bytes a row.

## 5. Run it

```bash
freighter move file://$PWD/load/manifest.json
```

The mover reads each shard from BigQuery, sorts it, writes it to the emulator
in key order, and writes a verdict beside the manifest:

```bash
ls load
jq '{shard_index, outcome, rows_read, cells_acknowledged, cells_failed}' load/shard_0000.json
cbt -project example-project -instance example-instance count example_table
```

```json
{
  "shard_index": 0,
  "outcome": "complete",
  "rows_read": 100000,
  "cells_acknowledged": 100000,
  "cells_failed": 0
}
```

and `cbt count` prints `100000`.

## 6. Verify it

```bash
freighter verify file://$PWD/load/manifest.json --out report.json; echo "exit $?"
cat report.json
```

```json
{
  "version": 1,
  "ok": true,
  "shards": 1,
  "problems": [],
  "stray_verdicts": []
}
```

`ok: true` and exit 0 mean every shard in the manifest has a verdict, every
verdict is `complete` with nothing failed, and every shard's acknowledged
cells equal the source table's row count over the shard's buckets. That is the
load's success criterion; the mover's own exit code is not
([Verifying a load](verifying-a-load.md)).

## 7. Clean up

```bash
bq rm -f -t my-project:my_dataset.freighter_quickstart
pkill -f cbtemulator   # the emulator
rm -r load source.json report.json query.sql
```

For a real load, [Operating a load](operating.md) covers ramping up against a
table that serves live reads, re-running failed shards, and stopping a load.
