# Preparing a source table

Freighter reads a BigQuery table shaped like this:

| Column | Type | Meaning |
|---|---|---|
| `key` | STRING | the Bigtable row key |
| `json` | STRING | the cell value. NULL means "delete this row" when the manifest sets `null_json_deletes_row` |
| `bucket` | INT64 | the key's quantile bucket |

It is partitioned by integer range on `bucket` and clustered on `key`.
`freighter prepare` builds it from any query that yields `key STRING, json
STRING`.

## `freighter prepare`

```bash
freighter prepare --query-file q.sql --table my-project.my_dataset.freighter_source --out source.json
```

| Flag | Default | Meaning |
|---|---|---|
| `--query <sql>` or `--query-file <path>` | required, one of them | SQL yielding `key STRING, json STRING`. `--query-file -` reads stdin |
| `--table <project.dataset.table>` | required | the table to create. It must not exist yet |
| `--buckets <n>` | `4000` | an upper bound on the number of buckets, 1 to 4,000. The cut is thinned so each bucket holds at least 50,000 rows |
| `--expires-days <n>` | `7` | days until BigQuery drops the table; `0` means never |
| `--null-json-deletes-row` | off | keep rows whose `json` is NULL, as row deletes. Without it they are left out of the table |
| `--billing-project <p>` | the table's project | project the queries are billed to |
| `--out <path>` | stdout | where to write the [source record](#the-source-record) |

It refuses, with exit 2, before it scans anything:

- **a table that already exists.** The boundaries in a source table are also
  the Bigtable pre-split points, and a load may be reading the old table, so
  `prepare` never replaces one. Drop it deliberately (`bq rm -t
  my-project:my_dataset.freighter_source`) and run again.
- **columns of the wrong type.** `key` and `json` must both be scalar STRING.
  A native `JSON` column, an `INT64` key or an `ARRAY` is refused, as is a
  query missing either column or one BigQuery rejects. Cast in the query:
  `TO_JSON_STRING(...) AS json`.
- **a `--buckets` outside 1 to 4,000.**

It refuses, also with exit 2, after the first scan when the query yields no
rows to load.

If a step fails *after* the table is built, the error says so and ends "drop
the table and rerun": the table exists now, so a rerun refuses it until you
drop it. If the source record cannot be written to `--out`, `prepare` prints it
to stdout instead, because a rerun would cost a full scan.

Running it needs the BigQuery roles in the [README](../README.md):
`roles/bigquery.jobUser` on the billing project, `roles/bigquery.dataEditor` on
the dataset, and read access to whatever the query reads.

## What it does

Each step below is one query. `TARGET` is your query; the SQL is what
`prepare` runs, so you can check what it did or build a table by hand. Rows
with a NULL `key` are always left out. Rows with a NULL `json` are left out
unless you passed `--null-json-deletes-row`, in which case the
`AND json IS NOT NULL` below is dropped.

### 1. Check the table does not exist

A metadata read, before any query.

### 2. Probe the column types

```sql
SELECT key, json FROM (
TARGET
) LIMIT 0
```

This bills nothing and returns the result schema, which is how the wrong
column types are caught before a scan.

### 3. Find the cut points

```sql
SELECT APPROX_QUANTILES(key, 4000) AS cuts, MIN(key) AS lowest,
       COUNT(*) AS row_count
FROM (
TARGET
)
WHERE key IS NOT NULL AND json IS NOT NULL
```

The `4000` is `--buckets`. This is the one full scan of your query before the
build.

### 4. Choose the boundaries

From the result, `prepare` takes the **interior** cuts (the first and last
elements of `cuts` are dropped), thins them, drops any cut equal to `lowest`,
removes duplicates and sorts them. These are the boundaries. With *n*
boundaries the table has *n* + 1 buckets.

The bucket count follows three limits:

- **At most 4,000 buckets.** One `CREATE TABLE AS SELECT` may write at most
  4,000 partitions.
- **At least 50,000 rows per bucket.** Each bucket is read through its own
  partition, and a very fine cut of a small table spends its time walking
  partitions: 1,000 rows over 1,000 partitions read in 27 s, versus about a
  second from one. So `prepare` keeps every *k*-th cut, leaving at most
  `row_count / 50,000` buckets.
- **Small enough to fit in memory.** A shard is at least one bucket and is held
  whole in memory. If `plan` says one bucket does not fit, rebuild with more
  buckets. See [Sizing a load](sizing.md).

### 5. Build the table

```sql
CREATE TABLE `my-project`.`my_dataset`.`freighter_source`
PARTITION BY RANGE_BUCKET(bucket, GENERATE_ARRAY(0, 4000, 1))
CLUSTER BY key
OPTIONS (
  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
)
AS
SELECT key, json, RANGE_BUCKET(key, ARRAY<STRING>['boundary 1', 'boundary 2', ...]) AS bucket
FROM (
TARGET
)
WHERE key IS NOT NULL AND json IS NOT NULL
```

`GENERATE_ARRAY` runs to the bucket count, and the `OPTIONS` clause is left
out with `--expires-days 0`. `RANGE_BUCKET` puts keys below the first boundary
in bucket 0 and keys in `[boundary[i-1], boundary[i])` in bucket *i*. A
statement over BigQuery's 1 MB query limit, which takes thousands of very long
keys, is refused before it is sent: lower `--buckets`.

### 6. Count it

The table's row and byte counts, from its metadata.

### 7. Sample row sizes

```sql
(SELECT key, json FROM `my-project`.`my_dataset`.`freighter_source` WHERE bucket = 0 LIMIT 200)
UNION ALL
(SELECT key, json FROM `my-project`.`my_dataset`.`freighter_source` WHERE bucket = 400 LIMIT 200)
UNION ALL
...
```

Up to ten buckets spread evenly from bucket 0, 200 rows each. Over the sample,
`prepare` takes two means of the bytes one row adds to a write:
`len(key) + len(gzip(json))`, gzipped with the mover's own encoder, and
`len(key) + len(json)`. A row with a NULL `json` counts its key only.
BigQuery's logical row size is recorded too, but never used for sizing: it
overstates what is written several-fold.

### 8. Find the largest bucket

```sql
SELECT MAX(n) AS max_rows FROM (SELECT COUNT(*) AS n FROM `my-project`.`my_dataset`.`freighter_source` GROUP BY bucket)
```

Quantile cuts are not equal when keys repeat, and a load runs out of memory on
the biggest shard, not the average one.

## The source record

`prepare` writes one JSON document, the input to `plan`. Like the manifest, it
is a contract: it has an integer `version`, and `plan` refuses a version it
does not know and any field it does not recognize. An example lives at
[freighter/fixtures/freighter-source-record-example.json](../freighter/fixtures/freighter-source-record-example.json):

```json
{
  "version": 1,
  "table": "projects/example-project/datasets/freighter/tables/source",
  "billing_project": "example-project",
  "rowkey_column": "key",
  "range_column": "bucket",
  "boundaries": ["k/0500000", "k/1000000"],
  "buckets": 3,
  "rows": 1500000,
  "logical_bytes_per_row": 412.6,
  "gzip_bytes_per_row": 97.3,
  "raw_bytes_per_row": 391.0,
  "max_rows_per_bucket": 620114,
  "null_json_deletes_row": false
}
```

| Field | Type | Meaning |
|---|---|---|
| `version` | integer | must be `1` |
| `table` | string | `projects/<p>/datasets/<d>/tables/<t>`, the manifest's `source_table` form |
| `billing_project` | string | project the queries were billed to; the manifest's `billing_project` |
| `rowkey_column` | string | always `key` |
| `range_column` | string | always `bucket` |
| `boundaries` | array of strings | the interior cut points, strictly ascending. Bucket *i* ≥ 1 starts at `boundaries[i-1]` |
| `buckets` | integer | `boundaries` + 1 |
| `rows` | integer | rows in the table |
| `logical_bytes_per_row` | number | BigQuery's bytes per row. For the reader; never used for sizing |
| `gzip_bytes_per_row` | number | sampled mean of `len(key) + len(gzip(json))`; `plan` sizes a `gzip` load from it |
| `raw_bytes_per_row` | number | sampled mean of `len(key) + len(json)`; `plan` sizes a `none` load from it |
| `max_rows_per_bucket` | integer | row count of the largest bucket |
| `null_json_deletes_row` | bool | whether NULL `json` rows were kept as deletes |

## Rules that are easy to break

These matter most when you write the query, and when you build a table by
hand.

- **Never transform `key` in the bucketing.** Three orderings must agree for a
  bucket to be one contiguous key run: BigQuery compares STRINGs by Unicode
  code point, freighter sorts by UTF-8 bytes, and Bigtable orders row keys by
  bytes. Code-point order and UTF-8 byte order agree for all valid Unicode, so
  they do. A `COLLATE`, `LOWER` or `NORMALIZE` on `key` breaks that silently:
  the load still succeeds, writing out of order.
- **Never hash the key into buckets.** Freighter can't detect it; you just lose
  ordered writes.
- **Keys must be unique.** Two source rows with one key become two writes to
  one cell; the last one wins, and the counts still match.
- **Don't rebuild the table under a running load.** Verification counts the
  source table again after the load. If its contents changed, the counts
  won't match.

## Pre-splitting the Bigtable table

The boundaries are also good split points for the destination table. Creating
it pre-split spreads the first writes across tablets instead of waiting for
Bigtable to split under load. They are the source record's `boundaries`:

```bash
cbt createtable my_table "families=data" "splits=$(jq -r '.boundaries | join(",")' source.json)"
```

A boundary containing a comma needs a different route, such as the Bigtable
admin API.
