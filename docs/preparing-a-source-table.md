# Preparing a source table

Freighter reads a BigQuery table shaped like this:

| Column | Type | Meaning |
|---|---|---|
| `key` | STRING | the Bigtable row key (any name; the manifest's `rowkey_column`) |
| `json` | STRING | the cell value, gzipped on write. NULL means "delete this row" when the manifest sets `null_json_deletes_row` |
| `bucket` | INT64 | the key's quantile bucket (any name; the manifest's `range_column`) |

It is partitioned by integer range on `bucket` and clustered on `key`.

You start from any query that yields `key STRING, json STRING`. Call it
`TARGET` below. Building the table takes two queries.

## 1. Find the cut points

```sql
SELECT APPROX_QUANTILES(key, 1000) AS cuts, MIN(key) AS lowest, COUNT(*) AS row_count
FROM (TARGET)
WHERE key IS NOT NULL AND json IS NOT NULL
```

Drop the `AND json IS NOT NULL` if NULL `json` rows are deletes you want
loaded.

From the result, take the **interior** cuts (drop the first and last element
of `cuts`), remove duplicates and any cut equal to `lowest`, and sort them.
These are the boundaries. With *n* boundaries the table has *n* + 1 buckets.

Choosing the bucket count:

- **At most 4,000 buckets.** One `CREATE TABLE AS SELECT` may write at most
  4,000 partitions.
- **At least ~50,000 rows per bucket.** Each bucket is read through its own
  partition; a very fine cut of a small table spends its time walking
  partitions (1,000 rows over 1,000 partitions read in 27 s, versus about a
  second from one). For small tables, thin the boundaries by keeping every
  *k*-th one.
- **Small enough to fit in memory.** A shard is at least one bucket and is held
  whole in memory. See [Sizing a load](sizing.md).

## 2. Build the table

```sql
CREATE TABLE `my-project.my_dataset.freighter_source`
PARTITION BY RANGE_BUCKET(bucket, GENERATE_ARRAY(0, 1001, 1))  -- 0 .. buckets
CLUSTER BY key
OPTIONS (expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL 7 DAY))
AS
SELECT key, json,
       RANGE_BUCKET(key, ARRAY<STRING>['boundary 1', 'boundary 2', ...]) AS bucket
FROM (TARGET)
WHERE key IS NOT NULL AND json IS NOT NULL
```

`RANGE_BUCKET` puts keys below the first boundary in bucket 0 and keys in
`[boundary[i-1], boundary[i])` in bucket *i*.

## Rules that are easy to break

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
Bigtable to split under load:

```bash
cbt createtable my_table "families=data" "splits=boundary 1,boundary 2,..."
```

## Measuring row size

For sizing you need the mean bytes one row adds to a write: `len(key) +
len(gzip(json))`, not BigQuery's logical row size, which overstates it
several-fold. Sample a few hundred rows from a spread of buckets and gzip the
`json` locally at level 6 (freighter's level). Also record the row count of
the largest bucket:

```sql
SELECT MAX(n) AS max_rows FROM (SELECT COUNT(*) AS n FROM `...freighter_source` GROUP BY bucket)
```
