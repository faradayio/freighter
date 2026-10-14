# Verifying a load

A load is complete when every shard in the manifest has a verdict that matches
the source. The process exit code is not enough: a single failed shard gets a
`failed` verdict and its lane carries on (see [Verdict reference](verdicts.md)).

## 1. Count the source per bucket

Over the manifest's whole bucket span:

```sql
SELECT bucket,
       COUNTIF(json IS NOT NULL) AS set_count,
       COUNTIF(json IS NULL) AS delete_count
FROM `my-project.my_dataset.freighter_source`
WHERE bucket BETWEEN @lo AND @hi
GROUP BY bucket
```

## 2. Compare per shard

For each shard in the manifest, sum `set_count` and `delete_count` over its
`[lo, hi]` and compare with its verdict. The shard passes when:

- the verdict file exists,
- `outcome` is `complete`,
- `cells_failed` and `row_deletes_failed` are 0,
- `cells_acknowledged == set_count` and `row_deletes_acknowledged == delete_count`.

Also flag verdict files for shard indexes the manifest doesn't contain: they
come from a different manifest written to the same prefix.

## 3. Re-run what failed

Write a new manifest containing only the failing shards, **with the same
`cell_timestamp_micros`**, and run it under a new prefix. Rewriting a cell
that already landed at the same timestamp leaves Bigtable as it was, so
re-running a shard that partly landed is safe. Use a new prefix for each
attempt so one attempt's verdicts never overwrite another's.

## What this does not check

The counts are freighter's own record of what Bigtable acknowledged. They do
not read Bigtable back. If you want that too, read a sample of keys from each
shard and compare the decompressed value with the source `json`. Counting
the Bigtable table instead is a full scan of it, which competes with live
reads.
