# Verifying a load

A load is complete when every shard in the manifest has a verdict that matches
the source. The mover's exit code is not enough: a single failed shard gets a
`failed` verdict and its lane carries on (see [Verdict reference](verdicts.md)).
`freighter verify` makes the comparison.

## `freighter verify`

```bash
freighter verify gs://my-bucket/loads/2026-10-14/manifest.json --out report.json
```

| Flag | Default | Meaning |
|---|---|---|
| `<manifest-uri>` | required | the manifest the mover ran, `gs://` or `file://`. Its verdicts are read from beside it |
| `--billing-project <p>` | the manifest's `billing_project` | project the count query is billed to |
| `--out <path>` | stdout | where to write the report |

It exits:

- **0** when every shard checks out and there are no stray verdicts (`ok` is
  `true`),
- **1** when the report lists any problem or stray verdict,
- **2** when the check itself could not run: the manifest or a verdict could
  not be read or was refused, or the count query failed. No report is written.

It reads the source table and the verdicts only. It never reads Bigtable; see
[What this does not check](#what-this-does-not-check). It needs the mover's
BigQuery read access plus `roles/bigquery.jobUser` on the billing project.

## The verify report

The report is a contract like the manifest, with an integer `version`. As an
output, its readers should ignore fields they do not know, as for verdicts;
a breaking change gets a new `version`. An example lives at
[freighter/fixtures/freighter-verify-report-example.json](../freighter/fixtures/freighter-verify-report-example.json):

```json
{
  "version": 1,
  "ok": false,
  "shards": 200,
  "problems": [
    {"shard": 4, "reason": "no verdict"},
    {"shard": 7, "reason": "acknowledged 149 cells, source has 150 rows to set"}
  ],
  "stray_verdicts": [9]
}
```

| Field | Type | Meaning |
|---|---|---|
| `version` | integer | `1` |
| `ok` | bool | `true` when `problems` and `stray_verdicts` are both empty. The exit code follows it |
| `shards` | integer | shards in the manifest |
| `problems` | array | one entry per way a shard's verdict disagrees with the source, in manifest order: `shard` is the shard index, `reason` says what is wrong |
| `stray_verdicts` | array of integers | indexes of verdict files beside the manifest for shards the manifest does not contain, ascending |

## What it checks

### 1. Read the verdicts

`verify` lists the manifest's own directory, without descending into
subdirectories, and reads every file named `shard_<digits>.json`. Anything
else there is ignored, including a ramp's `step-N/` directories. A verdict
that cannot be parsed, or whose file name and `shard_index` disagree, stops
the check with exit 2: a directory like that is not one `verify` can judge.

### 2. Count the source per bucket

One query over the span from the lowest `lo` to the highest `hi` among the
manifest's shards:

```sql
SELECT `bucket` AS bucket,
       COUNTIF(json IS NOT NULL) AS set_count,
       COUNTIF(json IS NULL) AS delete_count
FROM `my-project`.`my_dataset`.`freighter_source`
WHERE `bucket` BETWEEN 0 AND 3999
GROUP BY bucket
```

The column is the manifest's `range_column`. The table is partitioned on it,
so the query reads only the partitions in the span. A ramp step's shards are
one contiguous run of buckets, so verifying a step costs exactly its buckets.

### 3. Compare per shard

For each shard in the manifest, `verify` sums `set_count` and `delete_count`
over its `[lo, hi]` and compares them with its verdict. The shard passes when:

- the verdict file exists (else `no verdict`),
- `outcome` is `complete`, and `cells_failed` and `row_deletes_failed` are 0
  (else, for example, `outcome failed, 12 cells and 0 row deletes failed`),
- `cells_acknowledged` equals `set_count` (else `acknowledged 149 cells,
  source has 150 rows to set`),
- `row_deletes_acknowledged` equals `delete_count` (else `acknowledged 3 row
  deletes, source has 4 rows to delete`).

It also lists verdict files for shard indexes the manifest does not contain
as `stray_verdicts`. They come from a different manifest written to the same
directory, and they mean the directory is shared: a verdict of the other
manifest's at an index this one also has would be read as this one's. Give
every attempt its own directory, as `plan` does for every step.

## Re-run what failed

Write a new manifest containing only the failing shards, **with the same
`cell_timestamp_micros`**, and run it in a new directory. Rewriting a cell
that already landed at the same timestamp leaves Bigtable as it was, so
re-running a shard that partly landed is safe. Use a new directory for each
attempt so one attempt's verdicts never overwrite another's, then `verify`
the new manifest.

`verify` does not write the re-run manifest for you. Copy the failing shards
out of the old one, for example with `jq`, and check the result with
`freighter validate`. To re-plan the whole load instead, pass the old
timestamp to `freighter plan --cell-timestamp-micros`. See
[Operating a load](operating.md#re-running-failed-shards).

## What this does not check

The counts are freighter's own record of what Bigtable acknowledged. They do
not read Bigtable back. If you want that too, read a sample of keys from each
shard and compare the cell value with the source `json`. Counting the Bigtable
table instead is a full scan of it, which competes with live reads.
