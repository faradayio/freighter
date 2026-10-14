# Verdict reference

After each shard the mover writes `shard_<index:04>.json` beside the manifest.
An example is at [freighter/fixtures/freighter-verdict-example.json](../freighter/fixtures/freighter-verdict-example.json).

The mover reports; it does not judge whether the load is complete. That is
[verification](verifying-a-load.md).

## Outcome and counts

| Field | Meaning |
|---|---|
| `manifest_version` | the manifest version the mover ran |
| `shard_index` | the shard's `index` |
| `outcome` | `complete` or `failed` |
| `rows_read` | rows read from BigQuery for the shard's bucket range |
| `cells_encoded` | cells built (rows with a non-NULL `json`) |
| `cells_acknowledged` | cells Bigtable confirmed, each counted once, on final success |
| `cells_failed` | cells that still failed after bounded retries |
| `row_deletes_acknowledged` | row deletes Bigtable confirmed |
| `row_deletes_failed` | row deletes that still failed after retries |

**The completeness gate** for a shard is all of:

- `outcome == "complete"`
- `cells_failed == 0` and `row_deletes_failed == 0`
- `cells_acknowledged` equals the source's non-NULL-`json` row count over the
  shard's buckets
- `row_deletes_acknowledged` equals the source's NULL-`json` row count (when
  `null_json_deletes_row`)

A shard that errors out (a read error, an unexpected empty shard, a write
error that ends the shard) gets a `failed` verdict with every count and timing at zero,
even if some of its cells had already landed. A shard that finishes writing
with `cells_failed > 0` gets a `failed` verdict with real counts.

## Request shape and retries

| Field | Meaning |
|---|---|
| `entries` | `MutateRows` entries sent on first attempt. Retries re-send a subset and are not counted here |
| `requests` | `MutateRows` RPCs issued, including retries |
| `entry_retries` | per-entry retries after a transient status. Never affects the acknowledged or failed counts |
| `rpc_retries` | retried unary calls, **read side and write side summed**: read-session creation and token minting, plus whole-batch `MutateRows` re-sends. The two can't be separated from the verdict |
| `read_reopens` | read streams re-opened after 90 s of silence or a transport error |

`entries / requests` is the measured request shape.

## Timings

| Field | Meaning |
|---|---|
| `read_seconds` | read session opened → last decoder finished |
| `sort_seconds` | sorting the shard by row key |
| `write_seconds` | first slice sent → last acknowledged |
| `stalled_seconds` | time spent waiting out silent streams before re-opening them |
| `wall_seconds` | read session opened → last acknowledged |
| `latency_ms_p50`, `latency_ms_p99` | latency per slice, including its retries |

A lane reads its next shard while writing the current one, so `wall_seconds`
includes time the read shard waited for the previous write. Rate a shard by
`write_seconds`, and a whole load by its overall wall time, not by summing
`wall_seconds`.
