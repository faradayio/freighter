# Operating a load

One load, start to finish, into a Bigtable table that is serving live reads at
the same time. The commands are covered in the [quickstart](quickstart.md);
this page is about running them safely and recovering when something goes
wrong.

## One plan per load

A load is everything written under one cell timestamp. `freighter plan` picks
that timestamp once, from the clock, and writes it into every manifest it
produces, so one `plan` invocation is one load. Do not run `plan` twice for
the same load without `--cell-timestamp-micros`: the second plan gets a new
timestamp, and every cell it writes becomes a second version instead of a
rewrite of the first.

`plan` refuses an `--out-dir` that already holds a manifest. Give every plan,
and every re-run, its own directory.

## Ramp up, and check between steps

A full-rate load starts by hitting the table with every lane at once. For a
table that serves live reads, ramp instead:

```bash
freighter plan --source source.json --lanes 8 --memory 24Gi \
  --bt-project my-project --bt-instance my-instance \
  --destination-table my_table --app-profile bulk-write \
  --ramp 0.05,0.10,0.85 --out-dir load
```

This writes three manifests, `load/step-0/`, `load/step-1/` and
`load/step-2/`, running at one lane, half the lanes and all of them
([Sizing a load](sizing.md#ramp)). Run them one at a time, and between steps:

1. **Verify the step.** `freighter verify` on the step's manifest must exit 0
   before the next step starts. A step with problems will not get better at a
   higher rate.
2. **Watch the serving side.** Read latency and error rates for the
   application reading the table, and Bigtable CPU. The mover cannot see
   these; it only knows its own write latency (`latency_ms_p50` and
   `latency_ms_p99` in each verdict).
3. **Then start the next step.** If serving suffered, stop here. The shards
   already written are complete and consistent; the remaining steps can run
   later, or with fewer lanes from a new plan with the same timestamp.

Copy each step's directory to wherever the mover runs (for example
`gs://my-bucket/loads/2026-10-14/step-0/manifest.json`), keeping the steps in
separate directories, so their verdicts stay apart.

## Re-running failed shards

`verify` lists every shard without a clean verdict. Re-run only those, with
the **same cell timestamp**, in a **new directory**:

```bash
mkdir rerun-1
jq --slurpfile report report.json '
  ([$report[0].problems[].shard] | unique) as $bad
  | .shards |= [.[] | select(.index | IN($bad[])) | .lane = 0]
  | .inflight_mutations = .flush_cells
' load/step-2/manifest.json > rerun-1/manifest.json
freighter validate file://$PWD/rerun-1/manifest.json
```

That keeps the failing shards, puts them all in one lane (lanes must be dense
from 0, so the original lane numbers would be refused), and sizes the
in-flight budget for one lane. Everything else, the timestamp included, is
copied from the original manifest. Then run it with `freighter move` and
`freighter verify` it like any other manifest.

Re-running a shard is safe even if part of it landed: a cell rewritten with
the same value at the same timestamp leaves Bigtable as it was. A new
directory per attempt matters because verdicts land beside the manifest:
re-using a directory overwrites the earlier attempt's verdicts, and `verify`
cannot tell one attempt's verdict from another's when their indexes agree.

To re-plan the whole load instead, for example with fewer lanes, run `plan`
again with the original timestamp:

```bash
jq .cell_timestamp_micros load/step-0/manifest.json
freighter plan ... --cell-timestamp-micros <that value> --out-dir load-2
```

## Stopping a load

The mover has no stop file or stop command. To stop it, stop the process:
Ctrl-C, `kill`, or delete the Kubernetes Job. Shards that had finished keep
their verdicts. Shards in progress get no verdict, and `verify` reports them
as `no verdict`, so a later re-run picks them up. Whatever they had written
is rewritten identically by that re-run.

`prepare` is different. Its long step is a BigQuery `CREATE TABLE AS SELECT`,
and stopping `prepare` does not cancel that job: it keeps running in BigQuery
and may still create the table. Cancel it in the BigQuery console or with
`bq cancel` if you need to, and drop the table before running `prepare` again.

## Running in Kubernetes

[examples/k8s/job.yaml](../examples/k8s/job.yaml) runs one manifest as a
Job, with `backoffLimit: 0` and `restartPolicy: Never`. A retry belongs to
whoever reads the verdicts: re-run only the shards without a clean verdict,
in a new directory. A Kubernetes retry would instead re-run every shard in the
same directory and overwrite the first attempt's verdicts.

Give the pod the memory you passed to `plan --memory`, with requests equal to
limits and whole CPUs.

## Reading verdicts

`verify` is the success check; the verdicts say why a shard looks the way it
does. The fields are in the [Verdict reference](verdicts.md). The ones worth
reading during a load:

- **`outcome`** and **`cells_failed`**: a `failed` shard, and how many cells
  Bigtable refused. Re-run it.
- **`rpc_retries`** and **`entry_retries`**: Bigtable pushing back. A steady
  rise from step to step says the table is near what it can take at this
  rate.
- **`latency_ms_p99`**: write latency as the mover saw it.
- **`read_reopens`** and **`stalled_seconds`**: BigQuery read streams that
  went silent and were re-opened. These cost time, not correctness.
- **`wall_seconds`**: with the shard count, what the rest of the load will
  take.

When a lane stops after three consecutive failed shards, the mover exits 1
and names the lane. The other lanes finish, and the stopped lane's remaining
shards have no verdict. Find out why the shards failed before re-running them.
