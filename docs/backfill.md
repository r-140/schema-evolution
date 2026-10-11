> Publication holds are now scoped to affected source projections and dependent dbt models. Independent branches continue, with separate watermarks. See [scoped holds](scoped-holds.md).

# Backfill cost and correctness

## Three different repairs

1. **Captured but unprojected field:** replay stored Bronze events with an approved reader. No source query required.
2. **Field absent from old events:** fetch/enrich from source or historical source backups. Current source values are not necessarily historical values.
3. **Changed business interpretation:** use per-version units/meaning/effective-time rules. Generic casts/full refresh may corrupt history.

This lab implements full and targeted repair for case 1 (`country`). Rename/type adaptation uses full projection replay. It exposes case 2 through explicit source enrichment and case 3 through a semantic hold.

## What full versus targeted does

Full: `stg_orders --full-refresh`, projecting all retained order events through a fixed cutoff.

Targeted: only already-staged events with an older mapping version and a nonempty raw `after.country`, optionally bounded by ingestion dates. A replacement is matched by event_id, not appended as a second version of the same transport event. Normal cursor is not advanced by replay.

A deletion-only country found in `before` is not a targeted candidate: deleted orders are absent from the current Gold report. Extending targeted replay to historical/as-of reports requires handling those historical before values too.

Both modes then rebuild hubs/links/satellites and Gold. This is intentionally a **partial optimization**: reducing projection work can leave downstream work almost unchanged.

The default dbt-clickhouse `legacy` incremental strategy rewrites the staging target while replacing selected event keys. A small candidate batch can therefore still read/write much of Silver. This is an important cost to expose, not a claim of O(changed-events) cost. Further experiments can use `delete_insert`, partition replacement, versioned ReplacingMergeTree staging or append-only mapping generations, with corresponding atomicity/dedup tradeoffs.

## Measurements

Every dbt query contains `schema_lab_run=<UUID>`. After the run, ClickHouse `system.query_log` is flushed and QueryFinish entries are aggregated:

- `read_rows`, `read_bytes`: tagged dbt model queries for `stg_*` projections (including staging replacement work).
- `downstream_read_rows`, `downstream_read_bytes`: other dbt model queries.
- `validation_read_rows`, `validation_read_bytes`: dbt test queries.
- `overhead_read_rows`, `overhead_read_bytes`: connection/metadata queries without a model/test node.
- `elapsed_seconds`: complete run, including tests and rebuilds.
- `query_count`: tagged finished queries.
- `projected_events_at_current_version`: staged event rows currently using the approved reader.

Attribution uses dbt query-comment tags for `node_name` and `node_type`, so a downstream model referencing staging is still attributed to its own model. Staging work includes reading/replacing existing staging, not only reading Bronze. Tests and connection overhead are separate. Inspect individual queries to distinguish storage layers precisely.

`projected_events_at_current_version` is a supplemental current-version staging count, not the number of events read by that one run. Read rows/bytes from the query log are the workload measurements.

Query-log read bytes mean ClickHouse's reported logical bytes read, not disk bytes, compressed storage cost or money. elapsed_seconds is sensitive to warm cache and the sequential full-then-targeted order. Do not claim a universal speedup from one run.

## Reproducible comparison

`compare` snapshots existing order staging, freezes cutoff to the completed normal cursor, performs full replay, restores baseline staging, then performs targeted replay. Both perform the same downstream rebuild/tests. It compares checksums over business facts, excluding mapping_version. Other control settings and accepted reader are identical.

The comparison assumes an additive country rollout and unchanged units/other mappings. Other replay types need their own candidate selection and equivalence contract.

Scale experiments:

```bash
./bin/dev demo --count 10000
# Fresh stack for each additional size:
./bin/dev demo --count 100000
./bin/dev demo --count 1000000
```

Generator uses one transaction per request. Large runs create bursty CDC and stress WAL/Kafka/storage, which is itself educational. Numbers of orders are actual source writes, not fabricated cost counters. `wait` can time out during a large run; call it again after inspecting progress.

## Further optimization exercises

- Partition raw by event/ingestion time and bound replay by affected partitions.
- Materialize schema_id → affected event/partition ranges, reducing repeated JSON predicates.
- Recompute only affected customer/order keys and Gold date/country groups.
- Handle old→new country movement: remove the old group contribution before adding the new one.
- Track deletion and late corrections; current-state recomputation and additive aggregation are different problems.
- Build a new model generation and publish it atomically after checks.
- Benchmark history retention limits: recovery is possible only while necessary events/schema definitions exist.

## Watermark/ordering limits

Normal processing uses a global **ingestion-time** cursor captured before a run; it does not use business event time or `max(order_id)`. Late source events arrive with a new ingestion time and remain eligible. Source LSN/Kafka offset determines current business state after projection. Replay uses an independent path and does not move the normal cursor.

Clock skew or a crashed raw write retried after the cutoff can complicate a timestamp watermark in production. Use durable monotonic batch identifiers or per-partition committed offset ranges for stronger checkpoint semantics. This lab is one sink process with a common clock. A shared ingestion lock covers raw-batch timestamp assignment/insert and cutoff capture, preventing an in-flight older-timestamp batch from landing after the cutoff snapshot.
