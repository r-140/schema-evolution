# Demo guide

Use a fresh stack for each independent breaking migration. Run `./bin/dev ready` then `./bin/dev register-cdc` once. `./bin/dev wait` waits until source keys/selected attributes match the latest raw events; it is more useful than a fixed sleep but is not a transactional snapshot across tables.

## 1. Optional field silently missing from Gold, then replay

```bash
./bin/dev generate 10000
./bin/dev wait
./bin/dev build
./bin/dev migrate add-country
./bin/dev generate 1000
./bin/dev wait
./bin/dev state
./bin/dev build
./bin/dev gold-summary
./bin/dev approve --country
./bin/dev compare
./bin/dev gold-summary
```

Before approval/replay: 11,000 current orders, 11,000 UNKNOWN countries. After replay: the 1,000 orders whose events carried country become visible by country; the original 10,000 remain UNKNOWN. Customer updates generated along the way are also captured.

Watch pending additive review, UNKNOWN country facts, replay debt and read bytes in Grafana. Pending review alert fires after approximately 2 minutes; evaluation/grouping adds a small delay. The controller detects schema change within approximately 5 seconds; new event schemas are checked immediately on ingestion.

`compare` executes full then targeted from the identical copied staging baseline and the normal processing cutoff. It verifies business-fact checksums, excludes mapping_version from equality, saves JSON/logs and leaves targeted results visible. Avoid source writes and scheduled builds during comparison. A failed comparison retains `ops.compare_stg_orders_<id>` for recovery.

## 2. Rename: hybrid holds publication, Bronze continues

```bash
./bin/dev generate 1000
./bin/dev wait
./bin/dev build
./bin/dev migrate rename-amount
./bin/dev generate 100
./bin/dev wait
./bin/dev state
./bin/dev build                       # expected 409, unresolved breaking change
./bin/dev approve --amount-field total_amount
./bin/dev build full
./bin/dev gold-summary
```

The new reader supports `total_amount` and historical `amount`. Both names are required during replay across the transition. Newly generated events continue accumulating in Bronze before approval.

## 3. Strict sink makes the same change an ingestion outage

```bash
./bin/dev generate 1000
./bin/dev wait
./bin/dev build
./bin/dev mode strict
./bin/dev migrate rename-amount
./bin/dev generate 100
./bin/dev state
./bin/dev wait                        # expected timeout: held sink, source ahead of Bronze
./bin/dev mode hybrid
./bin/dev wait                        # backlog reaches Bronze, publication still held
./bin/dev approve --amount-field total_amount
./bin/dev build full
```

Strict stops Kafka offset commits in the raw sink; Debezium still captures. Kafka retention/WAL limits still matter: “held safely forever” is not guaranteed.

## 4. Type change plus invalid data

```bash
./bin/dev generate 1000
./bin/dev wait
./bin/dev build
./bin/dev migrate amount-text
./bin/dev generate 100
./bin/dev wait
./bin/dev approve --amount-type text
./bin/dev build full
./bin/dev source-sql "UPDATE sales.orders SET amount='broken',updated_at=now() WHERE id=1"
./bin/dev wait
./bin/dev build                       # amount data test fails before downstream rebuild
./bin/dev source-sql "UPDATE sales.orders SET amount='12500',updated_at=now() WHERE id=1"
./bin/dev wait
./bin/dev build                       # historical bad event still fails
```

The last failure is intentional: projected history still contains an invalid old state, even if current source state is now good. Apply an explicit reviewed correction:

```bash
./bin/dev invalid-events
# Copy the event_id from the previous command:
./bin/dev repair-amount 'lab.sales.orders:0:REPLACE_OFFSET' --amount-minor 12500 --reason 'Source owner confirmed correct amount for this event'
./bin/dev build full
```

The correction is append-only in `ops.event_repairs`, with reason and approval time. It changes projection interpretation; the original raw envelope stays unchanged. A later source update is another event, not an automatic repair of the previous state. This supports amount corrections only, not arbitrary JSON rewriting. Production requires authenticated approval/audit and review of historical-report semantics.

## 5. Expand–migrate–contract

```bash
./bin/dev generate 1000
./bin/dev wait
./bin/dev build
./bin/dev migrate expand-amount
./bin/dev wait
./bin/dev approve --amount-field total_amount
./bin/dev build full
./bin/dev generate 100              # generator dual-writes both amount fields
./bin/dev wait
./bin/dev build
./bin/dev migrate contract-amount
./bin/dev approve --amount-field total_amount
./bin/dev generate 100
./bin/dev wait
./bin/dev build
```

The old writer must also be retired before dropping its field in a real application. Here the generator examines the source schema so it can demonstrate both generations.

## 6. Same type, different units

```bash
./bin/dev semantic-change
./bin/dev state
./bin/dev build                      # expected held publication
```

The semantic marker demonstrates an explicit migration notification; a catalog fingerprint cannot infer units. `--amount-divisor` is available for controlled mapping experiments, but changing one global divisor and full-replaying mixed historical units is wrong. A real minor→major transition needs schema/contract version or an effective boundary and per-version interpretation. Do not approve this scenario as if structural compatibility proved correctness.

## 7. Field did not exist in historical events

After scenario 1:

```bash
./bin/dev enrich-country 1000
./bin/dev wait
./bin/dev build
./bin/dev gold-summary
```

This is an explicit demo source enrichment, producing new UPDATE events with synthetic country values. It increases country coverage without claiming those were the actual country values at the original sale time. Source queries, extra WAL/CDC traffic and Silver/Gold updates all have costs.

## 8. Updates/deletes

```bash
./bin/dev mutate 1
./bin/dev wait
./bin/dev build
./bin/dev delete 2
./bin/dev wait
./bin/dev build
./bin/dev sql "SELECT order_id,status FROM gold.fact_order ORDER BY order_id LIMIT 10"
./bin/dev sql "SELECT order_id,status,is_deleted,source_lsn,kafka_offset FROM silver.sat_order_details WHERE order_id IN (1,2) ORDER BY order_id,source_lsn,kafka_offset"
```

Gold excludes deleted order 2, while Silver preserves the deletion/history. Kafka tombstones are retained in Bronze but not treated as another business fact.

## 9. Primary key and nullability

```bash
./bin/dev migrate nullable-amount
./bin/dev state
./bin/dev approve                   # rejected: required amount is nullable
```

On another fresh stack: `./bin/dev migrate change-pk`. The observer rejects business-identity drift. Approval does not implement migration to composite order identity; rebuild the design rather than treating that as a column rename.

## 10. Real Schema Registry compatibility checks (optional)

```bash
docker compose --profile registry up -d schema-registry
python3 bin/registry-demo.py
```

This isolated Avro exercise prints backward/forward/full compatibility for optional addition, type change and required rename. It does not change the main Debezium JSON format. Details: [compatibility.md](compatibility.md).

## Airflow commands

```bash
docker compose exec airflow airflow dags trigger medallion_incremental
docker compose exec airflow airflow dags trigger schema_backfill --conf '{"mode":"targeted"}'
docker compose exec airflow airflow dags trigger schema_backfill --conf '{"mode":"targeted","from_date":"2026-10-01 00:00:00.000000","to_date":"2026-10-02 00:00:00.000000"}'
```

## Troubleshooting

- `./bin/dev logs lab`, `./bin/dev logs connect`, `./bin/dev cdc-status`.
- Connector registration can fail while Connect is starting; retry `register-cdc` after checking its logs.
- Health API checks storage, not capture progress. `wait`/`cdc-status` check the data path.
- ClickHouse init SQL and PostgreSQL init SQL run only for fresh volumes.
- Existing lab volumes from another code version may need explicit migrations; image restart does not reset schemas.
- dbt run logs and measurement JSON: `artifacts/`.
- A held breaking change is expected behavior; switching to hybrid restores raw ingestion but does not approve publication.
