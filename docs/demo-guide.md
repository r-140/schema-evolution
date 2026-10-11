# Schema evolution demos: step-by-step reference

The goal is to demonstrate **detect → decide → adapt → replay → measure**, and distinguish missing analytical visibility, loss of stored data, a schema hold and a data-quality failure. Panel interpretation: [Grafana guide](grafana-guide.md). Architecture: [scoped holds](scoped-holds.md), [ETag](etag-architecture.md), and [hybrid design tradeoffs](hybrid-design.md).

## 0. Returning to an existing project

Do not rerun `demo` blindly: migrations are not idempotent and the generator adds orders. Exact counts below apply only to the stated clean baseline.

```bash
./bin/dev ready
./bin/dev cdc-status
./bin/dev state
./bin/dev gold-summary
./bin/dev source-sql "SELECT table_name,column_name,data_type,is_nullable FROM information_schema.columns WHERE table_schema='sales' ORDER BY table_name,ordinal_position"
./bin/dev sql "SELECT run_id,mode,status,started_at FROM ops.backfill_runs ORDER BY started_at DESC LIMIT 5"
```

Inspect mode, etag_config, contract, mapping (country/channel flags and amount field/type/divisor), unresolved changes, impact and entity_cursors. `gold-summary` can fail before the first build; that is expected. `ready` checks API/storage health, `cdc-status` checks Connect, and `wait` compares current source keys/selected attributes with the latest raw events. None of these alone proves Gold is fresh.

Use hybrid mode for classic scenarios 1–10; ETag configuration is inactive in that mode. If the source already has country/channel or a renamed amount column, select the relevant continuation instead of repeating ALTER ADD/RENAME.

### Preparing a new baseline

```bash
./bin/dev up
./bin/dev ready
./bin/dev register-cdc
./bin/dev mode hybrid
./bin/dev generate 1000
./bin/dev wait
./bin/dev build
./bin/dev gold-summary
```

1. Compose starts the services; `register-cdc` creates the sales.customers/orders connector.
2. The [generator](../app/generator.py) creates 100 customers and 1,000 orders using the current source schema.
3. The [sink](../app/sink.py) preserves complete CDC envelopes in Bronze before committing Kafka offsets.
4. The [runner](../app/pipeline.py) checks the schema, freezes a cutoff, and runs staging, tests, Vault/Gold and eligible-model tests.
5. Expect status=success, Gold orders=1000 and unknown_country_orders=1000. Bronze contains more than 1,000 physical rows because customers/snapshots/events also contribute.

Grafana: Observer health=1, Pending=0, Publication blocked=0 and ETag=0. Bronze grows, projection backlog approaches zero after the build, and Gold age resets.

### Isolating independent exercises

Rename, type change, expand/contract, PK and nullability need separate starting baselines. Running every section sequentially against one volume changes the schema and expected results. `./bin/dev down` preserves volumes. Export required artifacts, source/control and ClickHouse data if you need a recoverable checkpoint.

**Only when intentionally deleting this lab's data to create a new baseline**:

```bash
# Deletes this Compose project's volumes, including history and control settings.
docker compose down --volumes
./bin/dev up
./bin/dev ready
./bin/dev register-cdc
./bin/dev mode hybrid
./bin/dev generate 1000
./bin/dev wait
./bin/dev build
```

This is not a recovery step for an existing experiment. A second Compose project cannot run concurrently on the same host ports without port overrides.

### Intermediate checks for any scenario

```bash
./bin/dev state
./bin/dev sql "SELECT entity,op,count() FROM bronze.events GROUP BY entity,op ORDER BY entity,op"
./bin/dev sql "SELECT mapping_version,count() FROM silver.stg_orders GROUP BY mapping_version ORDER BY mapping_version"
./bin/dev gold-summary
# Replace LOG_NAME with build.log or state.runs[].details.log:
tail -n 100 artifacts/LOG_NAME.log
```

`partial_success` is an intentional hold of some models, HTTP 200. `failed` indicates a SQL/test failure, typically HTTP 500. Holding both source projections produces HTTP 409. Airflow runs one HTTP task: partial_success is green; held dbt nodes do not appear as individual skipped Airflow tasks.

## 1. Optional country: stored in Bronze, invisible in Gold

**Start:** section 0's clean baseline, hybrid, country=False and delivery_channel=False. Code: [migration](../app/migrations.py), [observer](../app/observer.py), [projection](../dbt/models/silver/stg_orders.sql), [country dimension](../dbt/models/gold/dim_country.sql).

### Step 1 — add the field and generate events

```bash
./bin/dev migrate add-country
./bin/dev generate 100
./bin/dev wait
./bin/dev state
./bin/dev sql "SELECT count() FROM bronze.events WHERE entity='orders' AND JSONExtractString(payload,'after','country')!=''"
```

ALTER adds nullable TEXT. The generator now populates country on new orders. The observer registers an additive review; reader-compatible events remain in Bronze. The raw query should find at least 100 events carrying country. Grafana Pending additive rises; blocked=0 and ETag=0.

### Step 2 — build using the old mapping

```bash
./bin/dev build
./bin/dev gold-summary
```

Expect success, orders=1100 and unknown_country_orders=1100. The mapping does not read country, so visibility is lost while raw data remains available. UNKNOWN rises and backlog falls after cursor advancement. The pending-review alert waits for its age threshold.

### Step 3 — approve the projection

```bash
./bin/dev approve --country
./bin/dev state
./bin/dev gold-summary
```

Approval increments mapping.version and enables country. It is not a rebuild: Gold still contains 1,100 UNKNOWN countries. Reviews resolve. After metric refresh, Country events awaiting replay reports existing staging events with captured country and an older mapping version.

### Step 4 — compare recovery strategies

```bash
./bin/dev compare
python3 -m json.tool artifacts/comparison.json
./bin/dev gold-summary
```

[compare_backfills](../app/pipeline.py) runs full and targeted replay from the same staging baseline/cutoff. The [event filter](../dbt/macros/event_filter.sql) restricts targeted replay to captured country and older mapping versions. Expect same_business_result=true, orders=1100 and UNKNOWN=1000. Country cannot be recovered for the original 1,000 events that never contained it. Grafana replay debt falls; read-byte series separate full/targeted projection and downstream work. No fixed savings ratio is promised.

Automation alternative **on a fresh stack**: `./bin/dev demo --count 10000` creates 10,000+1,000 orders and expects 10,000 UNKNOWN after replay. Do not combine it with this manual exercise on the same baseline.

## 2. Rename: hold orders, continue customers

**Start:** a separate clean hybrid baseline. Code: [DDL](../app/migrations.py), [impact plan](../app/impact.py), [runner](../app/pipeline.py).

```bash
./bin/dev migrate rename-amount
./bin/dev generate 100
./bin/dev wait
./bin/dev state
./bin/dev build
./bin/dev gold-summary
```

1. amount becomes total_amount; the generator switches names.
2. Bronze preserves new events and wait succeeds.
3. Catalog/event validation creates breaking reviews. Impact holds stg_orders and dependent Vault/Gold models; the customer branch remains eligible.
4. Build returns partial_success. Gold orders stays at 1,000. The orders cursor remains unchanged while the customers cursor advances.

Grafana: blocked=1, breaking/event_breaking reviews and growing backlog. Scoped alerts identify sales.orders, changed fields and models. ETag stays zero because this is hybrid mode. Global Gold age continues rising during partial success.

### Recovery

```bash
./bin/dev approve --amount-field total_amount
./bin/dev build full
./bin/dev gold-summary
```

The reader supports total_amount with fallback to historical amount. Full replay reads the bounded history across both generations. Expect success and orders=1100; hold/reviews clear and Gold updates. Full replay does not advance normal incremental cursors: a subsequent `./bin/dev build` catches them up, using event_id for projection idempotence.

## 3. Strict: demonstrate a coupled raw-consumer outage

**Start:** a separate clean baseline. Code: [sink strict check](../app/sink.py), [global blocked predicate](../app/observer.py).

```bash
./bin/dev mode strict
./bin/dev migrate rename-amount
./bin/dev generate 100
./bin/dev state
./bin/dev wait
```

The schema/event check holds the raw batch and Kafka commits. Expect wait to time out: source changes have not reached Bronze. Connect can remain RUNNING because Debezium capture is separate from successful sink writes. This demonstrates the shared strict consumer's coupling, not selective sink isolation.

Grafana: blocked=1; Bronze stops catching up and arrival age can rise. Projection backlog counts only stored raw rows and does not measure the entire Kafka backlog. Inspect source/raw synchronization and logs.

```bash
./bin/dev mode hybrid
./bin/dev wait
./bin/dev build
./bin/dev approve --amount-field total_amount
./bin/dev build full
./bin/dev gold-summary
```

Switching to hybrid releases raw ingestion without approving the contract. The pre-approval build is partial_success. After approval/full, expect orders=1100 and normal Gold publication. Kafka retention and WAL storage are finite: a strict hold is not safe indefinitely.

## 4. Type change, invalid history and audited repair

**Start:** a separate baseline with amount BIGINT. Code: [approval](../app/service.py), [staging parser/repair](../dbt/models/silver/stg_orders.sql), [valid_amount](../dbt/tests/valid_amount.sql).

```bash
./bin/dev migrate amount-text
./bin/dev generate 100
./bin/dev wait
./bin/dev state
./bin/dev build
./bin/dev approve --amount-type text
./bin/dev build full
```

BIGINT→TEXT initially holds orders, producing partial_success. Approval enables an int64/string reader. Full rebuild handles both event-schema generations. Expect success and Gold orders=1100. Hold/reviews rise and then resolve.

### A data failure with an already approved schema

```bash
./bin/dev source-sql "UPDATE sales.orders SET amount='broken',updated_at=now() WHERE id=1"
./bin/dev wait
./bin/dev build
./bin/dev invalid-events
```

The TEXT schema is unchanged, so the observer does not hold it. Parsing broken yields NULL and valid_amount returns failing rows. Expect failed before downstream rebuild; Gold retains the old value. Failed builds rises and Gold age does not reset. This is a test failure, not a schema mismatch.

```bash
./bin/dev source-sql "UPDATE sales.orders SET amount='12500',updated_at=now() WHERE id=1"
./bin/dev wait
./bin/dev build
```

Expect another failure: a valid new source state does not delete the earlier invalid staging event.

```bash
./bin/dev invalid-events
# Copy the failing event_id, not schema_id or order_id:
./bin/dev repair-amount 'lab.sales.orders:0:REPLACE_OFFSET' --amount-minor 12500 --reason 'Source owner confirmed amount for this historical event'
./bin/dev sql "SELECT * FROM ops.event_repairs ORDER BY approved_at DESC LIMIT 5"
./bin/dev build full
./bin/dev gold-summary
```

Repair appends a reasoned override without rewriting raw data. Full replay applies the correction; valid_amount passes and Gold updates. Failed builds remains a historical count. Verify the test log and order 1's value. The repair endpoint supports amount only, not arbitrary event cleanup.

## 5. Expand–migrate–contract

**Start:** a separate baseline. Code: [migrations](../app/migrations.py), [dual-write generator](../app/generator.py), [reader fallback](../dbt/models/silver/stg_orders.sql).

```bash
./bin/dev migrate expand-amount
./bin/dev wait
./bin/dev state
./bin/dev approve --amount-field total_amount
./bin/dev build full
```

Expand adds total_amount, copies amount values and makes the column NOT NULL. UPDATEs generate CDC history. In hybrid the addition is compatible and needs review without stopping the whole pipeline. The reader switches to total_amount while retaining amount fallback. After full replay, expect Gold orders=1000.

```bash
./bin/dev generate 100
./bin/dev wait
./bin/dev build
./bin/dev source-sql "SELECT id,amount,total_amount FROM sales.orders ORDER BY id DESC LIMIT 5"
```

The generator sees both fields and dual-writes them. Expect matching values and Gold orders=1100. Grafana shows normal processing without a hold; backlog falls after the build.

```bash
./bin/dev migrate contract-amount
./bin/dev state
./bin/dev approve --amount-field total_amount
./bin/dev generate 100
./bin/dev wait
./bin/dev build
./bin/dev gold-summary
```

Dropping the old field creates a review/hold against the full approved catalog, even though the reader has switched. Approval confirms the contract phase. Expect orders=1200 after build. Real applications must retire old writers before DROP; the lab's schema-aware generator handles its own transition automatically.

## 6. Same structure, different units

**Start:** a baseline. Code: [explicit semantic marker](../app/service.py), [semantic hold](../app/impact.py).

```bash
./bin/dev semantic-change
./bin/dev state
./bin/dev build
```

This command does not change DDL or actual values. It creates a metadata review for minor→major units. Structural fingerprints cannot infer this meaning. Expect orders held, customer partial_success, a semantic review and scoped alert. ETag stays zero in hybrid.

**There is no correct automatic approval/replay solution in this exercise.** Changing one global amount_divisor and replaying mixed-unit history corrupts old interpretation. A version/effective boundary with per-version conversion is required and intentionally not implemented here. Stop after diagnosis and use a separate baseline for another exercise. Clearing an alert through forced approval does not prove business correctness.

## 7. Backfill cannot recover a value that never existed

**Start:** completed scenario 1: 1,100 orders, UNKNOWN=1000, country=True.

```bash
./bin/dev sql "SELECT count() FROM bronze.events WHERE entity='orders' AND op IN ('c','r') AND NOT JSONHas(payload,'after','country')"
./bin/dev enrich-country 1000
./bin/dev wait
./bin/dev build
./bin/dev gold-summary
```

[populate_country](../app/migrations.py) explicitly updates up to 1,000 source rows with country=NULL using synthetic PL/DE/FR values. New CDC events now carry data absent from historical envelopes. The approved incremental reader processes them. Expect UNKNOWN=0 and orders=1100 on this baseline.

Grafana Bronze grows and UNKNOWN falls after build. This enriches current state; it does not prove the country at the time of the original sale. Replaying old events without enrichment would not produce these values.

## 8. Update/delete, Vault history and current Gold

**Start:** any successful baseline with orders 1 and 2 present and no holds. Code: [mutator](../app/generator.py), [satellite](../dbt/models/silver/sat_order_details.sql), [fact](../dbt/models/gold/fact_order.sql).

```bash
./bin/dev mutate 1
./bin/dev wait
./bin/dev build
./bin/dev sql "SELECT order_id,status FROM gold.fact_order WHERE order_id=1"
```

UPDATE changes status to paid. Raw preserves the event, Vault records the changed state and Gold selects the current version. Expect paid without a change in order count.

```bash
./bin/dev delete 2
./bin/dev wait
./bin/dev build
./bin/dev sql "SELECT count() FROM gold.fact_order WHERE order_id=2"
./bin/dev sql "SELECT order_id,status,is_deleted,source_lsn,kafka_offset FROM silver.sat_order_details WHERE order_id IN (1,2) ORDER BY order_id,source_lsn,kafka_offset"
./bin/dev gold-summary
```

DELETE remains in raw and Silver history while current facts exclude order 2. Expect count=0 for order 2 and one fewer total order. A Kafka tombstone is not another business fact. Bronze rows can increase while Gold orders decreases; that is normal.

## 9. Primary key and nullability: schema holds before tests

**Start:** independent baselines for 9a and 9b. Code: [structural comparison](../app/compatibility.py), [approval validation](../app/service.py), [existing tests](../dbt/models/schema.yml).

### 9a — required amount becomes nullable

```bash
./bin/dev migrate nullable-amount
./bin/dev state
./bin/dev build
./bin/dev approve
```

DROP NOT NULL changes the contract without replacing existing values with NULL. The observer creates a breaking review. Build holds orders and returns partial_success. Approval is rejected because the reader requires non-null amount. Grafana blocked=1 and scoped alerts fire; Failed builds need not increase.

If state is queried immediately after DDL, wait for the observer poll or invoke build, which scans the catalog again. **Tests in the held branch do not run.** DDL alone without NULL data does not necessarily fail not_null. Here valid_amount checks NULL/negative amounts; generic staging not_null is configured for event_id.

Restore the original structural constraint:

```bash
./bin/dev source-sql "ALTER TABLE sales.orders ALTER COLUMN amount SET NOT NULL"
./bin/dev approve
./bin/dev build
```

SET NOT NULL requires no NULL rows. Approval explicitly resolves the existing review; reverting the schema does not automatically close control.changes history. Expect success after build.

### 9b — composite source primary key

```bash
./bin/dev migrate change-pk
./bin/dev state
./bin/dev build
./bin/dev approve
```

PK becomes (id,customer_id); the catalog detects changed key membership. The consumer/Vault still use id as business identity. Expect the order branch held and approval rejected for unsupported PK drift. **Existing unique tests need not fail:** values can remain unique until repeated ids appear, and event_id uniqueness checks event identity rather than the source business key.

Supporting composite identity requires changes to the generator, CDC keys, staging hashes, Vault links, dimensions/facts and tests. This lab does not implement them. Do not bypass the hold to get a green pipeline; continue on a separate baseline.

## 10. Real Schema Registry: a separate Avro exercise

**Start:** Kafka running; the main JSON CDC stream remains unchanged. Code: [script](../bin/registry-demo.py), [compatibility explanation](compatibility.md).

```bash
docker compose --profile registry up -d schema-registry
python3 bin/registry-demo.py
```

The script creates random subjects, registers baseline Avro and calls compatibility APIs for optional addition, amount type change and required rename under BACKWARD/FORWARD/FULL_TRANSITIVE. Expect optional_add compatible in all modes, type_change incompatible, and required rename incompatible at least backward. The script finishes with PASS.

Grafana should not show a new CDC mismatch just because these Registry exercises ran. Main Debezium uses embedded JSON schemas rather than these subjects. Inspect the script JSON instead of waiting for a dashboard review. Reruns use new subjects.

## 11. ETag hold, conditional approval, 428/412 and recovery

**Start:** a clean baseline. Code: [ETag](../app/etag.py), [API](../app/service.py), [script](../bin/etag-demo.py), [architecture](etag-architecture.md).

```bash
./bin/dev mode etag
./bin/dev etag-config --policy hold --require-if-match true
curl -i http://127.0.0.1:8090/contract
OLD_ETAG=$(curl -sD - -o /dev/null http://127.0.0.1:8090/contract | tr -d '\r' | awk 'tolower($1)=="etag:" {print $2}')
printf '%s\n' "$OLD_ETAG"
curl -i http://127.0.0.1:8090/contract -H "If-None-Match: $OLD_ETAG"
```

Save the quoted strong token for approved contract+mapping. Header names are case-insensitive. GET with the current If-None-Match returns 304 without a body. This is API cache validation, not an HTTP call per CDC event.

```bash
./bin/dev migrate add-country
./bin/dev generate 100
./bin/dev wait
./bin/dev build
./bin/dev state
```

The addition is reader-compatible but changes exact schema identity. Expect partial_success: orders held, customers running. Gold orders=1000; the new 100 orders are in Bronze. Grafana ETag=1 and blocked=1; scoped alerts identify country and affected models.

```bash
curl -i -X PUT http://127.0.0.1:8090/contract -H 'Content-Type: application/json' -d '{"country":true}'
curl -i -X PUT http://127.0.0.1:8090/contract -H 'Content-Type: application/json' -H "If-Match: $OLD_ETAG" -d '{"country":true}'
curl -i -X PUT http://127.0.0.1:8090/contract -H 'Content-Type: application/json' -H "If-Match: $OLD_ETAG" -d '{"country":true}'
```

**On this baseline**, the body uses appropriate defaults. Missing header→428; saved token→200 with a new ETag; repeat with stale token→412. A 412 on the first attempted approval means the contract changed: fetch, review and retry deliberately rather than bypassing CAS.

```bash
./bin/dev build
./bin/dev gold-summary
```

Expect success, orders=1100 and UNKNOWN=1000. The held 100 events never passed through the old reader, so the preserved order cursor now processes them with the new mapping. No backfill is needed for those events. ETag/blocked become zero after refresh. Previously projected historical fields can still require replay.

Automation **on a separate fresh stack**, instead of the manual steps: `python3 bin/etag-demo.py`. It generates its own baseline, verifies 304/428/412 and partial_success, then tests warn/off and leaves the stack in hybrid with an additional delivery_channel review.

## 12. Warn: compatible drift uses the old reader

**Start:** a separate baseline with country=False; do not repeat add-country on scenario 11's source.

```bash
./bin/dev mode etag
./bin/dev etag-config --policy warn --require-if-match true
./bin/dev migrate add-country
./bin/dev generate 100
./bin/dev wait
./bin/dev build
./bin/dev gold-summary
```

Exact mismatch is reported without holding compatible projection. Expect success, orders=1100 and UNKNOWN=1100. Grafana ETag=1, blocked=0 and scoped alerts with held=false. The order cursor has advanced: approval alone cannot repair these events.

```bash
OLD_ETAG=$(curl -sD - -o /dev/null http://127.0.0.1:8090/contract | tr -d '\r' | awk 'tolower($1)=="etag:" {print $2}')
curl -i -X PUT http://127.0.0.1:8090/contract -H 'Content-Type: application/json' -H "If-Match: $OLD_ETAG" -d '{"country":true}'
./bin/dev build
./bin/dev gold-summary
./bin/dev compare
./bin/dev gold-summary
```

After approval ETag=0, but an ordinary build does not rewrite the already processed 100 events: UNKNOWN remains 1100. Replay restores captured country, reducing UNKNOWN to 1000. Country debt clears; full/targeted cost appears in Grafana. **Warn does not permit a breaking reader change:** rename/type/semantic reviews still hold the affected branch.

### Off and returning to hybrid

```bash
./bin/dev etag-config --policy off --require-if-match false
./bin/dev mode hybrid
./bin/dev state
```

Off disables exact equality gating; require_if_match controls legacy `/approve` in etag mode. Conditional PUT `/contract` always requires If-Match. Hybrid ignores exact equality but retains breaking/event/semantic holds.

## 13. Delivery channel: source field to Gold report

**Start:** a successful stack without source delivery_channel; skip migration if it exists. No breaking holds. Code: [mapping/projection](../dbt/models/silver/stg_orders.sql), [satellite](../dbt/models/silver/sat_order_details.sql), [dimension](../dbt/models/gold/dim_delivery_channel.sql), [report](../dbt/models/gold/revenue_by_delivery_channel.sql), [approval helper](../bin/enable-delivery-channel.py).

```bash
./bin/dev mode etag
./bin/dev etag-config --policy warn --require-if-match true
./bin/dev migrate add-channel
# Choose existing order ids; this baseline uses 1..100:
./bin/dev source-sql "UPDATE sales.orders SET delivery_channel='online',updated_at=now() WHERE id BETWEEN 1 AND 100"
./bin/dev wait
./bin/dev build
./bin/dev sql "SELECT * FROM gold.revenue_by_delivery_channel ORDER BY delivery_channel,currency"
```

The generator automatically fills country, **but not delivery_channel**, so explicit source UPDATE is necessary. Raw stores online while the old mapping projects UNKNOWN channel. Grafana ETag=1, blocked=0 and held=false alerts. Country replay debt does not measure channel debt.

```bash
curl -s http://127.0.0.1:8090/contract | python3 -m json.tool
# After reviewing the complete observed source schema:
python3 bin/enable-delivery-channel.py
./bin/dev build
./bin/dev sql "SELECT * FROM gold.revenue_by_delivery_channel ORDER BY delivery_channel,currency"
./bin/dev build full
./bin/dev sql "SELECT * FROM gold.revenue_by_delivery_channel ORDER BY delivery_channel,currency"
```

The helper preserves current country/amount settings, enables channel and uses conditional approval. Ordinary incremental processing does not rewrite existing events. Full replay reads online from raw: on the 1,000-order baseline expect 100 online and 900 UNKNOWN, totaling 1,000. Revenue is grouped by currency; do not add unlike currencies.

ETag clears after approval, but only the Gold SQL report confirms channel publication after full replay. Compare/targeted are country-specific; compare rejects delivery_channel=True. If upgrading from a version without channel models, deploy current dbt code and rebuild lab while preserving volumes first.

## 14. Customer isolation and a shared downstream dependency

**Start:** a separate clean baseline. Code: [source roots/dependency closure](../app/impact.py), [semantic fact dependency](../dbt/models/gold/fact_order.sql), [entity cursors](../dbt/macros/event_filter.sql).

```bash
./bin/dev source-sql "ALTER TABLE sales.customers ADD COLUMN loyalty_level TEXT"
./bin/dev mode etag
./bin/dev etag-config --policy hold --require-if-match true
./bin/dev generate 100
./bin/dev wait
./bin/dev build
./bin/dev state
```

Exact customer mismatch holds customer staging/Vault/dimension. fact_order explicitly depends on dim_customer, so shared facts/reports are also held. Order staging, order Vault and independent order dimensions continue. Expect partial_success; the orders cursor advances and the customers cursor stays unchanged. Grafana blocked=1 with sales.customers/loyalty_level labels; global Gold freshness does not reset.

```bash
# This exercise accepts the catalog addition without adding an analytical field.
# Review the mapping and preserve its existing settings:
python3 - <<'PY'
import json
from urllib.request import Request,urlopen
base='http://127.0.0.1:8090'
with urlopen(base+'/contract') as r:
    resource=json.load(r); token=r.headers['ETag']
body={k:resource['mapping'].get(k,False) for k in ('country','delivery_channel')}
body.update({k:resource['mapping'][k] for k in ('amount_field','amount_type','amount_divisor')})
print('Approving unchanged analytical mapping:',body)
req=Request(base+'/contract',method='PUT',data=json.dumps(body).encode(),headers={'Content-Type':'application/json','If-Match':token})
with urlopen(req) as r: print(r.read().decode())
PY
./bin/dev build
./bin/dev gold-summary
```

Approval accepts the additive catalog without promising loyalty_level in Gold. The next successful build catches up customer history from its preserved cursor and rebuilds shared models. Expect orders=1100. A new analytical field requires model changes and replay.

## 15. Airflow and automated checks

**Start:** a suitable successful stack. Code: [DAG](../airflow/dags/schema_evolution.py), [runner](../app/pipeline.py).

```bash
docker compose exec airflow airflow dags list
docker compose exec airflow airflow dags trigger medallion_incremental
./bin/dev state
```

The Airflow task calls `/build`; the lab runner invokes dbt, and ClickHouse executes SQL. Wait for task completion in Airflow or state.runs: triggering is not evidence of success. A selective hold yields a green task with partial_success in its result/XCom. SQL/test failure yields a failed task. Detailed logs are in artifacts; affected models are in state.

```bash
# Requires country=True, channel=False and an unheld graph:
docker compose exec airflow airflow dags trigger schema_backfill --conf '{"mode":"targeted"}'
# Bounds use ingest_at UTC rather than business ordered_at. Use actual dates:
docker compose exec airflow airflow dags trigger schema_backfill --conf '{"mode":"targeted","from_date":"2026-10-01 00:00:00.000000","to_date":"2026-10-02 00:00:00.000000"}'
```

Targeted replay repairs only eligible country events in the ingestion window. An empty window can succeed without changing data. Replay requires an unheld graph. The scheduled DAG starts paused; enable it in the UI to use scheduling. Pause scheduled builds and source writes during compare.

Automation options, each on its own fresh stack:

```bash
./bin/dev demo --count 10000
# Or on a separate starting baseline:
./bin/dev smoke
# Or on a separate starting baseline:
python3 bin/etag-demo.py
```

Smoke covers country replay equality, update/delete, strict→hybrid, selective rename holds and historical replay. Local checks without a full Docker stack: [impact unit tests](../tests/test_impact.py), [runner watermark regression](../tests/check_scoped_pipeline.py), [SQL fixtures](../tests/check_clickhouse_sql.py), [ETag HTTP tests](../tests/check_etag_api.py). Coverage and limitations: [verification](verification.md).

## Troubleshooting without restarting the whole stack

| Symptom | Next check |
|---|---|
| Connection reset immediately after rebuild | Run ready before retrying; inspect lab logs |
| wait timeout | Connect status, source/raw synchronization, strict mode and connect/lab logs |
| partial_success | state.impact and entity_cursors; expected selective hold |
| 409 during replay | Held graph, concurrent build or missing replay prerequisites |
| 428 approval | Missing If-Match; GET /contract |
| 412 approval | Stale token; fetch and review again without automatic approval |
| failed valid_amount | invalid-events, logs and explicit repair; a source UPDATE does not erase history |
| ETag=0 but new field invisible | Mapping/model deployment, old cursor, raw field presence and replay |
| No data in Grafana | /metrics, observer health, Prometheus target and datasource |
| Backlog remains after full replay | Full does not advance incremental cursors; run ordinary build |

Preserve build JSON/logs, comparison.json and alerts.jsonl. They explain state transitions when returning later. Source migrations and approval mutate state; this documentation does not execute them automatically.
