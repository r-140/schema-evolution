# Grafana: dashboard and alert interpretation

This guide covers **CDC schema evolution and backfill cost**, dashboard UID `schema-evolution`. Open the [Grafana dashboard](http://localhost:3000/d/schema-evolution), using `admin / admin`. Exercises are in the [step-by-step demo guide](demo-guide.md). Grafana shows platform health; validate business reports with SQL against Gold.

## Where the values come from

The [observer](../app/observer.py) compares the PostgreSQL catalog with the approved contract. The [lab monitoring loop](../app/service.py) runs the observer and [metrics.refresh](../app/metrics.py) approximately every five seconds. Prometheus scrapes `/metrics`, and Grafana queries Prometheus. Panels update after observation, scraping and dashboard refresh rather than synchronously with CLI commands. Separately, the sink checks a CDC event schema when it encounters a new fingerprint.

Configuration: [dashboard JSON](../infra/grafana/dashboards/schema-lab.json), [datasource](../infra/grafana/provisioning/datasources/datasources.yml), [alert rules](../infra/grafana/provisioning/alerting/rules.yml), and [contact/grouping policy](../infra/grafana/provisioning/alerting/contact.yml). Restart Grafana after changing provisioning: `docker compose restart grafana`. Normal source/data changes require no restart.

## Every panel

| Grafana title | Metric / unit | Interpretation |
|---|---|---|
| Pending schema reviews | `schema_changes_pending{severity}` / records | Unresolved `control.changes` records: additive, breaking, event_breaking and semantic. Counts observations/fingerprints rather than unique tables. One DDL change can produce both catalog and event reviews. |
| Publication blocked | `schema_publication_blocked` / 0 or 1 | 1 means at least one branch is held. It does not mean all dbt models, CDC or all Gold models have stopped. Inspect `/state → impact` for the actual scope. |
| Oldest unresolved change | `schema_change_oldest_seconds` / seconds | Age of the oldest unresolved review, not event delivery latency. Resolved reviews no longer contribute after approval. |
| Bronze event rows | `cdc_bronze_events` / physical rows | Includes INSERT/UPDATE/DELETE, snapshots and redelivery. Does not equal the order count: customers, tombstones and possible duplicates also contribute. |
| Projection backlog | `cdc_projection_backlog_events` / rows | Raw events newer than the minimum orders/customers cursor. Usually approaches zero after a successful incremental build on an idle source. During selective holds it is conservative and can include independent events already processed. |
| Gold freshness | `gold_age_seconds` / seconds | Time since the last fully successful, validated Gold build. Partial success does not reset the global timestamp. Independent models can update while this value rises. Before the first success, it does not prove Gold exists. |
| Unknown country facts | `gold_unknown_country_orders` / current orders | Facts with country=UNKNOWN. This can be legitimate when historical events never carried country. Approval alone does not reduce this value. |
| Country events awaiting replay | `schema_unprojected_country_events` / events | Raw events with nonempty country whose corresponding existing staging event has an older mapping_version. This is a **country-specific measure**, not universal field debt or an order count. Events never projected because of a hold may not yet appear here. |
| Measured backfill read bytes | `backfill_last_read_bytes{layer,mode}` / bytes | Last successful measured run for each mode, separating projection and downstream reads. Not cumulative, not money, and includes incremental runs. Validation and overhead are recorded in the run JSON. Partial_success does not update this panel. |
| Observer health | `schema_observer_up` / 0 or 1 | 1 means the latest observation/metric collection cycle succeeded. At 0, inspect lab logs. This does not check readiness of every service. |
| Last Bronze arrival age (idle is normal) | `cdc_bronze_age_seconds` / seconds | Time since the latest arrival. Also rises for a healthy idle source; interpret it alongside expected writes, `wait` and Connect status. |
| Failed builds | `pipeline_failed_runs_total` / runs | Persistent count of runs with status=failed. Does not return to zero after a fix. Despite the `_total` name, this is a Gauge reading the database. Expected holds with partial_success do not increment it. |
| ETag schema mismatch | `schema_etag_precondition_failed` / 0 or 1 | Exact source/approved-contract mismatch in etag mode with hold/warn. It is zero in hybrid and with policy=off even if breaking reviews exist. Zero after approval does not prove backfill is complete. |

UNKNOWN/replay-debt panels can show initial values before their tables exist; verify a baseline build first. `No data` is different from zero: check the datasource, `/metrics`, Prometheus target and observer logs.

## Finding affected tables, fields and held models

The dashboard contains aggregates. Scoped details are available in alert labels and `/state`; the current dashboard JSON has no additional model-list panel.

```bash
./bin/dev state
curl -s http://127.0.0.1:8090/metrics | rg 'schema_source_impact|schema_publication_blocked|schema_etag_precondition_failed'
curl -s http://127.0.0.1:8090/state | python3 -c 'import json,sys; s=json.load(sys.stdin); print(json.dumps({k:s[k] for k in ("impact","entity_cursors")},indent=2))'
```

In Grafana Explore, run `schema_source_impact > 0` as an instant query. Labels include `source_schema`, `source_table`, `changed_fields`, `affected_models` and `held`. Under warn, affected_models means potentially affected models; `held=false` means they continue running. In classic hybrid mode, compatible additive reviews appear in Pending reviews; the scoped impact gauge currently activates for exact mismatch or breaking/semantic reasons.

## Alerts and timing

| Alert | Condition | Meaning and next action |
|---|---|---|
| Schema change: affected models held | `schema_source_impact{held="true"}>0`, no pending duration | Read schema/table, fields and models. Bronze continues in hybrid/etag. Fix the reader/contract without clearing raw history. |
| Schema version precondition mismatch | `schema_source_impact>0`, no pending duration | Historically named for ETag; the rule now includes hybrid breaking/semantic impact too. held=false is notification without a hold. held=true can fire alongside the hold alert. |
| Schema review pending too long | Oldest review >120 s, pending for 30 s | A review needs a decision; this includes additive changes. |
| Country data needs historical replay | Country replay debt >0, pending for 30 s | Approval alone is insufficient. Check captured country values and replay. |
| Schema observer unavailable | Observer up <1, pending for 30 s | Check dependencies and lab logs. If lab itself disappears, the metric can be absent; configured `noDataState: OK` is not a substitute for a separate service-up alert. |
| Gold stale while projection has backlog | Gold age >600 s and backlog >0, pending for 30 s | Possible held branch, missing scheduled build or SQL/test failure. Inspect state/runs before triggering full replay. |

Rules are evaluated every ten seconds. Observation/scrape latency, `for`, group_wait=5 s and group_interval=30 s add delay. Alerts can arrive after the CLI has already reported a change, and resolved notifications are not instantaneous.

Open **Alerting → Alert rules** in Grafana and inspect labels/annotations. The local webhook writes notifications to `artifacts/alerts.jsonl`; email/Slack delivery is not configured:

```bash
tail -n 5 artifacts/alerts.jsonl
./bin/dev logs lab
```

## Common combinations

| Observation | Likely explanation | Check |
|---|---|---|
| Bronze grows, blocked=1, partial_success | A branch is held at Bronze→Silver; independent processing continues | impact and separate entity_cursors |
| Source writes continue, wait times out, mode=strict | Shared raw consumer batch is held; Connect can remain RUNNING | sink.py, lab logs, cdc-status |
| ETag=0, UNKNOWN remains high | Contract approved, but replay is pending or country never existed in raw | raw JSONHas/JSONExtractString and staging mapping_version |
| ETag=1, blocked=0, build succeeds | Compatible drift under warn | policy and active mapping |
| Observer=1, failed builds rises, Gold unchanged | Data/SQL failure with a structurally valid schema | run log and invalid-events |
| Arrival age rises without new source writes | Normal idle time | Generate a small batch and wait |
| Targeted projection is cheaper, total savings are small | Downstream Vault/Gold rebuild dominates | comparison.json and downstream/validation/overhead reads |

## Comparing cost fairly

Use `./bin/dev compare` with country=True and delivery_channel=False. It freezes one cutoff, copies staging, runs full replay, restores the same staging baseline and runs targeted replay. `same_business_result=true` verifies business-fact equality; mapping_version provenance is excluded from the checksum.

Inspect `artifacts/comparison.json`: projection `read_rows/read_bytes`, downstream, validation and overhead separately. Query-log bytes measure read work, not newly written data or latency. No fixed speedup is promised: results depend on data volume, ClickHouse/adapter behavior and shared downstream work. This comparison does not cover delivery_channel replay; use full replay for that field.
