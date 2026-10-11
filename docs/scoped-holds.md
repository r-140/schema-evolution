# Scoped schema holds

Publication holds are table-scoped, then propagate through dbt dependencies. Bronze continues in hybrid/etag modes. The separate strict sink demonstration still holds the shared Kafka consumer batch.

`orders` drift with ETag `hold` holds `stg_orders`, its Vault models, order dimensions, `fact_order` and revenue views. `stg_customers`, `hub_customer`, `sat_customer_details` and `dim_customer` continue. Customer drift holds the customer branch and shared facts/reports, while order staging, order Vault and independent order dimensions continue. `fact_order` declares its semantic dependency on `dim_customer` explicitly.

The runner selects explicit allowed model names without `+` expansion, and uses cautious indirect test selection: tests touching a held model do not run. A build with useful independent work returns HTTP 200 and `status: partial_success`; only when both projections are held does `/build` return 409. Replay/comparison requires an unblocked graph. Runtime SQL/test failures still fail the run; this feature isolates schema holds, not arbitrary execution failures.

Dependency discovery scans the literal `ref()` calls in this lab's model SQL, including `depends_on` comments. Source-to-projection bindings are explicit because both projections read the same unified Bronze model. If extending this lab to dynamic Jinja refs or multiple projects, use the compiled dbt manifest and a deployment-owned source binding registry. Isolation is at table/model level, not column lineage. Approval remains one conditional update of the complete contract.

## Watermarks and recovery

`control.settings.entity_cursors` stores independent ingestion cutoffs for orders and customers. Existing installations bootstrap both from the old `cursor`. Only successfully projected and validated branches advance their cutoff. The legacy cursor becomes the minimum of both; its backlog metric is conservative and can include already processed independent events. A held branch resumes from its unchanged cutoff after approval, so another branch cannot skip its accumulated events. Previously projected historical fields still require explicit replay. Global Gold freshness is updated only on an unblocked successful build.

`GET /state` contains `impact.sources`, `held_models`, `runnable_models` and `entity_cursors`; run results contain the same plan. Grafana uses `schema_source_impact` labels for source schema/table, changed fields, affected models and held=true/false. Warn alerts identify impact but do not hold compatible changes. Breaking reader/semantic changes still hold their branch even with exact policy warn/off. Alerts name potential affected models even under warn.

The label lists are deliberately bounded to this small lab. Production should use stable table/model identifiers and link an impact API from alert annotations instead of placing changing field lists in Prometheus labels.

## Upgrade an existing stack

Overlay the project files while preserving volumes and artifacts. Then:

```bash
docker compose up -d --build lab
./bin/dev ready
# Grafana provisioning is read on startup:
docker compose restart grafana
./bin/dev build
curl -s http://127.0.0.1:8090/state | python3 -m json.tool
```

No source/Kafka/ClickHouse restart or volume reset is required. Source changes can be made with the existing migration/source-sql commands. For a fresh-stack example, `python3 bin/etag-demo.py` now asserts partial success and independent customer publication during an order hold.
