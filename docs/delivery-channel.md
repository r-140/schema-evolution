# Add captured delivery_channel to Silver and Gold

The optional field already exists in PostgreSQL and raw Bronze. Agreeing its source schema resolved the drift alert, but did not change its analytic projection. This update adds:

- An explicit `delivery_channel` reader flag, default false for existing mappings.
- Nullable projected channel in `stg_orders`, included in the descriptive hashdiff.
- `sat_order_details` carries it through its existing event-history construction.
- `dim_delivery_channel`, including UNKNOWN, and `fact_order.delivery_channel_sk`.
- `revenue_by_delivery_channel` and dimension uniqueness/referential tests.

## Apply to an existing stack

Overlay the updated project code into the current project directory. Preserve Docker volumes and `artifacts/`. Pause scheduled Airflow builds during the migration. Rebuild only the lab container, leaving source, Kafka and ClickHouse running:

```bash
docker compose up -d --build lab
./bin/dev ready
python3 bin/enable-delivery-channel.py
./bin/dev build full
./bin/dev sql "SELECT * FROM gold.revenue_by_delivery_channel ORDER BY delivery_channel,currency"
```

The approval helper fetches the current ETag, preserves country/amount settings, enables the new reader flag and submits conditional approval. It increments mapping.version. It deliberately does not retry a 412 blindly; fetch and review a concurrently changed resource before running it again. Review pending source changes before using this helper: approval accepts the current source catalog as a batch, consistent with the educational API.

The first build must be **full**, because existing staging physically lacks the new column and its old projected events lack channel. Full replay recreates order staging with the new schema, reinterprets raw events and rebuilds dependent Silver/Gold models. It does not reread the PostgreSQL source.

The 100 explicit source UPDATE events carrying `delivery_channel=online` become current online facts. Old events without channel remain UNKNOWN; no historical values are invented. Silver retains pre-update and post-update states when the hashdiff changes. Load/source ordering here is not a claim of business-effective SCD2 validity.

After migration, ordinary `./bin/dev build` uses the new flag for future events. No source/Kafka/ClickHouse restart or volume deletion is needed. Models are rebuilt individually: use a maintenance window for report readers; this small lab does not publish the entire star atomically.

## Backfill limitations

`build targeted` / `compare` originally select **country** candidates. They must not be used as a generic channel repair. The country comparison is now explicitly refused while channel mapping is enabled, to avoid reporting a false equivalence for different repair coverage. Channel rollout currently uses full replay. A future targeted channel repair needs a channel-specific candidate selector, replaced event IDs and downstream reconciliation.

## Included runtime fixes

- Staging dbt tests use `--indirect-selection cautious`, so Gold-dependent tests run after Gold exists.
- CLI readiness retries temporary connection reset/timeouts.
- ETag demo normalizes HTTP response-header keys, avoiding case-sensitive dictionary lookup.

## Validation

SQL/data test exercises channel projection, current online facts and dimension relationships on embedded ClickHouse. HTTP tests cover explicit approval and rejection when the source lacks the field. Docker integration remains unexecuted in the authoring environment.
