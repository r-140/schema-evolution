> Publication holds are now scoped to affected source projections and dependent dbt models. Independent branches continue, with separate watermarks. See [scoped holds](docs/scoped-holds.md).

# CDC Schema Evolution Lab

Runnable educational lab focused on **detect → decide → adapt → replay → measure**, rather than only comparing schema-on-read and schema-on-write.

The implementation preserves business payloads using a **schema-on-read foundation in raw Bronze**, then checks approved contracts before affected dbt transformations. Independent branches continue when another branch is held. A schema-on-write hybrid is discussed as an alternative architecture, not an implemented ingestion path; see [the design comparison](docs/hybrid-design.md).

**PostgreSQL → Debezium → Kafka → raw ClickHouse Bronze → dbt Data Vault Silver → star-schema Gold.** Airflow orchestrates builds/replays; Prometheus and Grafana expose review debt, held publication, hidden fields and real query read costs.

## Quick start (Ubuntu)

Requirements: Docker Engine + Compose v2, Python 3.10+, approximately 12–16 GB available RAM, 15 GB free disk. Internet access is needed for images and Python packages. Ports bind to localhost. Credentials are intentionally local-demo credentials.

```bash
chmod +x bin/dev
./bin/dev up
./bin/dev ready
./bin/dev demo --count 10000
```

Run `demo` on a **fresh stack**: the migration is intentionally not silently reapplied. To repeat while preserving earlier results, use a different Compose project and port override, or explicitly remove only this lab's volumes after exporting `artifacts/`.

The scripted demo:

1. Registers Debezium and produces orders/customers.
2. Builds baseline Data Vault and star schema.
3. Adds optional `orders.country`, then produces more events.
4. Builds with the old mapping: the field is stored in Bronze but Gold still says UNKNOWN.
5. Approves country projection, without automatically rewriting history.
6. Compares full versus targeted replay from **the same staging baseline and frozen raw cutoff**.
7. Verifies that both yield the same business facts and saves measurements.

Old orders that never carried country remain UNKNOWN. This is deliberate: a replay cannot invent historical values.

## Return to the project

- [Step-by-step demos](docs/demo-guide.md): startup/state checks, 15 scenarios, commands, implementation links, expected outcomes and recovery.
- [Grafana guide](docs/grafana-guide.md): every panel, alerts, common metric combinations and interpretation limits.
- [Hybrid design tradeoffs](docs/hybrid-design.md): implemented raw-first schema-on-read foundation, pre-dbt contract gating, and a proposed schema-on-write hybrid with advantages and limitations.

## Open the lab

| Component | URL / credentials |
|---|---|
| Grafana | http://localhost:3000 — admin / admin |
| Lab API + OpenAPI | http://localhost:8090/docs |
| Current contract and pending changes | http://localhost:8090/state |
| Gold report | http://localhost:8090/gold |
| Prometheus | http://localhost:9090 |
| Airflow | http://localhost:8080 — user `admin`; generated password shown in `docker compose logs airflow` or `docker compose exec airflow cat /opt/airflow/standalone_admin_password.txt` |
| Debezium Connect | http://localhost:8083/connectors/evolution-source/status |
| PostgreSQL source | localhost:5433 — lab / lab / database lab |
| ClickHouse HTTP/native | localhost:8123 / localhost:9000 — lab / lab |

Grafana alerts are provisioned, with a local webhook receiver. Notifications appear in lab logs and `artifacts/alerts.jsonl`; no external team messages are sent. Replace the contact point with your team's destination if desired.

Airflow DAGs are initially paused so scheduled builds do not invalidate controlled experiments. Unpause `medallion_incremental` for continuous operation; `schema_backfill` is manually triggered. Pause scheduling during benchmark comparisons.

## What to explore

| Scenario | Technique / outcome |
|---|---|
| Added optional field | Catalog diff + event fingerprint; ingestion stays live; review remains pending |
| Rename required field | Publication held in hybrid mode; strict mode holds Kafka commits too |
| bigint → text | Explicit reader adaptation; malformed text fails the amount test; audited correction + replay restores processing |
| NOT NULL → nullable | Contract rejects relaxation of a required value |
| Primary-key change | Catalog contract rejects identity changes; not auto-approved |
| Expand–migrate–contract | Dual fields, deploy a reader that understands both, then remove the old field |
| Changed units, same type | Explicit semantic metadata alert; structural checks cannot infer meaning |
| Historical field captured but ignored | Targeted projection replay and downstream rebuild |
| Historical field never captured | Source enrichment / new CDC events, with no claim of historical reconstruction |
| Duplicate, update, delete | Dedup by topic/partition/offset; ordered satellite history; current Gold facts |
| Registry compatibility | Optional real Avro backward/forward/full compatibility API experiment |

Read [docs/demo-guide.md](docs/demo-guide.md) for step-by-step commands and expected results, [docs/architecture.md](docs/architecture.md) for contracts/ownership/model structures, and [docs/backfill.md](docs/backfill.md) for what the cost measurements mean.

## Configurable ETag / If-Match variant

```bash
./bin/dev mode etag
./bin/dev etag-config --policy hold --require-if-match true
# Or alert-only:
./bin/dev etag-config --policy warn --require-if-match true
# Complete fresh-stack demo:
python3 bin/etag-demo.py
```

Real HTTP `GET/PUT /contract` prevents stale approval with ETag/If-Match and atomic control-setting locks. Separately, exact writer-schema preconditions distinguish “compatible” from “reviewed version”. Bronze keeps ingesting. Policies `hold`, `warn`, `off` are configurable at runtime or through first-start environment defaults. See [docs/etag-preconditions.md](docs/etag-preconditions.md) for configuration, two-reviewer conflicts and interview discussion.

**Per-event behavior:** the sink computes a schema fingerprint and checks its local cache; it does not call a schema REST API for every event. Exact ETag-mode catalog matching runs before dbt builds. See the per-event/cache/REST section in [docs/etag-preconditions.md](docs/etag-preconditions.md).

ETag architecture and sequence diagrams: [docs/etag-architecture.md](docs/etag-architecture.md).

## Add delivery_channel to Gold

See [docs/delivery-channel.md](docs/delivery-channel.md) for upgrading the running lab, explicit reader approval and full raw replay. Includes `dim_delivery_channel`, a fact FK and a channel report.

## Verification

```bash
./bin/dev test                         # standard-library contract tests
python3 -m venv .venv
.venv/bin/pip install -r requirements.lock.txt
DBT_SEND_ANONYMOUS_USAGE_STATS=false .venv/bin/dbt parse --project-dir dbt --profiles-dir dbt
.venv/bin/pip install chdb==3.3.0 httpx==0.28.1
PYTHONPATH=. .venv/bin/python tests/check_etag_api.py
.venv/bin/python tests/check_clickhouse_sql.py
./bin/dev smoke                        # full stack test after ./bin/dev up, on a fresh stack
```

The embedded ClickHouse test exercises all SQL, duplicate handling, missing historical country, full/targeted equivalence, rename/text parsing, updates/deletes, Data Vault/star invariants and malformed amount rejection. It does not exercise Kafka Connect, Docker entrypoints, Airflow/Grafana provisioning or dbt's adapter materialization.

**Authoring environment:** dependency installation, contract tests, dbt parsing, application import and embedded ClickHouse SQL tests are checked. Docker is unavailable here, so full Compose integration is supplied as an executable smoke test and CI workflow, not reported as already passed. See [docs/verification.md](docs/verification.md).

## Implementation map

- `app/compatibility.py`: catalog/reader checks and stable schema fingerprint.
- `app/observer.py`: bounded in-memory compatibility cache, catalog polling, persisted review lifecycle.
- `app/sink.py`: batch raw preservation and commit-after-insert ordering.
- `app/service.py`: explicit approval, schema/source migrations, state, metrics and demo endpoints.
- `app/pipeline.py`: validation gates, separate normal watermark and replay path, comparison snapshot.
- `dbt/models/silver`: projections, hubs, immutable relationship links, descriptive/effectivity satellites.
- `dbt/models/gold`: order-grain fact and customer/date/country/currency dimensions.
- `airflow/dags`: independent scheduled transformation and manual bounded replay.
- `infra/grafana`: dashboards, Grafana-managed alerts and local contact point.
- `bin/check-contract.py`: source-schema PR check, independent of dbt deployment.

This lab uses a small custom **raw sink** to expose failure and preservation semantics. ClickHouse also has an official Kafka Connect sink; this is not a claim that the custom sink is that connector or that exactly-once delivery is implemented. The upstream source is standard Debezium PostgreSQL + Kafka.
