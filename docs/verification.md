# Verification record

Authoring checks:

- Python dependency resolution and installation for requirements succeeded.
- 12 contract tests and 1 measurement attribution test passed (addition, rename, type/nullability, hashing, approved historical reader aliases and Connect timestamptz mapping).
- dbt 1.9.4 + dbt-clickhouse 1.9.1 project parsing succeeded.
- Embedded ClickHouse/chdb test executes model SQL and full/targeted replay equivalence with synthetic Debezium-format events.
- Python compile/import and YAML/JSON syntax checks are performed before packaging.

Docker is not installed in the authoring environment. Therefore Compose startup, image availability for the user's CPU architecture, actual Debezium/Kafka replication, adapter table swapping, Airflow startup and Grafana-managed alert delivery were not executed here. These are covered by `bin/dev smoke` and the optional `workflow_dispatch` Docker CI job.

The embedded SQL test manually materializes model SELECT statements. It is meaningful SQL/data validation but is not dbt adapter integration testing.

On integration failure, inspect `docker compose logs`, connector status and `artifacts/*.log`. Report the failing component and preserve raw events/control state so recovery can be investigated. Do not remove volumes as the first troubleshooting step.

ETag/configurable-precondition update:

- 10 additional pure ETag/precondition tests passed (strong token, 428/412, semantics/version, compatible addition, hold/warn/off and writer schema normalization).
- 6 HTTP API tests with mocked control storage passed (200/304/400/412/428, stale second reviewer, runtime configuration).
- Total: 23 standard-library unit tests plus 6 HTTP tests. PostgreSQL row-lock concurrency itself remains a Docker integration check.
- `bin/etag-demo.py` supplies an end-to-end fresh-stack scenario; it has not been executed here because Docker is unavailable.
