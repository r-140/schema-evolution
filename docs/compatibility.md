# Compatibility techniques

| Technique | Implemented here | What it misses |
|---|---|---|
| Migration/PR schema diff | `bin/check-contract.py` | Unannounced out-of-band DDL, semantics |
| Source catalog polling | Names, types, nullability, primary-key membership | Transient changes between polls, business meaning |
| New event-schema fingerprint | Embedded Connect JSON, bounded decision cache | Fields connector never emits, unchanged-type meaning |
| Required-reader validation | Names/types/nullability required by mapping | Bad values inside otherwise compatible schemas |
| Data tests | Numeric amount, unique transport/business keys, fact/dimension/count invariants | Every possible business invariant |
| Semantic contract notification | Explicit units change marker | Undeclared meaning changes |
| Real Registry API experiment | Optional Avro backward/forward/full checks | dbt SQL compatibility and business meaning |

## Backward/forward direction

- Backward: a new reader can consume older writer schemas.
- Forward: an old reader can consume newer writer schemas.
- Full: both directions.
- Transitive: applies against all registered historical versions, rather than just the previous one.

The optional demo uses actual Confluent Registry REST compatibility checks. It does not approximate Avro compatibility with field-name comparisons. Required field addition without a default is different from adding a nullable field with a default. Avro's structural compatibility does not ensure an old dbt SQL projection handles a renamed business column.

The main JSON-reader checks are deliberately conservative. Type widening is held for review instead of automatically assumed safe: precision, units, range, ClickHouse target type and downstream arithmetic must be considered.

## PR check

Export the post-migration source schema in the same form as `contracts/baseline.json` (type, nullable and primary_key for each field), then:

```bash
python3 bin/check-contract.py --expected contracts/baseline.json --observed contracts/examples/additive.json
python3 bin/check-contract.py --expected contracts/baseline.json --observed contracts/examples/rename.json  # exits 1
```

A real source CI job should apply migration files to a disposable PostgreSQL, export its catalog and run this check. Do not infer the entire resulting database schema by parsing arbitrary SQL strings. This repository supplies the comparison gate and fixtures; it does not pretend to integrate with an existing source-service migration system.

## Mapping deployment

Approval is a deliberate consumer operation, separate from source DDL. It validates supported required fields and updates the current mapping. It also accepts historical `amount` when a newer reader prefers `total_amount`, and accepts historical bigint events when text amounts have been explicitly approved. Unsupported identity/nullability changes remain rejected.

Mapping is stored in control PostgreSQL; dbt variables are derived from it for a locked run. Production deployments should version mappings as immutable code/configuration with a per-schema reader registry, reviewer, effective boundary and rollback policy.

## Reference documentation

- PostgreSQL Debezium: https://debezium.io/documentation/reference/3.0/connectors/postgresql.html
- Official ClickHouse Kafka sink: https://clickhouse.com/docs/integrations/kafka/clickhouse-kafka-connect-sink
- dbt-clickhouse: https://github.com/ClickHouse/dbt-clickhouse
- Incremental adapter strategies: https://clickhouse.com/docs/integrations/dbt
- Schema Registry compatibility: https://docs.confluent.io/platform/current/schema-registry/fundamentals/schema-evolution.html
- Airflow Docker: https://airflow.apache.org/docs/apache-airflow/2.10.5/howto/docker-compose/index.html

Images/packages are pinned to a conservative runnable baseline rather than latest tags. The ClickHouse server uses the 25.3 release series; freeze a patch digest after local integration testing if strict reproducibility is required.
