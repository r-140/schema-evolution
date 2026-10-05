CREATE DATABASE IF NOT EXISTS bronze;
CREATE DATABASE IF NOT EXISTS silver;
CREATE DATABASE IF NOT EXISTS gold;
CREATE DATABASE IF NOT EXISTS ops;
CREATE TABLE IF NOT EXISTS bronze.events (
 event_id String, topic String, partition_id UInt32, kafka_offset UInt64,
 entity LowCardinality(String), op LowCardinality(String), schema_id String,
 source_lsn UInt64, source_ts_ms UInt64, ingest_at DateTime64(6, 'UTC'),
 key_json String, envelope String, payload String
) ENGINE=MergeTree PARTITION BY toYYYYMM(ingest_at)
ORDER BY (entity, ingest_at, event_id);
CREATE TABLE IF NOT EXISTS ops.backfill_runs (
 run_id String, mode String, started_at DateTime64(6, 'UTC'), elapsed_seconds Float64,
 projected_events_at_current_version UInt64, read_rows UInt64, read_bytes UInt64,
 downstream_read_rows UInt64, downstream_read_bytes UInt64, query_count UInt64,
 validation_read_rows UInt64,validation_read_bytes UInt64,
 overhead_read_rows UInt64,overhead_read_bytes UInt64,status String
) ENGINE=MergeTree ORDER BY (started_at, run_id);
CREATE TABLE IF NOT EXISTS ops.event_repairs (
 event_id String, corrected_amount_minor Int64, reason String,
 approved_at DateTime64(6, 'UTC')
) ENGINE=MergeTree ORDER BY (event_id, approved_at);
