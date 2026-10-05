{{ config(materialized='incremental', unique_key='event_id', incremental_strategy='legacy', order_by='(customer_hk,event_id)') }}
WITH raw AS (
 SELECT *, row_number() OVER (PARTITION BY event_id ORDER BY ingest_at DESC) AS duplicate_rank
 FROM {{ ref('raw_events') }} WHERE {{ event_filter('customers') }}
), parsed AS (
 SELECT *, if(op='d',JSONExtractRaw(payload,'before'),JSONExtractRaw(payload,'after')) AS body FROM raw WHERE duplicate_rank=1
)
SELECT event_id, JSONExtractInt(body,'id') AS customer_id,
 lower(hex(SHA256(concat('customer:',toString(customer_id))))) AS customer_hk,
 JSONExtractString(body,'name') AS name, JSONExtractString(body,'segment') AS segment,
 CAST(op='d' AS UInt8) AS is_deleted, source_lsn,kafka_offset,ingest_at AS load_ts,
 topic AS record_source,
 lower(hex(SHA256(toJSONString(tuple(name,segment,is_deleted))))) AS hashdiff,
 {{ var('mapping_version',1) }} AS mapping_version
FROM parsed
