{{ config(order_by='(order_hk,source_lsn,kafka_offset)') }}
SELECT event_id,order_hk,lower(hex(SHA256(concat(order_hk,':',customer_hk)))) AS link_hk,
is_deleted,load_ts,source_lsn,kafka_offset,record_source
FROM {{ ref('stg_orders') }}
