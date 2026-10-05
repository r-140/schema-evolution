{{ config(order_by='order_hk') }}
SELECT order_hk, order_id AS business_key, min(load_ts) AS load_ts,
argMin(record_source,tuple(source_lsn,kafka_offset)) AS record_source
FROM {{ ref('stg_orders') }} GROUP BY order_hk,order_id
