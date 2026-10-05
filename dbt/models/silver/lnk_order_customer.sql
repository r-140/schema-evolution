{{ config(order_by='link_hk') }}
SELECT lower(hex(SHA256(concat(order_hk,':',customer_hk)))) AS link_hk,
order_hk,customer_hk,min(load_ts) AS load_ts,
argMin(record_source,tuple(source_lsn,kafka_offset)) AS record_source
FROM {{ ref('stg_orders') }} GROUP BY link_hk,order_hk,customer_hk
