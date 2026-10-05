{{ config(order_by='customer_hk') }}
SELECT customer_hk, customer_id AS business_key, min(load_ts) AS load_ts,
argMin(record_source,tuple(source_lsn,kafka_offset)) AS record_source
FROM {{ ref('stg_customers') }} GROUP BY customer_hk,customer_id
