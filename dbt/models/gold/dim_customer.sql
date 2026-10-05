{{ config(order_by='customer_sk') }}
WITH ranked AS (
 SELECT *,row_number() OVER (PARTITION BY customer_hk ORDER BY source_lsn DESC,kafka_offset DESC) AS rn
 FROM {{ ref('sat_customer_details') }}
)
SELECT customer_hk AS customer_sk,customer_id,name,segment,is_deleted FROM ranked WHERE rn=1
