{{ config(order_by='(customer_hk,source_lsn,kafka_offset)') }}
WITH history AS (
 SELECT *,lagInFrame(hashdiff,1,'') OVER (
   PARTITION BY customer_hk ORDER BY source_lsn,kafka_offset
   ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
 ) AS previous_hashdiff
 FROM {{ ref('stg_customers') }}
)
SELECT * EXCEPT(previous_hashdiff) FROM history WHERE hashdiff != previous_hashdiff
