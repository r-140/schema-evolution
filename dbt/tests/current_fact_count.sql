WITH latest AS (
 SELECT *,row_number() OVER (PARTITION BY order_hk ORDER BY source_lsn DESC,kafka_offset DESC) AS rn
 FROM {{ ref('stg_orders') }}
)
SELECT 1 AS failure
WHERE (SELECT count() FROM latest WHERE rn=1 AND NOT is_deleted) !=
      (SELECT count() FROM {{ ref('fact_order') }})
