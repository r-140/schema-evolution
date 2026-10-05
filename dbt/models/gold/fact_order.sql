{{ config(order_by='(date_sk,order_sk)') }}
WITH latest_orders AS (
 SELECT *,row_number() OVER (PARTITION BY order_hk ORDER BY source_lsn DESC,kafka_offset DESC) AS rn
 FROM {{ ref('sat_order_details') }}
), latest_relationship AS (
 SELECT *,row_number() OVER (PARTITION BY order_hk ORDER BY source_lsn DESC,kafka_offset DESC) AS rn
 FROM {{ ref('sat_order_customer_effectivity') }}
)
SELECT o.order_hk AS order_sk,o.order_id,l.customer_hk AS customer_sk,
toUInt32(toYYYYMMDD(o.order_date)) AS date_sk,
lower(hex(SHA256(coalesce(o.country,'UNKNOWN')))) AS country_sk,
lower(hex(SHA256(o.currency))) AS currency_sk,o.amount,o.status,o.mapping_version
FROM latest_orders o
INNER JOIN latest_relationship e ON e.order_hk=o.order_hk AND e.rn=1
INNER JOIN {{ ref('lnk_order_customer') }} l ON l.link_hk=e.link_hk
WHERE o.rn=1 AND NOT o.is_deleted AND NOT e.is_deleted
