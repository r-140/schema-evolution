{{ config(order_by='delivery_channel_sk') }}
SELECT DISTINCT lower(hex(SHA256(coalesce(delivery_channel,'UNKNOWN')))) AS delivery_channel_sk,
coalesce(delivery_channel,'UNKNOWN') AS delivery_channel
FROM {{ ref('sat_order_details') }}
UNION DISTINCT SELECT lower(hex(SHA256('UNKNOWN'))),'UNKNOWN'
