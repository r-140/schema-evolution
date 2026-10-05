{{ config(order_by='currency_sk') }}
SELECT DISTINCT lower(hex(SHA256(currency))) AS currency_sk,currency
FROM {{ ref('sat_order_details') }}
