{{ config(order_by='country_sk') }}
SELECT DISTINCT lower(hex(SHA256(coalesce(country,'UNKNOWN')))) AS country_sk,
coalesce(country,'UNKNOWN') AS country FROM {{ ref('sat_order_details') }}
UNION DISTINCT SELECT lower(hex(SHA256('UNKNOWN'))),'UNKNOWN'
