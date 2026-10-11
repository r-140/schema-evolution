{{ config(materialized='view') }}
SELECT c.delivery_channel,cur.currency,count() AS orders,sum(f.amount) AS revenue
FROM {{ ref('fact_order') }} f
INNER JOIN {{ ref('dim_delivery_channel') }} c ON f.delivery_channel_sk=c.delivery_channel_sk
INNER JOIN {{ ref('dim_currency') }} cur ON f.currency_sk=cur.currency_sk
GROUP BY c.delivery_channel,cur.currency
