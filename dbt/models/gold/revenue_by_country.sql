{{ config(materialized='view') }}
SELECT d.order_date,c.country,cur.currency,count() AS orders,sum(f.amount) AS revenue
FROM {{ ref('fact_order') }} f
INNER JOIN {{ ref('dim_country') }} c ON f.country_sk=c.country_sk
INNER JOIN {{ ref('dim_date') }} d ON f.date_sk=d.date_sk
INNER JOIN {{ ref('dim_currency') }} cur ON f.currency_sk=cur.currency_sk
GROUP BY d.order_date,c.country,cur.currency
