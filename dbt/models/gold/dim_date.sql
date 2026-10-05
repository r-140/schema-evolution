{{ config(order_by='date_sk') }}
SELECT DISTINCT toUInt32(toYYYYMMDD(order_date)) AS date_sk,order_date,
toYear(order_date) AS year,toMonth(order_date) AS month,toDayOfMonth(order_date) AS day
FROM {{ ref('sat_order_details') }}
