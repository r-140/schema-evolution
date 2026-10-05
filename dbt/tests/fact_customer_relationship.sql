SELECT f.order_id FROM {{ ref('fact_order') }} f
LEFT JOIN {{ ref('dim_customer') }} d ON f.customer_sk=d.customer_sk
WHERE d.customer_sk = ''
