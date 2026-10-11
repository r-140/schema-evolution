SELECT f.order_id FROM {{ ref('fact_order') }} f
LEFT JOIN {{ ref('dim_delivery_channel') }} c ON f.delivery_channel_sk=c.delivery_channel_sk
WHERE c.delivery_channel_sk=''
