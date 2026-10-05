SELECT event_id FROM {{ ref('stg_orders') }} WHERE amount_minor IS NULL OR amount_minor < 0
