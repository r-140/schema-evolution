{{ config(materialized='incremental', unique_key='event_id', incremental_strategy='legacy', order_by='(order_hk,event_id)') }}
WITH raw AS (
 SELECT *, row_number() OVER (PARTITION BY event_id ORDER BY ingest_at DESC) AS duplicate_rank
 FROM {{ ref('raw_events') }} WHERE {{ event_filter('orders') }}
), parsed AS (
 SELECT *, if(op='d',JSONExtractRaw(payload,'before'),JSONExtractRaw(payload,'after')) AS body FROM raw WHERE duplicate_rank=1
), amounts AS (
 SELECT parsed.*, if(r.event_id != '', toNullable(r.corrected_amount_minor), coalesce(
   toInt64OrNull(JSONExtractRaw(body,'{{ var("amount_field","amount") }}')),
   toInt64OrNull(JSONExtractString(body,'{{ var("amount_field","amount") }}')),
   toInt64OrNull(JSONExtractRaw(body,'amount')),
   toInt64OrNull(JSONExtractString(body,'amount'))
 )) AS amount_minor
 FROM parsed
 LEFT JOIN (
   SELECT event_id,argMax(corrected_amount_minor,approved_at) AS corrected_amount_minor
   FROM {{ source("operations","event_repairs") }} GROUP BY event_id
 ) r ON parsed.event_id=r.event_id
)
SELECT event_id, JSONExtractInt(body,'id') AS order_id,
 lower(hex(SHA256(concat('order:',toString(order_id))))) AS order_hk,
 JSONExtractInt(body,'customer_id') AS customer_id,
 lower(hex(SHA256(concat('customer:',toString(customer_id))))) AS customer_hk,
 amount_minor,
 amount_minor / {{ var('amount_divisor',100) }} AS amount,
 JSONExtractString(body,'currency') AS currency,
 JSONExtractString(body,'status') AS status,
 {% if var('country_enabled',false) %}
 nullIf(JSONExtractString(body,'country'),'') AS country,
 {% else %}
 CAST(NULL AS Nullable(String)) AS country,
 {% endif %}
 {% if var('delivery_channel_enabled',false) %}
 nullIf(JSONExtractString(body,'delivery_channel'),'') AS delivery_channel,
 {% else %}
 CAST(NULL AS Nullable(String)) AS delivery_channel,
 {% endif %}
 toDate(parseDateTime64BestEffort(JSONExtractString(body,'ordered_at'),6,'UTC')) AS order_date,
 CAST(op='d' AS UInt8) AS is_deleted,source_lsn,kafka_offset,ingest_at AS load_ts, topic AS record_source,
 lower(hex(SHA256(toJSONString(tuple(amount_minor,currency,status,country,delivery_channel,order_date,is_deleted))))) AS hashdiff,
 {{ var('mapping_version',1) }} AS mapping_version
FROM amounts
