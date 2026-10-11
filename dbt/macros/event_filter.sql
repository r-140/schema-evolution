{% macro event_filter(entity) %}
entity = '{{ entity }}' AND op IN ('c','u','r','d')
AND ingest_at <= toDateTime64('{{ var("cutoff", "2100-01-01 00:00:00.000000") }}',6,'UTC')
{% if is_incremental() %}
  {% if var('replay_mode','incremental') == 'targeted' and entity == 'orders' %}
    AND JSONHas(payload,'after','country')
    AND JSONExtractString(payload,'after','country') != ''
    AND event_id IN (
      SELECT event_id FROM {{ this }} WHERE mapping_version < {{ var('mapping_version',1) }}
    )
    {% if var('from_date','') %}
    AND ingest_at >= toDateTime64('{{ var('from_date') }}',6,'UTC')
    {% endif %}
    {% if var('to_date','') %}
    AND ingest_at < toDateTime64('{{ var('to_date') }}',6,'UTC')
    {% endif %}
  {% else %}
    AND ingest_at > toDateTime64('{{ var("cursor_" ~ entity, var("cursor", "1970-01-01 00:00:00.000000")) }}',6,'UTC')
  {% endif %}
{% endif %}
{% endmacro %}
