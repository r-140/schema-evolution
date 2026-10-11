import logging
import time
from prometheus_client import Gauge, Counter
from .storage import ch, pg, get_setting,catalog
from .etag import schema_precondition

IMPACT=Gauge('schema_source_impact','Source drift and affected dbt models',
    ['source_schema','source_table','changed_fields','affected_models','held'])

ETAG_MISMATCH=Gauge('schema_etag_precondition_failed','Exact source-schema precondition failed in etag mode')
PENDING=Gauge('schema_changes_pending','Unresolved schema reviews',['severity'])
AGE=Gauge('schema_change_oldest_seconds','Age of oldest unresolved review')
BLOCKED=Gauge('schema_publication_blocked','Breaking contract prevents publication')
RAW=Gauge('cdc_bronze_events','Physical raw event rows, can contain replay duplicates')
BACKLOG=Gauge('cdc_projection_backlog_events','Raw rows newer than normal processing cursor')
GOLD_AGE=Gauge('gold_age_seconds','Seconds since last successful validated Gold build')
UNKNOWN=Gauge('gold_unknown_country_orders','Current facts whose country is UNKNOWN')
UNPROJECTED=Gauge('schema_unprojected_country_events','Raw events with country but staging still has older mapping')
READ=Gauge('backfill_last_read_bytes','Read bytes in last successful measured run',['layer','mode'])
SINK_AGE=Gauge('cdc_bronze_age_seconds','Seconds since latest ingestion; idle source also increases this metric')
FAILURES=Gauge('pipeline_failed_runs_total','Persistent count of failed dbt runs')
OBSERVER_OK=Gauge('schema_observer_up','Catalog/metrics observation succeeded')

def refresh():
    from .impact import current, cursors
    impact=current()
    IMPACT.clear()
    for scope in impact['sources'].values():
        IMPACT.labels(scope['source_schema'],scope['source_table'],
            ','.join(scope['changed_fields']),','.join(scope['affected_models']),
            str(scope['held']).lower()).set(bool(scope['reasons']))
    precondition=schema_precondition(get_setting('mode'),get_setting('etag_config',{}),get_setting('contract'),catalog())
    ETAG_MISMATCH.set(precondition['mismatch'])
    with pg() as c:
        rows=c.execute("SELECT kind,count(*) FROM control.changes WHERE resolved_at IS NULL GROUP BY kind").fetchall()
        counts=dict(rows)
        for k in ('additive','breaking','event_breaking','semantic'): PENDING.labels(k).set(counts.get(k,0))
        AGE.set(c.execute("SELECT coalesce(extract(epoch from now()-min(first_seen)),0) FROM control.changes WHERE resolved_at IS NULL").fetchone()[0])
        BLOCKED.set(precondition['hold'] or sum(counts.get(x,0) for x in ('breaking','event_breaking','semantic')) > 0)
        FAILURES.set(c.execute("SELECT count(*) FROM control.runs WHERE status='failed'").fetchone()[0])
    client=ch()
    RAW.set(client.query('SELECT count() FROM bronze.events').first_row[0])
    SINK_AGE.set(client.query("SELECT if(count()=0,0,dateDiff('second',max(ingest_at),now())) FROM bronze.events").first_row[0])
    BACKLOG.set(client.query("SELECT count() FROM bronze.events WHERE ingest_at > toDateTime64({cursor:String},6,'UTC')",parameters={'cursor':min(cursors().values())}).first_row[0])
    GOLD_AGE.set(time.time()-get_setting('last_gold_success',time.time()))
    tables=client.query("SELECT concat(database,'.',name) FROM system.tables WHERE database IN ('silver','gold')").result_rows
    names={r[0] for r in tables}
    if 'gold.fact_order' in names:
        UNKNOWN.set(client.query("SELECT count() FROM gold.fact_order WHERE country_sk=lower(hex(SHA256('UNKNOWN')))").first_row[0])
    if 'silver.stg_orders' in names:
        UNPROJECTED.set(client.query("SELECT count() FROM (SELECT DISTINCT event_id FROM bronze.events WHERE entity='orders' AND JSONExtractString(payload,'after','country')!='') r INNER JOIN silver.stg_orders s USING(event_id) WHERE s.mapping_version < {version:UInt64}",parameters={'version':get_setting('mapping')['version']}).first_row[0])
    for mode, read, downstream in client.query("SELECT mode,argMax(read_bytes,started_at),argMax(downstream_read_bytes,started_at) FROM ops.backfill_runs WHERE status='success' GROUP BY mode").result_rows:
        READ.labels('projection',mode).set(read); READ.labels('downstream',mode).set(downstream)
