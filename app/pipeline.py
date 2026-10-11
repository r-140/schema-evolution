import json
import subprocess
import threading
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from psycopg.types.json import Jsonb
from .storage import ch, pg, get_setting, set_setting
from .observer import scan_catalog, blocked
from .benchmark import collect
from .synchronization import INGEST_LOCK
from .etag import schema_precondition
from .compatibility import fingerprint

LOCK = threading.Lock()

def timestamp(value):
    return value.strftime('%Y-%m-%d %H:%M:%S.%f')

def command(args, variables, log):
    result = subprocess.run(['dbt',*args,'--project-dir','/lab/dbt','--profiles-dir','/lab/dbt',
        '--vars',json.dumps(variables)],capture_output=True,text=True)
    with log.open('a') as f:
        f.write(result.stdout+'\n'+result.stderr)
    if result.returncode:
        raise RuntimeError(f'dbt failed; see {log.name}')

def execute(mode='incremental', from_date='', to_date='', cutoff=None):
    observed=scan_catalog()
    from .impact import current, cursors, ROOTS
    impact = current(observed)
    allowed = set(impact['runnable_models'])
    positions = cursors()
    if not any(root in allowed for root in ROOTS.values()):
        raise ValueError('All source projections held: '+json.dumps(impact))
    if mode != 'incremental' and impact['held_models']:
        raise ValueError('Replay requires an unblocked dependency graph: '+json.dumps(impact))
    mapping = get_setting('mapping')
    if mode == 'targeted' and not mapping['country']:
        raise ValueError('Targeted country replay requires country mapping approval.')
    client = ch()
    if cutoff is None:
        with INGEST_LOCK:
            cutoff = timestamp(client.query('SELECT now64(6)').first_row[0])
    run_id = uuid.uuid4().hex
    marker = 'schema_lab_run='+run_id
    variables = {'run_id':run_id,'cursor':get_setting('cursor'),'cutoff':cutoff,
        'replay_mode':mode,'mapping_version':mapping['version'],'country_enabled':mapping['country'],
        'delivery_channel_enabled':mapping.get('delivery_channel',False),
        'amount_field':mapping['amount_field'],'amount_divisor':mapping['amount_divisor'],
        'from_date':from_date,'to_date':to_date,
        **{'cursor_'+entity: value for entity,value in positions.items()}}
    Path('/lab/artifacts').mkdir(exist_ok=True)
    log = Path('/lab/artifacts')/f'{run_id}.log'
    started = datetime.now(timezone.utc)
    with pg() as c:
        c.execute('INSERT INTO control.runs(id,mode,status) VALUES (%s,%s,%s)',(run_id,mode,'running'))
    begin = time.monotonic()
    status = 'failed'
    details = {}
    try:
        staging = allowed & {'raw_events','stg_customers','stg_orders'}
        if mode != 'incremental': staging.discard('stg_customers')
        args = ['run','--select',' '.join(sorted(staging))]
        if mode == 'full': args += ['--full-refresh']
        command(args,variables,log)
        command(['test','--select',' '.join(sorted(staging)),
                 '--indirect-selection','cautious'],variables,log)
        downstream = allowed - {'raw_events','stg_customers','stg_orders'}
        if downstream:
            command(['run','--select',' '.join(sorted(downstream))],variables,log)
        command(['test','--select',' '.join(sorted(allowed)),
                 '--indirect-selection','cautious'],variables,log)
        if mode == 'incremental':
            for entity,root in ROOTS.items():
                if root in staging: positions[entity] = cutoff
            set_setting('entity_cursors',positions)
            set_setting('cursor',min(positions.values()))
        if not impact['held_models']: set_setting('last_gold_success',time.time())
        status = 'partial_success' if impact['held_models'] else 'success'
    except Exception as exc:
        details['error'] = str(exc)
    elapsed = time.monotonic()-begin
    try:
        details.update(collect(client,marker))
    except Exception as exc:
        details.update(read_rows=0,read_bytes=0,downstream_read_rows=0,downstream_read_bytes=0,query_count=0,validation_read_rows=0,validation_read_bytes=0,overhead_read_rows=0,overhead_read_bytes=0,
                       query_log_error=str(exc))
    details.update(impact=impact,entity_cursors=positions)
    details.update(run_id=run_id,mode=mode,status=status,elapsed_seconds=elapsed,cutoff=cutoff,log=log.name)
    # Count projected source events from the same bounded candidate definition.
    # dbt generated SQL is authoritative; report projection result count as a supplemental metric.
    candidate = client.query('SELECT count() FROM silver.stg_orders WHERE mapping_version={version:UInt64}',
        parameters={'version':mapping['version']}).first_row[0] if status in ('success','partial_success') and 'stg_orders' in allowed else 0
    details['projected_events_at_current_version'] = candidate
    client.insert('ops.backfill_runs',[[run_id,mode,started,elapsed,candidate,details['read_rows'],details['read_bytes'],
        details['downstream_read_rows'],details['downstream_read_bytes'],details['query_count'],
        details['validation_read_rows'],details['validation_read_bytes'],details['overhead_read_rows'],details['overhead_read_bytes'],status]])
    with pg() as c:
        c.execute('UPDATE control.runs SET status=%s,finished_at=now(),details=%s WHERE id=%s',(status,Jsonb(details),run_id))
    Path('/lab/artifacts',f'{run_id}.json').write_text(json.dumps(details,indent=2))
    if status == 'failed': raise RuntimeError(json.dumps(details))
    return details

def run(mode='incremental',from_date='',to_date=''):
    if not LOCK.acquire(blocking=False): raise ValueError('Another build/backfill is running.')
    try: return execute(mode,from_date,to_date)
    finally: LOCK.release()

def checksum(client):
    # Exclude mapping_version: provenance differs, business result must match.
    return client.query("SELECT count(),sum(cityHash64(toJSONString(tuple(order_id,customer_sk,date_sk,country_sk,currency_sk,delivery_channel_sk,amount,status)))) FROM gold.fact_order").first_row

def compare_backfills():
    if not LOCK.acquire(blocking=False): raise ValueError('Another build/backfill is running.')
    client = ch()
    backup = 'ops.compare_stg_orders_'+uuid.uuid4().hex
    try:
        from .impact import current
        if current(scan_catalog())['held_models']:
            raise ValueError('Resolve schema holds before comparing replays.')
        mapping = get_setting('mapping')
        if mapping.get('delivery_channel',False):
            raise ValueError('compare is country-only. Use full replay for delivery_channel; country targeted selector cannot repair arbitrary fields.')
        if not mapping['country']: raise ValueError('Approve country mapping first.')
        if not client.query("SELECT count() FROM system.tables WHERE database='silver' AND name='stg_orders'").first_row[0]:
            raise ValueError('Run initial build first.')
        client.command(f'CREATE TABLE {backup} AS silver.stg_orders')
        client.command(f'INSERT INTO {backup} SELECT * FROM silver.stg_orders')
        cutoff = get_setting('cursor')
        full = execute('full',cutoff=cutoff)
        full_checksum = checksum(client)
        client.command('TRUNCATE TABLE silver.stg_orders')
        client.command(f'INSERT INTO silver.stg_orders SELECT * FROM {backup}')
        targeted = execute('targeted',cutoff=cutoff)
        targeted_checksum = checksum(client)
        result = {'full':full,'targeted':targeted,'same_business_result':full_checksum==targeted_checksum,
            'full_checksum':full_checksum,'targeted_checksum':targeted_checksum,
            'note':'Same frozen raw cutoff and same staging baseline. Downstream full rebuild is intentionally included.'}
        Path('/lab/artifacts/comparison.json').write_text(json.dumps(result,indent=2))
        if not result['same_business_result']: raise RuntimeError('Backfill results differ; inspect artifacts/comparison.json')
        client.command(f'DROP TABLE {backup}')
        return result
    except Exception:
        # Keep staging backup available for manual restoration when comparison fails.
        raise
    finally: LOCK.release()
