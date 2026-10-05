import json
import logging
import os
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime
import requests
import uvicorn
from fastapi import FastAPI, HTTPException, Response, Header
from pydantic import BaseModel, Field
from prometheus_client import generate_latest, CONTENT_TYPE_LATEST
from psycopg.types.json import Jsonb
from . import observer, sink, metrics
from .storage import pg,ch,catalog,get_setting,set_setting,initialize
from .compatibility import fingerprint, connect_fields,event_compatible
from .generator import generate,mutate
from .migrations import migrate,populate_country,MIGRATIONS
from .pipeline import run,compare_backfills,LOCK
from .etag import contract_etag,require_match,PreconditionError,event_check,schema_precondition

logging.basicConfig(level=logging.INFO)
STOP=threading.Event()

def monitoring_loop():
    while not STOP.is_set():
        try:
            observer.scan_catalog();metrics.refresh();metrics.OBSERVER_OK.set(1)
        except Exception:
            metrics.OBSERVER_OK.set(0);logging.exception('observer/metrics failure')
        STOP.wait(5)

@asynccontextmanager
async def lifespan(app):
    initialize()
    threads=[threading.Thread(target=fn,args=(STOP,),daemon=True) for fn in (sink.loop,)]
    threads.append(threading.Thread(target=monitoring_loop,daemon=True))
    for t in threads: t.start()
    yield
    STOP.set()
    for t in threads: t.join(timeout=5)

app=FastAPI(title='CDC Schema Evolution Lab',lifespan=lifespan)

class Generation(BaseModel):
    count:int=Field(default=1000,ge=1,le=1000000)
    customers:int=Field(default=100,ge=1,le=100000)
    seed:int=42
class Approval(BaseModel):
    country:bool=False
    amount_field:str='amount'
    amount_type:str='bigint'
    amount_divisor:int=Field(default=100,gt=0)
class Build(BaseModel):
    mode:str='incremental'
    from_date:str=''
    to_date:str=''
class Mutation(BaseModel):
    order_id:int
    status:str='paid'
    delete:bool=False

@app.get('/health')
def health():
    with pg() as c: c.execute('SELECT 1')
    ch().query('SELECT 1')
    return {'status':'ok'}

@app.get('/metrics')
def prometheus(): return Response(generate_latest(),media_type=CONTENT_TYPE_LATEST)

@app.get('/state')
def state():
    with pg() as c:
        rows=c.execute('SELECT id,entity,kind,details,first_seen,resolved_at FROM control.changes ORDER BY id').fetchall()
        runs=c.execute('SELECT id,mode,status,details FROM control.runs ORDER BY started_at DESC LIMIT 10').fetchall()
    actual=catalog()
    guard=schema_precondition(get_setting('mode'),get_setting('etag_config',{}),get_setting('contract'),actual)
    return {'catalog':actual,'contract':get_setting('contract'),'mapping':get_setting('mapping'),
        'mode':get_setting('mode'),'etag_config':get_setting('etag_config'),'cursor':get_setting('cursor'),'publication_blocked':observer.blocked() or guard['hold'],'schema_precondition':guard,
        'changes':[dict(zip(['id','entity','kind','details','first_seen','resolved_at'],r)) for r in rows],
        'runs':[dict(zip(['id','mode','status','details'],r)) for r in runs]}

@app.post('/register-cdc')
def register():
    config={'connector.class':'io.debezium.connector.postgresql.PostgresConnector','tasks.max':'1',
        'database.hostname':'source','database.port':'5432','database.user':'lab','database.password':'lab',
        'database.dbname':'lab','topic.prefix':'lab','plugin.name':'pgoutput','slot.name':'evolution_slot',
        'publication.name':'evolution_pub','publication.autocreate.mode':'disabled',
        'table.include.list':'sales.customers,sales.orders','snapshot.mode':'initial',
        'decimal.handling.mode':'string','tombstones.on.delete':'true'}
    url=os.getenv('CONNECT_URL','http://connect:8083')+'/connectors/evolution-source/config'
    r=requests.put(url,json=config,timeout=30)
    if not r.ok: raise HTTPException(502,r.text)
    return r.json()

@app.get('/cdc-status')
def cdc_status():
    r=requests.get(os.getenv('CONNECT_URL','http://connect:8083')+'/connectors/evolution-source/status',timeout=10)
    if not r.ok: raise HTTPException(502,r.text)
    return r.json()

@app.post('/generate')
def data(body:Generation): return generate(body.count,body.customers,body.seed)

@app.post('/mutate')
def mutation(body:Mutation): return {'affected':mutate(body.order_id,body.status,body.delete)}

@app.post('/migrate/{name}')
def migration(name:str):
    if name not in MIGRATIONS: raise HTTPException(400,{'available':list(MIGRATIONS)})
    try:
        result=migrate(name);observer.scan_catalog();return result
    except Exception as exc: raise HTTPException(409,str(exc))

@app.post('/source-enrich-country')
def enrich(limit:int=1000):
    if not 1<=limit<=1000000: raise HTTPException(400,'Invalid limit')
    return {'updated':populate_country(limit),'note':'New source UPDATE events, not historical value recovery.'}

@app.post('/mode/{mode}')
def set_mode(mode:str):
    if mode not in ('hybrid','strict','etag'): raise HTTPException(400,'Use hybrid, strict or etag')
    if not LOCK.acquire(blocking=False): raise HTTPException(409,'Build running')
    try:
        set_setting('mode',mode);return {'mode':mode}
    finally: LOCK.release()

class ETagConfig(BaseModel):
    schema_policy:str='hold'
    require_if_match:bool=True

@app.get('/config/preconditions')
def preconditions_config():
    return {'mode':get_setting('mode'),'etag':get_setting('etag_config')}

@app.put('/config/preconditions')
def configure_preconditions(body:ETagConfig):
    if body.schema_policy not in ('hold','warn','off'):
        raise HTTPException(400,'schema_policy: hold, warn or off')
    if not LOCK.acquire(blocking=False): raise HTTPException(409,'Build running')
    try:
        set_setting('etag_config',body.model_dump())
        return preconditions_config()
    finally: LOCK.release()

def contract_snapshot():
    with pg() as c:
        data=dict(c.execute("SELECT name,value FROM control.settings WHERE name IN ('contract','mapping')").fetchall())
    return data

@app.get('/contract')
def read_contract(if_none_match:str|None=Header(default=None)):
    from fastapi.responses import JSONResponse
    data=contract_snapshot()
    tag=contract_etag(data['contract'],data['mapping'])
    if if_none_match==tag: return Response(status_code=304,headers={'ETag':tag,'Cache-Control':'no-cache'})
    return JSONResponse(data,headers={'ETag':tag,'Cache-Control':'no-cache'})

@app.put('/contract')
def conditional_approval(body:Approval,if_match:str|None=Header(default=None)):
    if if_match is None: raise HTTPException(428,'If-Match required; GET /contract first')
    return approve(body,if_match)

@app.post('/approve')
def approve(body:Approval,if_match:str|None=Header(default=None)):
    # Legacy approval remains available; /contract requires optimistic concurrency.
    if not LOCK.acquire(blocking=False): raise HTTPException(409,'Build running')
    try:
        if get_setting('mode')=='etag' and get_setting('etag_config',{}).get('require_if_match',True) and if_match is None:
            raise HTTPException(428,'ETag mode requires If-Match; GET /contract first')
        actual=catalog()
        with pg() as c:
            with c.transaction():
                # All approval writers lock the same rows before reading and validating.
                data=dict(c.execute("SELECT name,value FROM control.settings WHERE name IN ('contract','mapping') ORDER BY name FOR UPDATE").fetchall())
                current=contract_etag(data['contract'],data['mapping'])
                if if_match is not None:
                    try: require_match(if_match,current)
                    except PreconditionError as exc:
                        raise HTTPException(exc.status,str(exc),headers={'ETag':current})
                if body.amount_field not in actual['orders']: raise HTTPException(400,'amount_field missing in source')
                if body.amount_type not in ('bigint','text'): raise HTTPException(400,'Unsupported amount_type')
                if actual['orders'][body.amount_field]['type'] != body.amount_type:
                    raise HTTPException(400,'amount_type does not match source')
                if actual['orders'][body.amount_field]['nullable']: raise HTTPException(400,'Required amount must be NOT NULL')
                if body.country and 'country' not in actual['orders']: raise HTTPException(400,'country missing')
                if body.amount_field not in ('amount','total_amount'): raise HTTPException(400,'Demo supports amount or total_amount')
                mapping={'version':data['mapping']['version']+1,**body.model_dump()}
                for entity in ('customers','orders'):
                    expected=dict(data['contract'][entity])
                    if entity=='orders':
                        expected.pop('amount',None);expected.pop('total_amount',None);expected.pop('country',None)
                    diff=observer.compare(expected,actual[entity])
                    breaking=[x for x in diff['changes'] if x['severity']=='breaking']
                    if breaking: raise HTTPException(400,{'unsupported_changes':breaking})
                for name,value in [('contract',actual),('mapping',mapping)]:
                    c.execute('INSERT INTO control.settings VALUES (%s,%s) ON CONFLICT(name) DO UPDATE SET value=excluded.value',(name,Jsonb(value)))
                c.execute('UPDATE control.changes SET resolved_at=now() WHERE resolved_at IS NULL')
                for sid,entity,schema in c.execute('SELECT fingerprint,entity,schema_json FROM control.event_schemas').fetchall():
                    allowed,_=event_compatible(connect_fields(schema),mapping,entity)
                    c.execute('UPDATE control.event_schemas SET allowed=%s WHERE fingerprint=%s',(allowed,sid))
        observer.clear_cache()
        from fastapi.responses import JSONResponse
        return JSONResponse({'mapping':mapping,'note':'Approval changes future projection only. Historical events require replay.'},
                            headers={'ETag':contract_etag(actual,mapping)})
    finally: LOCK.release()

@app.get('/event-schemas')
def event_schemas():
    with pg() as c:
        rows=c.execute('SELECT fingerprint,entity,allowed FROM control.event_schemas ORDER BY first_seen').fetchall()
    return [dict(zip(['schema_id','entity','reader_compatible'],r)) for r in rows]

@app.get('/event-precondition/{schema_id}')
def writer_precondition(schema_id:str):
    with pg() as c:
        row=c.execute('SELECT entity,schema_json,allowed FROM control.event_schemas WHERE fingerprint=%s',(schema_id,)).fetchone()
    if row is None: raise HTTPException(404,'Event schema not observed yet')
    result=event_check(get_setting('contract'),row[0],row[1])
    return {**result,'reader_compatible':row[2],'schema_id':schema_id,
        'note':'Application-level event precondition; no HTTP header is being transported through Kafka.'}

@app.post('/semantic-change')
def semantic_change():
    with pg() as c:
        c.execute("INSERT INTO control.changes(entity,fingerprint,kind,details) VALUES ('orders',%s,'semantic',%s) ON CONFLICT DO NOTHING",
            (fingerprint({'units':'major'}),Jsonb({'amount_units':'minor -> major','note':'Explicit metadata event; structural diff cannot detect units.'})))
    return {'publication_blocked':True}

@app.post('/build')
def build(body:Build):
    if body.mode not in ('incremental','full','targeted'): raise HTTPException(400,'Invalid mode')
    for value in (body.from_date,body.to_date):
        if value:
            try: datetime.strptime(value,'%Y-%m-%d %H:%M:%S.%f')
            except ValueError: raise HTTPException(400,'Dates: YYYY-MM-DD HH:MM:SS.ffffff UTC')
    try: return run(body.mode,body.from_date,body.to_date)
    except ValueError as exc: raise HTTPException(409,str(exc))
    except Exception as exc: raise HTTPException(500,str(exc))

@app.post('/compare-backfills')
def compare():
    try: return compare_backfills()
    except ValueError as exc: raise HTTPException(409,str(exc))
    except Exception as exc: raise HTTPException(500,str(exc))

@app.get('/gold')
def gold():
    try:
        r=ch().query('SELECT * FROM gold.revenue_by_country ORDER BY order_date,country LIMIT 100')
        return [dict(zip(r.column_names,row)) for row in r.result_rows]
    except Exception as exc: raise HTTPException(409,str(exc))

@app.get('/gold-summary')
def gold_summary():
    try:
        r=ch().query("SELECT count(),countIf(country_sk=lower(hex(SHA256('UNKNOWN')))),sum(amount) FROM gold.fact_order").first_row
        return dict(zip(['orders','unknown_country_orders','revenue'],r))
    except Exception as exc: raise HTTPException(409,str(exc))



class AmountRepair(BaseModel):
    event_id:str
    corrected_amount_minor:int=Field(ge=0)
    reason:str=Field(min_length=10)

@app.get('/invalid-events')
def invalid_events():
    try:
        r=ch().query('SELECT event_id,order_id,amount_minor FROM silver.stg_orders WHERE amount_minor IS NULL OR amount_minor<0 LIMIT 100')
        return [dict(zip(r.column_names,row)) for row in r.result_rows]
    except Exception as exc: raise HTTPException(409,str(exc))

@app.post('/repair-amount')
def repair_amount(body:AmountRepair):
    if not LOCK.acquire(blocking=False): raise HTTPException(409,'Build running')
    try:
        from datetime import timezone
        client=ch()
        if not client.query("SELECT count() FROM bronze.events WHERE entity='orders' AND event_id={event_id:String}",parameters={'event_id':body.event_id}).first_row[0]:
            raise HTTPException(404,'Original order event not found')
        client.insert('ops.event_repairs',[[body.event_id,body.corrected_amount_minor,body.reason,datetime.now(timezone.utc)]])
        return {'stored':True,'note':'Original envelope is unchanged. Full projection replay applies explicit correction and preserves its reason in ops.event_repairs.'}
    finally: LOCK.release()

@app.post('/alert-receiver')
def receive_alert(body:dict):
    # Local receiver only: no email/Slack messages sent by the artifact.
    from pathlib import Path
    Path('/lab/artifacts').mkdir(exist_ok=True)
    with open('/lab/artifacts/alerts.jsonl','a') as f: f.write(json.dumps(body)+'\n')
    logging.warning('Grafana alert: %s',json.dumps(body))
    return {'received':True}

@app.get('/sync-check')
def sync_check():
    from psycopg.rows import dict_row
    from psycopg import sql
    client=ch()
    details={}
    def normalize(value):
        if isinstance(value,datetime): return value.isoformat()
        if isinstance(value,str):
            try: return datetime.fromisoformat(value.replace('Z','+00:00')).isoformat()
            except ValueError: return value
        return str(value) if value is not None else None
    for entity in ('customers','orders'):
        latest=client.query("SELECT argMax(payload,tuple(source_lsn,kafka_offset)) FROM bronze.events WHERE entity={entity:String} AND op IN ('c','u','r','d') GROUP BY JSONExtractInt(if(op='d',JSONExtractRaw(payload,'before'),JSONExtractRaw(payload,'after')),'id')",parameters={'entity':entity}).result_rows
        actual={}
        for row in latest:
            payload=json.loads(row[0])
            if payload['op']!='d': actual[payload['after']['id']]=payload['after']
        with pg() as c:
            c.row_factory=dict_row
            expected=c.execute(sql.SQL('SELECT * FROM sales.{}').format(sql.Identifier(entity))).fetchall()
        mismatches=0
        for row in expected:
            event=actual.get(row['id'])
            if event is None: mismatches+=1;continue
            for field in ('updated_at','status','name','segment','country'):
                if field in row and normalize(row[field])!=normalize(event.get(field)):
                    mismatches+=1;break
        details[entity]={'source_rows':len(expected),'raw_current_rows':len(actual),'mismatched_rows':mismatches}
    return {'synchronized':all(x['source_rows']==x['raw_current_rows'] and x['mismatched_rows']==0 for x in details.values()),'tables':details}

if __name__=='__main__': uvicorn.run(app,host='0.0.0.0',port=8090)
