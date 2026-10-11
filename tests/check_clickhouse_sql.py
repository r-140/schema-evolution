#!/usr/bin/env python3
"""Real embedded ClickHouse SQL tests. Requires chdb==3.3.0 and dbt parse manifest.
Does not substitute for Docker/Kafka/adapter materialization integration tests.
"""
import json
import tempfile
from pathlib import Path
from jinja2 import Environment,StrictUndefined
from chdb.session import Session

ROOT=Path(__file__).resolve().parents[1]
manifest=json.loads((ROOT/'dbt/target/manifest.json').read_text())
models={n['name']:n for n in manifest['nodes'].values() if n['resource_type']=='model'}
folder=tempfile.TemporaryDirectory()
session=Session(folder.name)

def query(sql): return session.query(sql,'JSONEachRow')
def rows(sql):
    result=str(query(sql)).strip()
    return [json.loads(line) for line in result.splitlines()] if result else []

for statement in (ROOT/'infra/clickhouse/init.sql').read_text().split(';'):
    if statement.strip(): query(statement)

def insert(entity,event_id,offset,body,op='c',before=None,ingest='2026-10-01 10:00:00.000000'):
    payload={'op':op,'after':body,'before':before,'source':{'lsn':offset,'ts_ms':0}}
    values={'event_id':event_id,'topic':'lab.sales.'+entity,'partition_id':0,'kafka_offset':offset,'entity':entity,
        'op':op,'schema_id':'test','source_lsn':offset,'source_ts_ms':0,'ingest_at':ingest,
        'key_json':'{}','envelope':json.dumps({'schema':{},'payload':payload}),'payload':json.dumps(payload)}
    query('INSERT INTO bronze.events FORMAT JSONEachRow\n'+json.dumps(values))

v={'cutoff':'2100-01-01 00:00:00.000000','cursor':'1970-01-01 00:00:00.000000','mapping_version':1,
    'country_enabled':False,'amount_field':'amount','amount_divisor':100,'replay_mode':'incremental'}

def render(name,incremental=False):
    schema=models[name]['schema']
    env=Environment(undefined=StrictUndefined)
    env.globals.update(config=lambda **kw:'',ref=lambda n:models[n]['schema']+'.'+n,
        source=lambda a,b:('ops.' if a=='operations' else 'bronze.')+b,var=lambda n,d=None:v.get(n,d),is_incremental=lambda:incremental,
        this=schema+'.'+name)
    macros=(ROOT/'dbt/macros/event_filter.sql').read_text()
    return env.from_string(macros+'\n'+models[name]['raw_code']).render()

def build_model(name):
    schema=models[name]['schema'];query(f'DROP TABLE IF EXISTS {schema}.{name}')
    materialized=models[name]['config']['materialized']
    engine='VIEW' if materialized=='view' else 'TABLE'
    suffix='' if engine=='VIEW' else ' ENGINE=MergeTree ORDER BY tuple()'
    try: query(f'CREATE {engine} {schema}.{name}{suffix} AS '+render(name))
    except Exception as exc: raise RuntimeError(name+': '+str(exc)) from exc

order=[]
visited=set()
def visit(name):
    if name in visited:return
    for dep in models[name]['depends_on']['nodes']:
        if dep.startswith('model.'):visit(manifest['nodes'][dep]['name'])
    visited.add(name);order.append(name)
for name in models:visit(name)

def downstream():
    for name in order:
        if name not in ('raw_events','stg_customers','stg_orders'):build_model(name)

def facts():return rows('SELECT order_id,customer_sk,date_sk,country_sk,currency_sk,amount,status FROM gold.fact_order ORDER BY order_id')

customer={'id':1,'name':'Alice','segment':'retail','updated_at':'2026-10-01T10:00:00Z'}
base={'id':1,'customer_id':1,'amount':10000,'currency':'PLN','status':'created','ordered_at':'2026-10-01T10:00:00Z','updated_at':'2026-10-01T10:00:00Z'}
insert('customers','c1',1,customer)
insert('orders','o1',2,base)
insert('orders','o1',2,base,ingest='2026-10-01 10:00:01.000000')
insert('orders','o2',3,{**base,'id':2,'country':'PL'})
for name in order:build_model(name)
assert rows('SELECT count() n FROM silver.stg_orders')[0]['n']==2,'At-least-once deduplication failed'
assert rows("SELECT count() n FROM gold.fact_order WHERE country_sk=lower(hex(SHA256('UNKNOWN')))")[0]['n']==2
# Same baseline, full versus targeted replay.
query('CREATE TABLE ops.baseline ENGINE=MergeTree ORDER BY tuple() AS SELECT * FROM silver.stg_orders')
v.update(country_enabled=True,mapping_version=2,replay_mode='full')
build_model('stg_orders');downstream();full=facts()
query('TRUNCATE TABLE silver.stg_orders');query('INSERT INTO silver.stg_orders SELECT * FROM ops.baseline')
v['replay_mode']='targeted'
query('CREATE TABLE ops.targeted ENGINE=MergeTree ORDER BY tuple() AS '+render('stg_orders',True))
assert rows('SELECT count() n FROM ops.targeted')[0]['n']==1
query('CREATE TABLE ops.merged ENGINE=MergeTree ORDER BY tuple() AS SELECT * FROM silver.stg_orders WHERE event_id NOT IN (SELECT event_id FROM ops.targeted) UNION ALL SELECT * FROM ops.targeted')
query('TRUNCATE TABLE silver.stg_orders');query('INSERT INTO silver.stg_orders SELECT * FROM ops.merged')
downstream();assert full==facts(),'Full and targeted business results differ'
# Expand/contract rename + text cast, update/delete and consecutive satellite changes.
insert('orders','o3',4,{**base,'id':3,'amount':None,'total_amount':'25000','country':'DE'})
insert('orders','o4',5,{**base,'status':'paid'},op='u',before=base)
insert('orders','o5',6,None,op='d',before={**base,'status':'paid'})
v.update(amount_field='total_amount',replay_mode='full')
build_model('stg_orders');downstream()
assert [r['order_id'] for r in facts()]==[2,3]
assert facts()[1]['amount']==250
for name,node in manifest['nodes'].items():
    if node['resource_type']=='test' and name.startswith('test.schema_evolution_lab.') and node.get('raw_code') and node['original_file_path'].startswith('tests/'):
        env=Environment(undefined=StrictUndefined);env.globals.update(ref=lambda n:models[n]['schema']+'.'+n)
        sql=env.from_string(node['raw_code']).render();assert not rows(sql),node['name']
# Invalid text amount must be detected, never silently cast to zero.
insert('orders','o6',7,{**base,'id':4,'amount':None,'total_amount':'broken'})
build_model('stg_orders')
assert len(rows('SELECT * FROM silver.stg_orders WHERE amount_minor IS NULL'))==1
query("INSERT INTO ops.event_repairs VALUES ('o6',12500,'Reviewed correction','2026-10-04 10:00:00.000000')")
build_model('stg_orders');downstream()
assert not rows('SELECT * FROM silver.stg_orders WHERE amount_minor IS NULL')
assert rows("SELECT JSONExtractString(payload,'after','total_amount') original FROM bronze.events WHERE event_id='o6'")[0]['original']=='broken'
insert('orders','o7',8,{**base,'id':5,'amount':None,'total_amount':'35000','delivery_channel':'online'})
v['delivery_channel_enabled']=True
build_model('stg_orders');downstream()
assert rows("SELECT orders FROM gold.revenue_by_delivery_channel WHERE delivery_channel='online'")[0]['orders']==1
assert not rows("SELECT f.order_id FROM gold.fact_order f LEFT JOIN gold.dim_delivery_channel c ON f.delivery_channel_sk=c.delivery_channel_sk WHERE c.delivery_channel_sk=''")
print('PASS: all model SQL; raw dedup; absent historical country; full/targeted equivalence; rename/text replay; updates/deletes; Data Vault and star invariants; invalid amount gate and audited repair')
session.close();folder.cleanup()
