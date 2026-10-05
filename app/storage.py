import json
import os
import psycopg
import clickhouse_connect
from psycopg.types.json import Jsonb

def pg():
    return psycopg.connect(os.getenv('PG_DSN', 'postgresql://lab:lab@localhost:5433/lab'), autocommit=True)

def ch():
    return clickhouse_connect.get_client(host=os.getenv('CH_HOST', 'localhost'), port=8123,
        username=os.getenv('CH_USER', 'lab'), password=os.getenv('CH_PASSWORD', 'lab'))

def get_setting(name, default=None):
    with pg() as c:
        r = c.execute('SELECT value FROM control.settings WHERE name=%s', (name,)).fetchone()
        return r[0] if r else default

def set_setting(name, value):
    with pg() as c:
        c.execute('INSERT INTO control.settings VALUES (%s,%s) ON CONFLICT(name) DO UPDATE SET value=excluded.value', (name,Jsonb(value)))

def catalog():
    with pg() as c:
        rows = c.execute("SELECT cols.table_name,cols.column_name,cols.data_type,cols.is_nullable,EXISTS (SELECT 1 FROM information_schema.table_constraints tc JOIN information_schema.key_column_usage k USING(constraint_catalog,constraint_schema,constraint_name) WHERE tc.constraint_type='PRIMARY KEY' AND tc.table_schema=cols.table_schema AND tc.table_name=cols.table_name AND k.column_name=cols.column_name) FROM information_schema.columns cols WHERE cols.table_schema='sales' ORDER BY cols.table_name,cols.ordinal_position").fetchall()
    result = {}
    for table, name, typ, nullable, primary_key in rows:
        result.setdefault(table,{})[name] = {'type':typ, 'nullable':nullable=='YES','primary_key':primary_key}
    return result

def initialize():
    baseline = json.load(open('/lab/contracts/baseline.json'))
    if get_setting('contract') is None:
        set_setting('contract', baseline)
        set_setting('mapping', {'version':1,'country':False,'amount_field':'amount','amount_type':'bigint','amount_divisor':100})
        set_setting('mode',os.getenv('SCHEMA_GUARD_MODE','hybrid'))
        set_setting('cursor','1970-01-01 00:00:00.000000')

    if get_setting('etag_config') is None:
        set_setting('etag_config',{'schema_policy':os.getenv('ETAG_SCHEMA_POLICY','hold'),
            'require_if_match':os.getenv('ETAG_REQUIRE_IF_MATCH','true').lower()=='true'})
