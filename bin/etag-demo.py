#!/usr/bin/env python3
"""Fresh-stack control/data-plane precondition demo. Uses only stdlib."""
import json
import sys
from urllib.request import Request,urlopen
from urllib.error import HTTPError
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parent))
from lab import call,ready,register,wait
BASE='http://localhost:8090'
def request(method,path,body=None,headers=None):
    req=Request(BASE+path,method=method,data=json.dumps(body).encode() if body is not None else None,
                headers={'Content-Type':'application/json',**(headers or {})})
    try:
        with urlopen(req,timeout=3600) as r:
            raw=r.read();return r.status,{k.lower(): v for k, v in r.headers.items()},json.loads(raw) if raw else None
    except HTTPError as r:
        raw=r.read();return r.code,{k.lower(): v for k, v in r.headers.items()},json.loads(raw) if raw else None
ready();register()
call('/mode/etag',{})
call('/config/preconditions',{'schema_policy':'hold','require_if_match':True},'PUT')
call('/generate',{'count':1000});wait();call('/build',{'mode':'incremental'})
status,headers,resource=request('GET','/contract');assert status==200
old=headers['etag']
assert request('GET','/contract',headers={'If-None-Match':old})[0]==304
call('/migrate/add-country',{});call('/generate',{'count':100});wait()
assert request('POST','/build',{'mode':'incremental'})[0]==409
schemas=call('/event-schemas')
checks=[call('/event-precondition/'+s['schema_id']) for s in schemas if s['entity']=='orders']
assert any(c['reader_compatible'] and not c['matched'] for c in checks)
body={'country':True}
assert request('PUT','/contract',body)[0]==428
status,headers,_=request('PUT','/contract',body,{'If-Match':old});assert status==200
new=headers['etag'];assert new!=old
assert request('PUT','/contract',body,{'If-Match':old})[0]==412
call('/build',{'mode':'incremental'})
# Configurable alert-only path: equality mismatch does not block technically compatible data.
call('/config/preconditions',{'schema_policy':'warn','require_if_match':False},'PUT')
call('/migrate/add-channel',{});call('/generate',{'count':10});wait()
call('/build',{'mode':'incremental'})  # compatible drift continues in warn mode
call('/config/preconditions',{'schema_policy':'off','require_if_match':False},'PUT')
call('/mode/hybrid',{})
print(json.dumps({'passed':True,'old_contract_etag':old,'new_contract_etag':new,
 'event_checks':checks,'checks':['304 cache validation','428 missing token','412 stale writer',
 'compatible new schema fails exact equality','raw continues while Gold held','conditional approval and recovery','runtime configuration']},indent=2))
