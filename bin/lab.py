#!/usr/bin/env python3
"""Host CLI uses only Python standard library; server owns dependencies."""
import argparse
import json
import os
import sys
import time
from urllib.request import Request,urlopen
from urllib.error import HTTPError,URLError
BASE=os.getenv('LAB_URL','http://localhost:8090')

def call(path,body=None,method=None):
    data=json.dumps(body).encode() if body is not None else None
    req=Request(BASE+path,data=data,headers={'Content-Type':'application/json'},method=method)
    try:
        with urlopen(req,timeout=3600) as r: return json.load(r)
    except HTTPError as exc: raise RuntimeError(exc.read().decode()) from exc

def wait(timeout=180):
    end=time.time()+timeout
    stable=0
    while time.time()<end:
        try:
            state=call('/sync-check')
            if state['synchronized']:
                stable+=1
                if stable>=3: return state
            else: stable=0
        except (URLError,RuntimeError): stable=0
        time.sleep(2)
    raise RuntimeError('CDC did not synchronize; inspect ./bin/dev cdc-status, ./bin/dev state and logs. Strict mode may intentionally hold ingestion.')

def ready(timeout=180):
    end=time.time()+timeout
    while time.time()<end:
        try: return call('/health')
        except (URLError,RuntimeError): time.sleep(2)
    raise RuntimeError('Lab API not ready')

def register():
    end=time.time()+180
    while time.time()<end:
        try:return call('/register-cdc',{},'POST')
        except (URLError,RuntimeError):time.sleep(2)
    raise RuntimeError('Connect not ready for connector registration')

def demo(count):
    ready();register()
    call('/generate',{'count':count,'customers':100})
    wait();initial=call('/build',{'mode':'incremental'})
    call('/migrate/add-country',{})
    call('/generate',{'count':max(100,count//10),'customers':100,'seed':43})
    wait();call('/build',{'mode':'incremental'})
    before=call('/gold-summary')
    call('/approve',{'country':True})
    comparison=call('/compare-backfills',{})
    after=call('/gold-summary')
    return {'before_approval_and_replay':before,'after_replay':after,'comparison':comparison,
        'note':'Old orders without country in raw remain UNKNOWN. Only captured country values can be replayed.'}

def smoke():
    result=demo(1000)
    assert result['comparison']['same_business_result']
    assert result['after_replay']['orders']==1100
    assert result['after_replay']['unknown_country_orders']==1000
    call('/mutate',{'order_id':1,'status':'paid'});wait();call('/build',{'mode':'incremental'})
    call('/mutate',{'order_id':2,'delete':True});wait();call('/build',{'mode':'incremental'})
    assert call('/gold-summary')['orders']==1099
    call('/mode/strict',{});call('/migrate/rename-amount',{})
    call('/generate',{'count':10,'customers':100});time.sleep(5)
    assert call('/state')['publication_blocked']
    try:call('/build',{'mode':'incremental'})
    except RuntimeError:pass
    else:raise AssertionError('Breaking schema did not hold build')
    call('/mode/hybrid',{});wait()
    call('/approve',{'country':True,'amount_field':'total_amount'})
    call('/build',{'mode':'full'})
    assert call('/gold-summary')['orders']==1109
    return {'passed':True,'checks':['country replay equivalence','missing historical country','update/delete','strict/hybrid transition','breaking publication hold','historical rename replay']}

def main():
    parser=argparse.ArgumentParser(description='Schema evolution lab CLI')
    parser.add_argument('command',nargs='?',default='help')
    parser.add_argument('value',nargs='?')
    parser.add_argument('--count',type=int,default=10000)
    parser.add_argument('--customers',type=int,default=100)
    parser.add_argument('--country',action='store_true')
    parser.add_argument('--amount-field',default='amount')
    parser.add_argument('--amount-minor',type=int,default=12500)
    parser.add_argument('--policy',default='hold',choices=['hold','warn','off'])
    parser.add_argument('--require-if-match',default='true',choices=['true','false'])
    parser.add_argument('--reason',default='Explicit demo correction after source-owner investigation')
    parser.add_argument('--amount-type',default='bigint',choices=['bigint','text'])
    parser.add_argument('--amount-divisor',type=int,default=100)
    parser.add_argument('--from-date',default='')
    parser.add_argument('--to-date',default='')
    args=parser.parse_args();cmd=args.command
    if cmd=='help':
        parser.print_help();print('Commands: ready register-cdc cdc-status generate wait build migrate approve mode mutate delete enrich-country semantic-change compare demo smoke state gold gold-summary invalid-events repair-amount etag-config');return
    if cmd=='ready':result=ready()
    elif cmd=='wait':result=wait()
    elif cmd=='demo':result=demo(args.count)
    elif cmd=='smoke':result=smoke()
    elif cmd=='register-cdc':result=register()
    elif cmd=='generate':result=call('/generate',{'count':int(args.value or args.count),'customers':args.customers})
    elif cmd=='build':result=call('/build',{'mode':args.value or 'incremental','from_date':args.from_date,'to_date':args.to_date})
    elif cmd=='migrate':result=call('/migrate/'+str(args.value),{})
    elif cmd=='mode':result=call('/mode/'+str(args.value),{})
    elif cmd=='approve':result=call('/approve',{'country':args.country,'amount_field':args.amount_field,'amount_type':args.amount_type,'amount_divisor':args.amount_divisor})
    elif cmd in ('mutate','delete'):result=call('/mutate',{'order_id':int(args.value or 1),'status':'paid','delete':cmd=='delete'})
    elif cmd=='enrich-country':result=call('/source-enrich-country?limit='+str(args.value or args.count),{})
    elif cmd=='etag-config':result=call('/config/preconditions',{'schema_policy':args.policy,'require_if_match':args.require_if_match=='true'},'PUT')
    elif cmd=='repair-amount':result=call('/repair-amount',{'event_id':args.value,'corrected_amount_minor':args.amount_minor,'reason':args.reason})
    elif cmd=='compare':result=call('/compare-backfills',{})
    elif cmd in ('semantic-change',):result=call('/'+cmd,{})
    elif cmd in ('cdc-status','state','gold','gold-summary','invalid-events'):result=call('/'+cmd)
    else: parser.error('Unknown command '+cmd)
    print(json.dumps(result,indent=2,default=str))

if __name__=='__main__':
    try: main()
    except Exception as exc: print(str(exc),file=sys.stderr);sys.exit(1)
