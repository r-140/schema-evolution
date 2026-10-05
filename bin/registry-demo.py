#!/usr/bin/env python3
import json,time,uuid
from urllib.request import Request,urlopen
from urllib.error import HTTPError,URLError
BASE='http://localhost:8085'
def call(path,body=None,method=None):
    req=Request(BASE+path,data=json.dumps(body).encode() if body is not None else None,
        headers={'Content-Type':'application/vnd.schemaregistry.v1+json'},method=method)
    with urlopen(req,timeout=15) as r:return json.load(r)
for _ in range(60):
    try:call('/subjects');break
    except (URLError,HTTPError):time.sleep(2)
else:raise SystemExit('Registry not ready')
base={'type':'record','name':'Order','fields':[{'name':'id','type':'long'},{'name':'amount','type':'long'}]}
added={**base,'fields':base['fields']+[{'name':'country','type':['null','string'],'default':None}]}
text={**base,'fields':[base['fields'][0],{'name':'amount','type':'string'}]}
rename={**base,'fields':[base['fields'][0],{'name':'total_amount','type':'long'}]}
output={}
for mode in ('BACKWARD_TRANSITIVE','FORWARD_TRANSITIVE','FULL_TRANSITIVE'):
    subject='schema-lab-'+uuid.uuid4().hex
    call('/config/'+subject,{'compatibility':mode},'PUT')
    call('/subjects/'+subject+'/versions',{'schema':json.dumps(base)},'POST')
    output[mode]={name:call('/compatibility/subjects/'+subject+'/versions/latest?verbose=true',
        {'schema':json.dumps(schema)},'POST') for name,schema in [('optional_add',added),('type_change',text),('required_rename',rename)]}
print(json.dumps(output,indent=2))
assert all(x['optional_add']['is_compatible'] for x in output.values())
assert all(not x['type_change']['is_compatible'] for x in output.values())
assert not output['BACKWARD_TRANSITIVE']['required_rename']['is_compatible']
print('PASS: real Registry compatibility APIs')
