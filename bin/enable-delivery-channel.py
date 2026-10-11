#!/usr/bin/env python3
"""Read/review current contract first; preserve existing reader settings on approval."""
import json
import os
from urllib.request import Request,build_opener,ProxyHandler
base=os.getenv('LAB_URL','http://127.0.0.1:8090')
opener=build_opener(ProxyHandler({}))
with opener.open(base+'/contract',timeout=30) as response:
    resource=json.load(response)
    token=response.headers['ETag']
body={k:resource['mapping'][k] for k in ('country','amount_field','amount_type','amount_divisor')}
body['delivery_channel']=True
print('Proposed reader settings:',json.dumps(body))
request=Request(base+'/contract',method='PUT',data=json.dumps(body).encode(),
                headers={'Content-Type':'application/json','If-Match':token})
with opener.open(request,timeout=30) as response:
    print(json.dumps(json.load(response),indent=2))
