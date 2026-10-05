"""HTTP precondition tests with mocked control storage; no Docker required."""
import copy,json,unittest
from pathlib import Path
from unittest.mock import patch
from fastapi.testclient import TestClient
from app import service

class Result:
    def __init__(self,rows):self.rows=rows
    def fetchall(self):return self.rows
class Connection:
    def __init__(self,state):self.state=state
    def __enter__(self):return self
    def __exit__(self,*args):pass
    def transaction(self):return self
    def execute(self,sql,args=None):
        if 'FROM control.settings' in sql:return Result([(k,copy.deepcopy(self.state[k])) for k in ('contract','mapping')])
        if sql.startswith('INSERT INTO control.settings'):
            self.state[args[0]]=copy.deepcopy(args[1].obj);return Result([])
        if 'FROM control.event_schemas' in sql:return Result([])
        if sql.startswith('UPDATE control.changes'):return Result([])
        raise AssertionError(sql)

class HTTPChecks(unittest.TestCase):
    def setUp(self):
        root=Path(__file__).resolve().parents[1]
        self.actual=json.loads((root/'contracts/examples/additive.json').read_text())
        self.state={'contract':json.loads((root/'contracts/baseline.json').read_text()),
                    'mapping':{'version':1,'country':False,'amount_field':'amount','amount_type':'bigint','amount_divisor':100},
                    'mode':'etag','etag_config':{'schema_policy':'hold','require_if_match':True}}
        self.patches=[patch.object(service,'pg',lambda:Connection(self.state)),
            patch.object(service,'catalog',lambda:copy.deepcopy(self.actual)),
            patch.object(service,'get_setting',lambda k,d=None:copy.deepcopy(self.state.get(k,d))),
            patch.object(service,'set_setting',lambda k,v:self.state.__setitem__(k,copy.deepcopy(v))),
            patch.object(service.observer,'clear_cache',lambda:None)]
        for p in self.patches:p.start()
        # Outside context-manager: do not launch actual service lifespan/CDC threads.
        self.client=TestClient(service.app)
    def tearDown(self):
        self.client.close()
        for p in self.patches:p.stop()
    def test_get_cache_validation(self):
        first=self.client.get('/contract');self.assertEqual(first.status_code,200)
        self.assertEqual(self.client.get('/contract',headers={'If-None-Match':first.headers['etag']}).status_code,304)
    def test_conditional_approval_stale_second_reviewer(self):
        tag=self.client.get('/contract').headers['etag']
        first=self.client.put('/contract',json={'country':True},headers={'If-Match':tag})
        self.assertEqual(first.status_code,200,first.text)
        second=self.client.put('/contract',json={'country':True},headers={'If-Match':tag})
        self.assertEqual(second.status_code,412,second.text)
        self.assertEqual(self.state['mapping']['version'],2)
        self.assertEqual(second.headers['etag'],first.headers['etag'])
    def test_missing_token_does_not_mutate(self):
        self.assertEqual(self.client.put('/contract',json={}).status_code,428)
        self.assertEqual(self.client.post('/approve',json={}).status_code,428)
        self.assertEqual(self.state['mapping']['version'],1)
    def test_config_disables_legacy_requirement(self):
        response=self.client.put('/config/preconditions',json={'schema_policy':'warn','require_if_match':False})
        self.assertEqual(response.status_code,200)
        self.assertEqual(self.client.post('/approve',json={'country':True}).status_code,200)
        self.assertEqual(self.client.put('/contract',json={}).status_code,428)
    def test_invalid_policy(self):
        self.assertEqual(self.client.put('/config/preconditions',json={'schema_policy':'typo'}).status_code,400)
    def test_weak_token_rejected(self):
        tag=self.client.get('/contract').headers['etag']
        self.assertEqual(self.client.put('/contract',json={},headers={'If-Match':'W/'+tag}).status_code,400)

if __name__=='__main__':unittest.main()
