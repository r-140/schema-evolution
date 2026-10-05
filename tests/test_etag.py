import unittest
from app.etag import contract_etag,require_match,PreconditionError,event_check,schema_precondition
from app.compatibility import compare

class Preconditions(unittest.TestCase):
    def setUp(self):
        self.contract={'orders':{'id':{'type':'bigint','nullable':False,'primary_key':True}}}
        self.mapping={'version':1,'amount_divisor':100}
        self.tag=contract_etag(self.contract,self.mapping)
    def test_matching_strong_token(self):require_match(self.tag,self.tag)
    def test_missing_token_428(self):
        with self.assertRaises(PreconditionError) as c:require_match(None,self.tag)
        self.assertEqual(c.exception.status,428)
    def test_stale_token_412(self):
        with self.assertRaises(PreconditionError) as c:require_match('"'+'0'*64+'"',self.tag)
        self.assertEqual(c.exception.status,412)
    def test_weak_wildcard_and_list_rejected(self):
        for value in ('W/'+self.tag,'*',self.tag+','+self.tag):
            with self.assertRaises(PreconditionError) as c:require_match(value,self.tag)
            self.assertEqual(c.exception.status,400)
    def test_mapping_semantics_change_token(self):
        self.assertNotEqual(self.tag,contract_etag(self.contract,{**self.mapping,'amount_divisor':1}))
    def test_revision_prevents_aba(self):
        self.assertNotEqual(self.tag,contract_etag(self.contract,{**self.mapping,'version':3}))
    def test_addition_compatible_but_exact_tag_differs(self):
        new={'orders':{**self.contract['orders'],'country':{'type':'text','nullable':True}}}
        self.assertTrue(compare(self.contract['orders'],new['orders'])['compatible'])
        self.assertTrue(schema_precondition('etag',{'schema_policy':'hold'},self.contract,new)['hold'])
    def test_warn_is_mismatch_without_hold(self):
        r=schema_precondition('etag',{'schema_policy':'warn'},self.contract,{})
        self.assertTrue(r['mismatch']);self.assertFalse(r['hold'])
    def test_off_and_hybrid_disable_equality_gate(self):
        self.assertFalse(schema_precondition('etag',{'schema_policy':'off'},self.contract,{})['mismatch'])
        self.assertFalse(schema_precondition('hybrid',{'schema_policy':'hold'},self.contract,{})['hold'])
    def test_connect_writer_tag(self):
        schema={'fields':[{'field':'after','fields':[{'field':'id','type':'int64','optional':False}]}]}
        self.assertTrue(event_check(self.contract,'orders',schema)['matched'])
        schema['fields'][0]['fields'].append({'field':'country','type':'string','optional':True})
        self.assertFalse(event_check(self.contract,'orders',schema)['matched'])
