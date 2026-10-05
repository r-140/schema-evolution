import unittest
from app.compatibility import compare,fingerprint,event_compatible,connect_fields

class Contracts(unittest.TestCase):
    def setUp(self):
        self.old={'id':{'type':'bigint','nullable':False},'amount':{'type':'bigint','nullable':False}}
    def test_added_field_requires_review_but_is_compatible(self):
        result=compare(self.old,{**self.old,'country':{'type':'text','nullable':True}})
        self.assertTrue(result['compatible']);self.assertEqual(result['changes'][0]['kind'],'added')
    def test_rename_is_breaking(self):
        self.assertFalse(compare(self.old,{'id':self.old['id'],'total_amount':self.old['amount']})['compatible'])
    def test_type_change_is_breaking(self):
        new={**self.old,'amount':{'type':'text','nullable':False}}
        self.assertFalse(compare(self.old,new)['compatible'])
    def test_required_becomes_nullable(self):
        self.assertFalse(compare(self.old,{**self.old,'amount':{'type':'bigint','nullable':True}})['compatible'])
    def test_primary_key_change_is_breaking(self):
        old={'id':{'type':'bigint','nullable':False,'primary_key':True}}
        new={'id':{'type':'bigint','nullable':False,'primary_key':False}}
        self.assertFalse(compare(old,new)['compatible'])
    def test_hash_is_order_independent(self):
        self.assertEqual(fingerprint(self.old),fingerprint(dict(reversed(list(self.old.items())))))
    def fields(self):
        return {'id':{'type':'int64','nullable':False},'customer_id':{'type':'int64','nullable':False},
            'amount':{'type':'int64','nullable':False},'currency':{'type':'string','nullable':False},
            'status':{'type':'string','nullable':False},'ordered_at':{'type':'string','nullable':False}}
    def test_timestamptz_is_connect_string(self):
        self.assertTrue(event_compatible(self.fields(),{'amount_field':'amount','amount_type':'bigint'},'orders')[0])
    def test_new_mapping_can_replay_old_amount_name(self):
        self.assertTrue(event_compatible(self.fields(),{'amount_field':'total_amount','amount_type':'bigint'},'orders')[0])
    def test_old_mapping_rejects_rename(self):
        fields=self.fields();fields['total_amount']=fields.pop('amount')
        self.assertFalse(event_compatible(fields,{'amount_field':'amount','amount_type':'bigint'},'orders')[0])
    def test_approved_text_mapping_accepts_old_and_new_types(self):
        fields=self.fields();fields['amount']['type']='string'
        self.assertTrue(event_compatible(fields,{'amount_field':'amount','amount_type':'text'},'orders')[0])
        self.assertTrue(event_compatible(self.fields(),{'amount_field':'amount','amount_type':'text'},'orders')[0])
    def test_unapproved_text_is_rejected(self):
        fields=self.fields();fields['amount']['type']='string'
        self.assertFalse(event_compatible(fields,{'amount_field':'amount','amount_type':'bigint'},'orders')[0])
    def test_before_schema_available_for_deletes(self):
        schema={'fields':[{'field':'before','fields':[{'field':'id','type':'int64','optional':False}]}]}
        self.assertEqual(connect_fields(schema),{'id':{'type':'int64','nullable':False}})

if __name__=='__main__': unittest.main()
