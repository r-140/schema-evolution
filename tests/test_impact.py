import unittest
from app.impact import plan, graph

class ImpactTests(unittest.TestCase):
    def setUp(self):
        self.approved = {'orders': {'id': {'type': 'bigint'}}, 'customers': {}}
        self.observed = {'orders': {'id': {'type': 'bigint'}, 'channel': {'type': 'text'}}, 'customers': {}}
    def test_order_hold_keeps_customer_branch(self):
        p = plan(self.approved,self.observed,'etag',{'schema_policy':'hold'})
        self.assertIn('fact_order',p['held_models'])
        self.assertIn('revenue_by_delivery_channel',p['held_models'])
        self.assertIn('dim_customer',p['runnable_models'])
        self.assertIn('raw_events',p['runnable_models'])
        self.assertEqual(p['sources']['orders']['changed_fields'],['channel'])
    def test_warn_does_not_hold(self):
        p = plan(self.approved,self.observed,'etag',{'schema_policy':'warn'})
        self.assertEqual(p['held_models'],[])
        self.assertEqual(p['sources']['orders']['reasons'],['etag_mismatch'])
    def test_customer_hold_propagates_semantic_fact_dependency(self):
        p = plan(self.approved,self.approved,'hybrid',{},
            [{'entity':'customers','kind':'event_breaking','details':{'invalid_fields':['name']}}])
        self.assertIn('fact_order',p['held_models'])
        self.assertIn('dim_customer',p['held_models'])
        self.assertIn('stg_orders',p['runnable_models'])
        self.assertIn('sat_order_details',p['runnable_models'])
    def test_off_keeps_breaking_safety(self):
        p = plan(self.approved,self.observed,'etag',{'schema_policy':'off'},
            [{'entity':'orders','kind':'breaking','details':{}}])
        self.assertIn('stg_orders',p['held_models'])
    def test_graph_complete(self):
        self.assertEqual(len(graph()),17)
        self.assertIn('dim_customer',graph()['fact_order'])
