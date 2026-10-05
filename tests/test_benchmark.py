import unittest
from app.benchmark import summarize

class Measurement(unittest.TestCase):
    def test_node_tags_separate_projection_downstream_tests_and_overhead(self):
        rows=[('stg_orders','model',10,100),('sat_order_details','model',20,200),
              ('valid_amount','test',30,300),('connection','connection',1,5)]
        result=summarize(rows)
        self.assertEqual(result['read_bytes'],100)
        self.assertEqual(result['downstream_read_bytes'],200)
        self.assertEqual(result['validation_read_bytes'],300)
        self.assertEqual(result['overhead_read_bytes'],5)
        self.assertEqual(result['query_count'],4)
