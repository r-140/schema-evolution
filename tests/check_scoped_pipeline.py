"""Runner checkpoint regression without external services."""
import sys
import tempfile
import unittest
from pathlib import Path
from contextlib import nullcontext
from unittest.mock import patch, MagicMock
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from app import pipeline
from app.impact import plan

class ScopedRun(unittest.TestCase):
    def test_partial_run_does_not_advance_held_cursor(self):
        baseline={'orders':{},'customers':{}}
        impact=plan(baseline,{'orders':{'channel':{}},'customers':{}},'etag',{'schema_policy':'hold'})
        settings={'mapping':{'version':1,'country':False,'amount_field':'amount','amount_divisor':100},
                  'cursor':'2026-01-01 00:00:00.000000'}
        client=MagicMock()
        calls=[]
        with tempfile.TemporaryDirectory() as directory:
            actual_path=Path
            def path(value,*args):
                return actual_path(directory,*args) if str(value)=='/lab/artifacts' else actual_path(value,*args)
            with patch('app.pipeline.scan_catalog',return_value={}), \
                 patch('app.impact.current',return_value=impact), \
                 patch('app.impact.cursors',return_value={'orders':settings['cursor'],'customers':settings['cursor']}), \
                 patch('app.pipeline.get_setting',side_effect=lambda name,*_:settings[name]), \
                 patch('app.pipeline.set_setting',side_effect=lambda name,value:settings.update({name:value})), \
                 patch('app.pipeline.pg',side_effect=lambda:nullcontext(MagicMock())), \
                 patch('app.pipeline.ch',return_value=client), \
                 patch('app.pipeline.collect',return_value=dict.fromkeys(['read_rows','read_bytes','downstream_read_rows','downstream_read_bytes','query_count','validation_read_rows','validation_read_bytes','overhead_read_rows','overhead_read_bytes'],0)), \
                 patch('app.pipeline.Path',side_effect=path), \
                 patch('app.pipeline.command',side_effect=lambda args,*_:calls.append(args)):
                result=pipeline.execute(cutoff='2026-02-01 00:00:00.000000')
        self.assertEqual(result['status'],'partial_success')
        self.assertEqual(settings['entity_cursors']['orders'],'2026-01-01 00:00:00.000000')
        self.assertEqual(settings['entity_cursors']['customers'],'2026-02-01 00:00:00.000000')
        self.assertEqual(settings['cursor'],settings['entity_cursors']['orders'])
        self.assertNotIn('last_gold_success',settings)
        for args in calls:
            self.assertNotIn('stg_orders',args[args.index('--select')+1].split())
            self.assertNotIn('fact_order',args[args.index('--select')+1].split())

if __name__=='__main__': unittest.main()
