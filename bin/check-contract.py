#!/usr/bin/env python3
import argparse,json,sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from app.compatibility import compare
p=argparse.ArgumentParser();p.add_argument('--expected',required=True);p.add_argument('--observed',required=True)
a=p.parse_args();old=json.load(open(a.expected));new=json.load(open(a.observed))
results={entity:compare(contract,new.get(entity,{})) for entity,contract in old.items()}
print(json.dumps(results,indent=2));sys.exit(0 if all(x['compatible'] for x in results.values()) else 1)
