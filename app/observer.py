import json
import logging
import time
import threading
from collections import OrderedDict

_CACHE=OrderedDict()
_CACHE_LOCK=threading.Lock()
_GENERATION=0

def clear_cache():
    global _GENERATION
    with _CACHE_LOCK:
        _CACHE.clear()
        _GENERATION+=1

def remember(sid,allowed,generation):
    with _CACHE_LOCK:
        if generation != _GENERATION: return sid,allowed
        _CACHE[sid]=allowed
        _CACHE.move_to_end(sid)
        while len(_CACHE)>512: _CACHE.popitem(last=False)
    return sid,allowed
from psycopg.types.json import Jsonb
from .compatibility import compare, fingerprint, connect_fields, event_compatible
from .storage import pg, catalog, get_setting

def scan_catalog():
    observed = catalog()
    baseline = get_setting('contract', {})
    with pg() as c:
        for entity, expected in baseline.items():
            actual = observed.get(entity,{})
            diff = compare(expected, actual)
            if diff['changes']:
                c.execute("INSERT INTO control.changes(entity,fingerprint,kind,details) VALUES (%s,%s,%s,%s) ON CONFLICT(entity,fingerprint) DO NOTHING",
                    (entity,fingerprint(actual),'breaking' if not diff['compatible'] else 'additive',Jsonb(diff)))
    return observed

def register_event(entity, schema):
    sid = fingerprint(schema)
    with _CACHE_LOCK:
        if sid in _CACHE: return sid,_CACHE[sid]
        generation=_GENERATION
    with pg() as c:
        r = c.execute('SELECT allowed FROM control.event_schemas WHERE fingerprint=%s',(sid,)).fetchone()
        if r:
            return remember(sid,r[0],generation)
        fields = connect_fields(schema)
        allowed, errors = event_compatible(fields,get_setting('mapping'),entity)
        c.execute('INSERT INTO control.event_schemas(fingerprint,entity,schema_json,allowed) VALUES (%s,%s,%s,%s) ON CONFLICT DO NOTHING',
            (sid,entity,Jsonb(schema),allowed))
        if not allowed:
            c.execute("INSERT INTO control.changes(entity,fingerprint,kind,details) VALUES (%s,%s,'event_breaking',%s) ON CONFLICT DO NOTHING",
                (entity,sid,Jsonb({'invalid_fields':errors,'source':'event_schema'})))
        return remember(sid,allowed,generation)

def blocked():
    with pg() as c:
        return c.execute("SELECT count(*) FROM control.changes WHERE resolved_at IS NULL AND kind IN ('breaking','event_breaking','semantic')").fetchone()[0] > 0

def loop(stop):
    while not stop.is_set():
        try:
            scan_catalog()
        except Exception:
            logging.exception('catalog observer failed')
        stop.wait(5)
