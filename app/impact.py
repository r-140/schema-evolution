"""Table-scoped holds using the lab's literal dbt ref dependencies."""
import re
from pathlib import Path
from .compatibility import fingerprint

ROOTS = {'customers': 'stg_customers', 'orders': 'stg_orders'}

def graph(project=None):
    project = Path(project or Path(__file__).resolve().parents[1] / 'dbt')
    result = {}
    for path in (project / 'models').rglob('*.sql'):
        result[path.stem] = set(re.findall(r"ref\(\s*['\"]([^'\"]+)['\"]\s*\)", path.read_text()))
    if not set(ROOTS.values()) <= result.keys():
        raise ValueError('Projection dependency graph is incomplete; refusing publication')
    return result

def plan(approved, observed, mode, config, changes=(), dependencies=None):
    dependencies = graph() if dependencies is None else dependencies
    scopes = {}
    for entity, root in ROOTS.items():
        old, new = approved.get(entity, {}), observed.get(entity, {})
        mismatch = fingerprint(old) != fingerprint(new)
        fields = {name for name in old.keys() | new.keys() if old.get(name) != new.get(name)}
        reasons = []
        if mismatch and mode == 'etag' and config.get('schema_policy', 'hold') != 'off':
            reasons.append('etag_mismatch')
        held = bool(reasons and config.get('schema_policy', 'hold') == 'hold')
        for change in changes:
            if change['entity'] != entity: continue
            detail = change.get('details') or {}
            fields.update(c['field'] for c in detail.get('changes', []))
            fields.update(detail.get('invalid_fields', []))
            if change['kind'] in ('breaking', 'event_breaking', 'semantic'):
                held = True
                reasons.append(change['kind'])
        affected = {root}
        while True:
            expanded = affected | {name for name, parents in dependencies.items() if parents & affected}
            if expanded == affected: break
            affected = expanded
        scopes[entity] = {'source_schema': 'sales', 'source_table': entity,
            'changed_fields': sorted(fields), 'reasons': sorted(set(reasons)),
            'held': held, 'affected_models': sorted(affected)}
    held_models = set().union(*(set(s['affected_models']) for s in scopes.values() if s['held']))
    return {'sources': scopes, 'held_models': sorted(held_models),
            'runnable_models': sorted(set(dependencies) - held_models)}

def current(observed=None):
    from .storage import pg, get_setting, catalog
    with pg() as c:
        changes = [dict(zip(('entity', 'kind', 'details'), row)) for row in
            c.execute('SELECT entity,kind,details FROM control.changes WHERE resolved_at IS NULL').fetchall()]
    return plan(get_setting('contract'), observed if observed is not None else catalog(),
                get_setting('mode'), get_setting('etag_config', {}), changes)

def cursors():
    from .storage import get_setting
    legacy = get_setting('cursor', '1970-01-01 00:00:00.000000')
    saved = get_setting('entity_cursors', {})
    return {entity: saved.get(entity, legacy) for entity in ROOTS}
