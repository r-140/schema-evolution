"""Consumer-contract comparison, deliberately stricter than JSON parseability."""
import hashlib
import json

def fingerprint(schema):
    return hashlib.sha256(json.dumps(schema, sort_keys=True, separators=(',', ':')).encode()).hexdigest()

def compare(required, observed):
    changes = []
    for name, old in required.items():
        if name not in observed:
            changes.append({'field': name, 'kind': 'removed', 'severity': 'breaking'})
            continue
        new = observed[name]
        if old['type'] != new['type']:
            changes.append({'field': name, 'kind': 'type_changed', 'severity': 'breaking', 'old': old['type'], 'new': new['type']})
        if old.get('primary_key',False) != new.get('primary_key',False):
            changes.append({'field':name,'kind':'primary_key_changed','severity':'breaking'})
        if not old['nullable'] and new['nullable']:
            changes.append({'field': name, 'kind': 'nullable', 'severity': 'breaking'})
    for name in observed.keys() - required.keys():
        changes.append({'field': name, 'kind': 'added', 'severity': 'review'})
    return {'compatible': not any(x['severity'] == 'breaking' for x in changes), 'changes': changes}

def connect_fields(schema):
    fields = schema.get('fields', [])
    after = next((x for x in fields if x.get('field') == 'after'), None)
    before = next((x for x in fields if x.get('field') == 'before'), None)
    return {x['field']: {'type': x['type'], 'nullable': x.get('optional', False)}
            for x in (after or before or {}).get('fields', [])}

def event_compatible(fields, mapping, entity):
    required = {'id': {'int64'}}
    if entity == 'customers':
        required.update(name={'string'}, segment={'string'})
    else:
        required.update(customer_id={'int64'}, currency={'string'}, status={'string'}, ordered_at={'string'})
        amount_names = [mapping['amount_field']]
        if mapping['amount_field'] != 'amount': amount_names.append('amount')
        amount_types = {'int64', 'string'} if mapping['amount_type'] == 'text' else {'int64'}
        if not any(name in fields and fields[name]['type'] in amount_types and not fields[name]['nullable'] for name in amount_names):
            return False, ['amount']
    errors = []
    for name, types in required.items():
        f = fields.get(name)
        if not f or f['type'] not in types or f['nullable']:
            errors.append(name)
    return not errors, errors
