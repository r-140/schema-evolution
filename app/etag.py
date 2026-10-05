"""Strong contract validators and exact writer-schema preconditions.
This module has no I/O, so version/compatibility semantics can be tested independently.
"""
import re
from .compatibility import fingerprint,connect_fields

class PreconditionError(ValueError):
    def __init__(self,status,message,current=None):
        super().__init__(message)
        self.status=status
        self.current=current

def contract_etag(contract,mapping):
    # Include semantics and monotonic mapping version, not just physical columns.
    return '"'+fingerprint({'contract':contract,'mapping':mapping})+'"'

def require_match(value,current):
    if value is None: raise PreconditionError(428,'If-Match required',current)
    if not re.fullmatch(r'"[0-9a-f]{64}"',value):
        raise PreconditionError(400,'Demo accepts exactly one quoted strong SHA-256 ETag; no weak tags, wildcard or lists',current)
    if value!=current: raise PreconditionError(412,'Contract changed; fetch, review and retry explicitly',current)

PG_TYPES={'bigint':'int64','text':'string','timestamp with time zone':'string'}

def approved_writer_fields(contract,entity):
    return {name:{'type':PG_TYPES.get(field['type'],field['type']),
                  'nullable':field['nullable']} for name,field in contract[entity].items()}

def schema_etag(entity,fields):
    return '"'+fingerprint({'entity':entity,'fields':fields})+'"'

def event_check(contract,entity,schema):
    expected=schema_etag(entity,approved_writer_fields(contract,entity))
    observed=schema_etag(entity,connect_fields(schema))
    return {'matched':expected==observed,'expected_schema_etag':expected,
            'observed_schema_etag':observed}

def schema_precondition(mode,config,approved,observed):
    enabled=mode=='etag' and config.get('schema_policy','hold')!='off'
    mismatch=enabled and fingerprint(approved)!=fingerprint(observed)
    return {'mismatch':mismatch,'hold':mismatch and config.get('schema_policy','hold')=='hold'}
