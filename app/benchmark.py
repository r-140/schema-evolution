def summarize(rows):
    result={'read_rows':0,'read_bytes':0,'downstream_read_rows':0,'downstream_read_bytes':0,
            'validation_read_rows':0,'validation_read_bytes':0,'overhead_read_rows':0,'overhead_read_bytes':0,
            'query_count':len(rows)}
    for name,kind,read_rows,read_bytes in rows:
        if kind=='model' and name.startswith('stg_'): prefix=''
        elif kind=='model': prefix='downstream_'
        elif kind=='test': prefix='validation_'
        else: prefix='overhead_'
        result[prefix+'read_rows']+=read_rows
        result[prefix+'read_bytes']+=read_bytes
    return result

def collect(client,marker):
    client.command('SYSTEM FLUSH LOGS')
    rows=client.query("SELECT extract(query,'node_name=([A-Za-z0-9_]+)'),extract(query,'node_type=([A-Za-z0-9_]+)'),read_rows,read_bytes FROM system.query_log WHERE type='QueryFinish' AND position(query,{marker:String})>0 AND query NOT LIKE '%system.query_log%'",parameters={'marker':marker}).result_rows
    return summarize(rows)
