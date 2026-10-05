"""At-least-once raw sink. Batch insert completes before Kafka offsets commit."""
import json
import logging
from datetime import datetime, timezone
from confluent_kafka import Consumer
from .storage import ch, get_setting
from .observer import register_event, blocked
import os
from .synchronization import INGEST_LOCK

COLUMNS=['event_id','topic','partition_id','kafka_offset','entity','op','schema_id',
         'source_lsn','source_ts_ms','ingest_at','key_json','envelope','payload']

def to_row(message):
    entity=message.topic().rsplit('.',1)[1]
    raw=message.value()
    event=json.loads(raw) if raw is not None else {}
    payload=event.get('payload') or {}
    schema=event.get('schema') or {}
    sid,allowed=register_event(entity,schema) if schema else ('tombstone',True)
    source=payload.get('source') or {}
    row=[f'{message.topic()}:{message.partition()}:{message.offset()}',message.topic(),message.partition(),message.offset(),
         entity,payload.get('op','tombstone'),sid,int(source.get('lsn') or 0),int(source.get('ts_ms') or 0),
         datetime.now(timezone.utc),message.key().decode() if message.key() else '',
         raw.decode() if raw is not None else 'null',json.dumps(payload,separators=(',',':'))]
    return row,allowed

def loop(stop):
    consumer=Consumer({'bootstrap.servers':os.getenv('KAFKA_BOOTSTRAP','kafka:9092'),
        'group.id':'bronze-raw-v1','auto.offset.reset':'earliest','enable.auto.commit':False,
        'max.poll.interval.ms':3600000})
    consumer.subscribe(['lab.sales.customers','lab.sales.orders'])
    pending=[]
    client=ch()
    try:
        while not stop.is_set():
            messages=pending or consumer.consume(num_messages=500,timeout=1)
            if not messages: continue
            valid=[]
            for message in messages:
                if message.error(): logging.warning('Kafka: %s',message.error())
                else: valid.append(message)
            if not valid: pending=[];continue
            try:
                strict=get_setting('mode')=='strict'
                if strict and blocked():
                    pending=valid;stop.wait(2);continue
                with INGEST_LOCK:
                    rows=[]
                    processed=[]
                    for message in valid:
                        row,allowed=to_row(message)
                        if strict and not allowed: break
                        rows.append(row);processed.append(message)
                    if rows:
                        client.insert('bronze.events',rows,column_names=COLUMNS)
                        # All consumed events for each partition through this offset are durable.
                        latest={}
                        for message in processed: latest[(message.topic(),message.partition())]=message
                        for message in latest.values(): consumer.commit(message=message,asynchronous=False)
                pending=valid[len(processed):]
                if pending: stop.wait(2)
            except Exception:
                # Insert can succeed before a commit failure: duplicates are intentional and deduplicated downstream.
                logging.exception('sink failed; uncommitted batch retried')
                pending=valid;stop.wait(2)
    finally:
        consumer.close()
