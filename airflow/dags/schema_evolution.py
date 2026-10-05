from datetime import datetime,timedelta
import os
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator

URL=os.getenv('LAB_URL','http://lab:8090')

def call(path,body=None):
    r=requests.post(URL+path,json=body or {},timeout=3600)
    r.raise_for_status()
    return r.json()

def build(**context):
    return call('/build',{'mode':'incremental'})

def backfill(**context):
    conf=context['dag_run'].conf or {}
    if conf.get('mode','targeted') not in ('targeted','full'):
        raise ValueError('Backfill mode must be targeted or full')
    return call('/build',{'mode':conf.get('mode','targeted'),
                         'from_date':conf.get('from_date',''),'to_date':conf.get('to_date','')})

with DAG('medallion_incremental',start_date=datetime(2025,1,1),schedule='*/5 * * * *',
         catchup=False,max_active_runs=1,is_paused_upon_creation=True,
         default_args={'retries':0,'execution_timeout':timedelta(hours=1)}) as incremental:
    PythonOperator(task_id='validate_project_build_gold',python_callable=build)

with DAG('schema_backfill',start_date=datetime(2025,1,1),schedule=None,catchup=False,
         max_active_runs=1,default_args={'retries':0,'execution_timeout':timedelta(hours=1)}) as replay:
    PythonOperator(task_id='bounded_replay_and_rebuild',python_callable=backfill)
