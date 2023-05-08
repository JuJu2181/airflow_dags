from datetime import datetime as dt 
from datetime import timedelta 
from airflow import DAG 
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator
import pandas as pd 
import os 

# Project folder path
project_folder_path = os.getcwd()+"/scripts"
os.chdir(project_folder_path)
exec(open('./init.py').read())
exec(open('./db_conn.py').read())
exec(open('./etl_db_data.py').read())

def_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "start_date": dt(2023,5,8)
}

with DAG("etl_db_conn",default_args=def_args,catchup=False) as dag:
    start = DummyOperator(task_id="START")
    etl = PythonOperator(task_id="EXTRACT_TRANSFORM_LOAD",python_callable=etl)
    end = DummyOperator(task_id="END")
    
start >> etl >> end