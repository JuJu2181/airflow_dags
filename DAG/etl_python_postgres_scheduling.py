from datetime import datetime as dt 
from airflow import DAG 
from airflow.operators.dummy_operator import DummyOperator 
from airflow.operators.python import PythonOperator
import os 
import sys 

# project folder path 
project_folder_path = "/mnt/d/Programming/Data Engineering/ETL_Python_Postgres"
sys.path.append(project_folder_path)
# importing functions from this module path
from main import main 

# default arguments 
def_args = {
    "owner": "anish",
    "retries": 0,
    "start_date": dt(2023,5,10)
}

# creating DAG object 
with DAG("DAG_ETL_PYTHON_POSTGRES",default_args=def_args,catchup=False) as dag:
    start = DummyOperator(task_id="START")
    etl = PythonOperator(task_id="ETL",
                        python_callable=main    
                        )
    end= DummyOperator(task_id="END")
    
start >> etl >> end