#* Simple ETL pipeline using DummyOperator of Airflow, just to get familiar with airflow environment
# Imports 
# DAG is needed to initialize DAG object. DAG -> Directed Acyclic Graph
from airflow import DAG 
# In Airflow operators are used to perform various data transformations
# Various operators are available in Airflow like PythonOperator, BashOperator. Here I am using DummyOperator which is simply like no-operation
from airflow.operators.dummy_operator import DummyOperator
# datetime is used to specify start_date of DAG
from datetime import datetime as dt 

# defining default arguments for DAG
default_args = {
    'owner': 'airflow',
    'start_date': dt(2023, 5, 5)
} 

# Defining DAG Object 

with DAG("ETL_Dummy",
        catchup=False,
        default_args=default_args) as dag:
    """
    Here "ETL Dummy" is the name of the ETL DAG 
    catchup=False means that Airflow will not run any backfill for this DAG
    """
    # These are the stages of ETL Pipeline
    start = DummyOperator(task_id="start")
    e = DummyOperator(task_id="extract")
    t = DummyOperator(task_id="transform")
    l = DummyOperator(task_id="load")
    end = DummyOperator(task_id="end")
    
# ETL Pipeline
start >> e >> t >> l >> end
# ELT Pipeline
# start >> e >> l >> t >> end
