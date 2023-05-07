
"""
Normally the dag file should only contain the structure of the pipeline created, and all the remaining business logic i-e python functions should be moved in some other files. This helps to make code clean, simple, and understandable.
"""
# Simple ETL pipeline implementation using PythonOperator 
# For datetime related operations
from datetime import datetime as dt 
from datetime import timedelta
# For creating DAG object
from airflow import DAG 
# Importing operators from airflow
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator 
import pandas as pd
import os 
import sys

# Project folder path
project_folder_path = os.getcwd()+"/scripts"
sys.path.append(project_folder_path)

# Two ways for modularization
# 1. Using os and exec to execute the scripts that contain python function before actually creating and starting the DAG object
# os.chdir(project_folder_path)
# exec(open('./init.py').read())
# exec(open('./functions_for_modular_code.py').read())

# 2. Using import to import module
# from init import *
from functions_for_modular_code import *

# Defining DAG args
def_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "start_date": dt(2023,5,7)
}

# Creating DAG object
with DAG("DAG_XCOM_Modularization_Example",
        default_args=def_args,
        catchup=False) as dag:
    start = DummyOperator(task_id="START")
    # in python operator we need to specify the python function to be called as callable
    e = PythonOperator(
        task_id="EXTRACT",
        python_callable=extract_fn,
        # to make the returned object of this function accessible in another function,we need to set do_xcom_push as true
        do_xcom_push=True 
        #but as this is set is true by default, even if we comment this line it will still work)
    )
    # If the function takes arguments, we specify the arguments as Operator Arguments i-e op_args
    t = PythonOperator(
        task_id="TRANSFORM",
        python_callable=transform_fn,
        op_args=[100],
        do_xcom_push=True
    )
    l = PythonOperator(
        task_id="LOAD",
        python_callable=load_fn,
        op_args=["Transformed Data 1", "Transformed Data 2"]
    )
    end = DummyOperator(task_id="END")
    
# setting up the ETL pipeline
start >> e >> t  >>  l >> end 
    