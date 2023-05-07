"""
In the previous example, when we tried directly returning a dataframe from a function while using xcom, we got error saying it was not serializable, this can be solved by using a wrapper function to wrap and call the extract, transform, load functions and instead simply calling the wrapper function in the dag as a single function. This helps us avoiding use of xcom for transferring the values/objects between different functions. The business logic for this implementation is found in scripts/functions_for_modular_code_without_xcom
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
# Add the project folder to system path to import the file as a module
sys.path.append(project_folder_path)
# importing functions from the module file
from functions_for_modular_code_without_xcom import *

# Defining DAG args
def_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "start_date": dt(2023,5,7)
}

# Creating DAG object
with DAG("DAG_XCOM_Modularization_Without_XCOM_Example",
        default_args=def_args,
        catchup=False) as dag:
    start = DummyOperator(task_id="START")
    # Creating a single python operator to call the etl wrapper function
    etl = PythonOperator(
        task_id="ETL",
        python_callable=etl_wrapper,
        op_args=[10,'a','b']
    )

    end = DummyOperator(task_id="END")
    
# setting up the ETL pipeline
start >> etl >> end 
    