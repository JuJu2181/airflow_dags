# Simple ETL pipeline implementation using PythonOperator 
# For datetime related operations
from datetime import datetime as dt 
from datetime import timedelta
# For creating DAG object
from airflow import DAG 
# Importing operators from airflow
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator 

# Python functions for extract transform and load 
def extract_fn():
    print("Extracting data")
    return "Extracted Data"
    
def transform_fn(a1):
    print(f"Transforming data: {a1}")
    return "Transformed Data"

def load_fn(p1,p2):
    print(f"Loading data: {p1} and {p2}")
    
#* Note: Instead of using separate functions for extract, transform and load we can also use a single function etl where we write entire code for extract, transform and load. But creating separate functions add modularity in program and makes complex problems simple.

#! Here although we have returned data from one function, we can't directly catch these returned values and use it in another function, for this to happen we need to use the concept of XCOMs

# Defining DAG args
def_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "start_date": dt(2023,5,7)
}

# Creating DAG object
with DAG("DAG_PythonOperator_Example",
        default_args=def_args,
        catchup=False) as dag:
    start = DummyOperator(task_id="START")
    # in python operator we need to specify the python function to be called as callable
    e = PythonOperator(
        task_id="EXTRACT",
        python_callable=extract_fn
    )
    # If the function takes arguments, we specify the arguments as Operator Arguments i-e op_args
    t = PythonOperator(
        task_id="TRANSFORM",
        python_callable=transform_fn,
        op_args=["This is the extracted data passed as argument"]
    )
    l = PythonOperator(
        task_id="LOAD",
        python_callable=load_fn,
        op_args=["Transformed Data 1", "Transformed Data 2"]
    )
    end = DummyOperator(task_id="END")
    
# setting up the ETL pipeline
start >> e >> t  >>  l >> end 
    