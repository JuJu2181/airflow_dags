
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
"""
About xcom(Cross communication) in Airflow. XCOM allows us to return an object from one of the function called by python operator and later this returned object can be used by another function down in the pipeline
"""

# Python functions for extract transform and load 
def extract_fn():
    print("Extracting data")
    extracted_obj = 10
    #! Important Note: The returned object should be JSON serializable else error occurs
    return extracted_obj
    
# Here ti stands for task instance. To be able to use the return object of previous function, we need to use a parameter called task instance which has xcom_pull function that allows us to access the returned object. Every execution of DAG has its own task instance
def transform_fn(a1,ti):
    # to access the returned object from previous function
    # Here we need to specify the task id of the function that returns the object
    xcom_pull_obj = ti.xcom_pull(task_ids=["EXTRACT"])
    print(f"Type of xcom pull object is {type(xcom_pull_obj)}")
    print(xcom_pull_obj)
    extracted_data = xcom_pull_obj[0]
    print(f"Transforming data: {extracted_data}")
    transformed_obj = {
        'Name': "Transformed Data",
        'Value': extracted_data+a1
    }
    
    # Pandas dataframe is not directly JSON Serializable so if we try returning dataframe we get error
    # data_t = {
    #     'id': [1,2,3],
    #     'name':['a','b','c']
    # }
    # transformed_df = pd.DataFrame(data_t)
    # # to make dataframe json serializable
    # json_data = transformed_df.to_json(orient='records')
    return transformed_obj

def load_fn(p1,p2,ti):
    xcom_pull_obj = ti.xcom_pull(task_ids=['TRANSFORM'])
    print(f"Loading data: {p1} and {p2}")
    transformed_data = xcom_pull_obj[0]
    print(f"Transformed data: {transformed_data['Value']}")
    

# Defining DAG args
def_args = {
    "owner": "airflow",
    "retries": 0,
    "retry_delay": timedelta(minutes=1),
    "start_date": dt(2023,5,7)
}

# Creating DAG object
with DAG("DAG_XCOM_Example",
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
    