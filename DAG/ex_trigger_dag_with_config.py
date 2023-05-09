from datetime import datetime as dt 
from airflow import DAG 
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import PythonOperator, get_current_context 
import time 


def get_config_params(**kwargs):
    # we can access all context parameters using get_current_context
    context = get_current_context() 
    print(f"Context Value: {context}")
    logical_date = kwargs["logical_data"]
    # Custom parameter is what we passed in airflow ui when triggering dag
    # {"custom_parameter":"This is the way."}
    custom_param = kwargs["dag_run"].conf.get("custom_parameter")
    todays_date = dt.now().date()
    # If both date are same it is part of normal execution else backdate execution. Usually useful for daily scheduling and also for report generation
    if logical_date.date() == todays_date:
        print("Normal execution")
    else: 
        print("Back-dated execution")
        if custom_param is not None:
            print(f"Custom parameter value is: {custom_param}")
    # sleep for 30 seconds
    time.sleep(15)
    
def_args = {"owner":"airflow","retries":0,"start_date":dt(2023,5,9)}
with DAG("DAG_config_params",default_args=def_args,catchup=False) as dag:
    start = DummyOperator(task_id="START")
    config_params = PythonOperator(task_id="DAG_CONFIG_PARAMS",python_callable=get_config_params)
    end = DummyOperator(task_id="END")
    
start >> config_params >> end 