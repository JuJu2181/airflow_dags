from datetime import datetime as dt 
from airflow import DAG 
from airflow.operators.dummy_operator import DummyOperator 
from airflow.operators.python import PythonOperator 
from airflow.models import Variable

"""
Variables can be used to store sensitive information like passwords, api keys which are actually stored in another database, so everytime we retreive the variables in airflow, airflow will create a new connection to the database.
"""

# Here when we do deserialize we will get the actual integer value stored in XYZ variable, else we get string XYZ
xyz = Variable.get("XYZ",deserialize_json=True)
json_obj = Variable.get("DB_CONN_VARS")
# Here we deserialize to convert json string to a dictionary object
dict_obj = Variable.get("DB_CONN_VARS",deserialize_json=True)
pwd = Variable.get("DB_PASSWORD")

def print_airflow_variables():
    print(f"The value of variable xyz is {xyz} and its datatype is {type(xyz)}")
    print(f"The value of variable json_obj is {json_obj} and its datatype is {type(json_obj)}")
    print(f"The value of the variable dict_obj is {dict_obj} and its datatype is {type(dict_obj)}")
    print(f"The value of variable dict_obj['DB_HOST_IP] is {dict_obj['DB_HOST_IP']}")
    print(f"The value of variable pwd is {pwd} and its datatype is {type(pwd)}")
    
def_args = {
    "owner": "airflow",
    "retries": 0,
    "start_date": dt(2023,5,9)
}

with DAG("Airflow_Variables_DAG",default_args=def_args,catchup=False) as dag:
    start = DummyOperator(task_id="START")
    airflow_variables = PythonOperator(task_id="AIRFLOW_VARIABLES",python_callable=print_airflow_variables)
    end = DummyOperator(task_id="END")
    
start >> airflow_variables >> end 