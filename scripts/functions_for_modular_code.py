from init import *

# This file will contain the business logic functions which will be called by PythonOperator in DAG object
# Python functions for extract transform and load 
def extract_fn():
    print(f'Global Variable: {GV}')
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
    