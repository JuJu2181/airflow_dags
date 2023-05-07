from init import *
import pandas as pd

# Python functions for extract transform and load 
def extract_fn():
    print(f'Global Variable: {GV}')
    print("Extracting data")
    extracted_obj = 10
    return extracted_obj
    
def transform_fn(a1,extracted_obj):
    extracted_data = extracted_obj
    transformed_data = extracted_data+100
    
    # Pandas dataframe is not directly JSON Serializable so if we try returning dataframe we get error
    data_t = {
        'id': [1,2,3,4],
        'name':['a','b','c',transformed_data]
    }
    transformed_df = pd.DataFrame(data_t)

    return transformed_df

def load_fn(p1,p2,transformed_obj):
    print(f"Loading data: {p1} and {p2}")
    transformed_data = transformed_obj
    print(f"Transformed data: {transformed_data.head()}")
    
    
# wrapper function for etl which will allow returning dataframes and also avoids use of xcom
def etl_wrapper(a1,t1,t2):
    extracted_obj = extract_fn()
    transformed_obj = transform_fn(a1,extracted_obj)
    loaded_obj = load_fn(t1,t2,transformed_obj)
    