# here implementing airflow DAG using decorators available in Airflow 2.0 
import json 
import pendulum 

# Airflow decorators 
from airflow.decorators import dag, task 

@dag(
    schedule=None,
    start_date=pendulum.datetime(2023,5,12,tz="UTC"),
    catchup=False,
    tags=["example","decorator"]
)
def example_taskflow_api():
    """ 
    ### TaskFlow API Tutorial Documentation
    This is a simple data pipeline example which demonstrates the use of
    the TaskFlow API using three simple tasks for Extract, Transform, and Load.
    Documentation that goes along with the Airflow TaskFlow API tutorial is
    located
    [here](https://airflow.apache.org/docs/apache-airflow/stable/tutorial_taskflow_api.html)
    > Note: This is an Example of DAG documentation
    """
    @task() 
    def extract():
        """
        #### Extract task
        A simple Extract task to get data ready for the rest of the data
        pipeline. In this case, getting data is simulated by reading from a
        hardcoded JSON string.
        > This is an example of Task documentation
        """
        data_string = '{"1001": 301.27, "1002": 433.21, "1003": 502.22}'

        order_data_dict = json.loads(data_string)
        return order_data_dict
    
    @task(multiple_outputs=True)
    def transform(order_data_dict: dict):
        """
        #### Transform task
        A simple Transform task which takes in the collection of order data and
        computes the total order value.
        """
        total_order_value = 0

        for value in order_data_dict.values():
            total_order_value += value

        return {"total_order_value": total_order_value}
    
    @task()
    def load(total_order_value: float):
        """
        #### Load task
        A simple Load task which takes in the result of the Transform task and
        instead of saving it to end user review, just prints it out.
        """

        print(f"Total order value is: {total_order_value:.2f}")

# Instead of specifying the ETL flow using >> and << we can directly use it like this and instead of using xcom we can directly pass results between the airflow tasks 
    order_data = extract()
    order_summary = transform(order_data)
    load(order_summary["total_order_value"])

# call the api function
example_taskflow_api()