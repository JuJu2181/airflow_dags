# Here is an example of taskgroup where we can nest a taskgroup inside taskgroup and it helps for parallel processing using airflow
"""
About TaskGroups and SubDAGs
- A TaskGroup can be used to organize tasks into hierarchical groups in Graph view. It is useful for creating repeating patterns and cutting down visual clutter. Unlike SubDAGs, Taskgroups are purely UI concept so the tasks in taskgroups live on same original DAG and honor all the DAG settings and pool configurations

SubDAG
--------
It is a deprecated concept in which we group a lot of tasks into a single logical unit which can be used across multiple DAGs and helps in parallel processing of tasks. SubDAGs, while serving a similar purpose as TaskGroups, introduces both performance and functional issues due to its implementation.
"""


from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime as dt 
from datetime import timedelta
from airflow.utils.task_group import TaskGroup

def_args = {
    "owner": "airflow",
    "start_date": dt(2023,6,8)
}

with DAG("dag_Task_groups",default_args=def_args) as dag:
    # If we create a dag object like this it will be following a sequential order and it will be simply like orchestrating the workflow
    start = DummyOperator(task_id="START")
    # a = DummyOperator(task_id="Task A")
    # a1 = DummyOperator(task_id="Task A1")
    # b = DummyOperator(task_id="Task B")
    # c = DummyOperator(task_id="Task C")
    # d = DummyOperator(task_id="Task D")
    # e = DummyOperator(task_id="Task E")
    # f = DummyOperator(task_id="Task F")
    # g = DummyOperator(task_id="Task G")
    end = DummyOperator(task_id="END")
    
# sequential flow
# start >> a >> a1 >> b >> c >> d >> e >> f >> g >> end 

    # Creating taskgroup1
    with TaskGroup("A-A1",tooltip="Task Group of A & A1") as gr_1:
        a = DummyOperator(task_id="Task_A")
        a1 = DummyOperator(task_id="Task_A1")
        b = DummyOperator(task_id="Task_B")
        c = DummyOperator(task_id="Task_C")
        
        a >> a1 
        # here it means a1 follows a 
        # Also if we don't specify downstream and upstream for b and c they will by default follow start so, a, b, c will be parallel to each other
        
    # Creating TaskGroup 2
    with TaskGroup("DEFG",tooltip="Nested TaskGroup DEFG") as gr_2:
        d = DummyOperator(task_id="Task_D")
        with TaskGroup("EFG",tooltip="Inner nested task group") as sgr_2:
            e = DummyOperator(task_id="Task_E")
            f = DummyOperator(task_id="Task_F")
            g = DummyOperator(task_id="Task_G")
            e >> f
            e >> g 
            # Here f ang g are parallel and follows e 
        
# Parallel flow using TaskGroups
start >> gr_1 >> gr_2 >> end