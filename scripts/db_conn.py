# This file contains code for connecting to a Postgresql database 
"""
About connections in Airflow
-----------------------------
Airflow is often used to pull and push data into other systems, and so it has a first-class Connection concept for storing credentials that are used to talk to external systems.
Connection is essentially a set of parameters - such as username, password and hostname along with the type of system that it connects to and a unique name called conn_id. Connections can be managed using either CLI or GUI

About Hooks in Airflow
------------------------
A Hook is a high-level interface to an external platform that lets you quickly and easily talk to them without having to write low-level code that hits their API or uses special libraries. They're also often the building blocks that Operators are built out of. They integrate with Connections to gather credentials, and many have a default conn_id; for example, the PostgresHook automatically looks for the Connection with a conn_id of postgres_default if you don't pass one in.
"""

# There are multiple ways to open database connection
# ?1. Using sqlalchemy & psycopg2 to connect to postgresql database
#* Traditional approach
# * the drawback of this method is that we need to maintain a separate env file to maintain security of user credentials
# from sqlalchemy import create_engine 
# import os
# from dotenv import load_dotenv,find_dotenv

# load_dotenv(find_dotenv())

# DB_USER = os.environ.get("DB_USER")
# DB_PWD = os.environ.get("DB_PWD")
# HOST_IP = os.environ.get("HOST_IP")
# DB_NAME = os.environ.get("DB_NAME")
# DB_PORT = os.environ.get("DB_PORT")
# DB_SCHEMA = os.environ.get("DB_SCHEMA")

# def get_db_conn():
#     engine = create_engine("postgres+psycopg2://{user}:{pw}@{ip}:{port}/{db}".format(user=DB_USER, pw=DB_PWD, ip=HOST_IP, db=DB_NAME, port=DB_PORT),
#     connect_args = {'options': '-csearch_path={}'.format(DB_SCHEMA)})
#     conn = engine.connect()
#     return conn 

# 2. Using Airflow concepts
from airflow.hooks.postgres_hook import PostgresHook 
def get_pg_hook_conn():
    # for connecting to the connection created in GUI from code
    conn = PostgresHook(postgres_conn_id="pg_etl_demo")
    return conn