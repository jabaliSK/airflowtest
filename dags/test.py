from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import os

def print_env_variable():
    print(os.environ)

with DAG(
    dag_id='print_env_var_dag',
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=['example'],
) as dag:
    print_var_task = PythonOperator(
        task_id='print_environment_variable',
        python_callable=print_env_variable,
    )
