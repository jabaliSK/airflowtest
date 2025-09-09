from airflow.operators.python import PythonVirtualenvOperator
from datetime import datetime
 
def my_task():
    from pycentral.base import DNACenter  # import inside the task
    print("pycentral imported successfully")
 
with DAG(
    dag_id="test_pycentral",
    start_date=datetime(2023, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
 
    task = PythonVirtualenvOperator(
        task_id="use_pycentral",
        python_callable=my_task,
        requirements=["pycentral"],
        system_site_packages=False,
    )
