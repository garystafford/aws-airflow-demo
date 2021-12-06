import os
from datetime import datetime

from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

DAG_ID = os.path.basename(__file__).replace(".py", "")


# Test function
def python_info():
    airflow_home = os.getenv("AIRFLOW_HOME")
    config_path = f"{airflow_home}/airflow.cfg"
    print(config_path)
    print("============================")

    with open(config_path, "r") as file:
        file_data = file.read()

        print()
        print("============================")
        print("airflow.cfg ...")
        print(file_data)
        print("============================")

    return "airflow_debug completed ..."


with DAG(
    DAG_ID,
    description="Inspect airflow.cfg DAG",
    schedule_interval=None,
    start_date=datetime(2020, 12, 1),
    catchup=False,
) as dag:
    start_task = DummyOperator(task_id="start_task")

    python_task = PythonOperator(task_id="python_task", python_callable=python_info)

    start_task >> python_task
