import os
import pkgutil
from datetime import datetime

import pkg_resources
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator

DAG_ID = os.path.basename(__file__).replace(".py", "")


def python_info():
    print("============================")
    print("Environment Variables ...")
    print(os.environ)
    print(f"Airflow_home: {os.getenv('AIRFLOW_HOME')}")
    print("============================")

    print()
    print("============================")
    print("Current Working Directory ...")
    print(os.getcwd())
    print("============================")

    print()
    print("============================")
    print("Packages from pkg_resources")
    print("============================")

    installed_packages = sorted(pkg_resources.working_set, key=lambda x: x.key)

    for package in installed_packages:
        print(f"{package.key}=={package.version}")

    print()
    print("============================")
    print("Packages from pkg_list")
    print("============================")

    pkg_list = sorted(list(pkgutil.iter_modules()), key=lambda x: x[1])

    for package in pkg_list:
        if package.ispkg:
            print(f"Package Name: {package[1]}")
            print(f"Package Path: {package[0].path}")
            print(f"is_package: {package.ispkg}")
            print("-----------")

    return "python_info completed ..."


with DAG(
    DAG_ID,
    description="MWAA Environment DAG",
    schedule_interval=None,
    start_date=datetime(2020, 12, 1),
    catchup=False,
) as dag:
    start_task = DummyOperator(task_id="start_task")

    python_task = PythonOperator(task_id="python_task", python_callable=python_info)

    start_task >> python_task
