import os
from datetime import timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

DAG_ID = os.path.basename(__file__).replace(".py", "")

DEFAULT_ARGS = {
    "owner": "garystafford",
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
}


def print_hello():
    return "Hello World."


def throw_exception():
    raise Exception("Goodbye Cruel World! An exception has occurred!")


with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    description="Throw an exception",
    dagrun_timeout=timedelta(hours=2),
    start_date=days_ago(1),
    schedule_interval=None,
    tags=["utilities", "python"],
) as dag:
    hello_operator = PythonOperator(task_id="hello_task", python_callable=print_hello)

    exception_operator = PythonOperator(
        task_id="exception_task", python_callable=throw_exception
    )

hello_operator >> exception_operator
