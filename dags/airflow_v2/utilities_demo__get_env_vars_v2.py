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


def print_env_vars():
    keys = str(os.environ.keys()).replace("', '", "'|'").split("|")
    keys.sort()
    for key in keys:
        print(key)


with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    description="Print all environment variables to logs",
    dagrun_timeout=timedelta(hours=2),
    start_date=days_ago(1),
    schedule_interval=None,
    tags=["utilities", "python"],
) as dag:
    get_env_vars_operator = PythonOperator(
        task_id="get_env_vars_task", python_callable=print_env_vars
    )

get_env_vars_operator
