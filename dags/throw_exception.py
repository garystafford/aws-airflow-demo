import os
from datetime import timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

DAG_ID = os.path.basename(__file__).replace('.py', '')

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ["{{ dag_run.conf['airflow_email'] }}"],
    'email_on_failure': True,
    'email_on_retry': False,
}


def print_hello():
    return 'Hello World'


def throw_exception():
    raise Exception("Goodbye World, an exception has occurred!")


with DAG(
        dag_id=DAG_ID,
        default_args=DEFAULT_ARGS,
        description='Throw an exception',
        dagrun_timeout=timedelta(hours=2),
        start_date=days_ago(1),
        schedule_interval='@once',
        tags=['python']
) as dag:
    hello_operator = PythonOperator(task_id='hello_task',
                                    python_callable=print_hello,
                                    dag=dag)

    exception_operator = PythonOperator(task_id='exception_task',
                                        python_callable=throw_exception,
                                        dag=dag)

hello_operator >> exception_operator
