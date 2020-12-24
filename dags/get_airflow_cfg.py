import os
from datetime import timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

DAG_ID = os.path.basename(__file__).replace('.py', '')

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False
}


def print_airflow_cfg(**kwargs):
    with open(f"{os.getenv('AIRFLOW_HOME')}/airflow.cfg", 'r') as airflow_cfg:
        file_contents = airflow_cfg.read()
        print(f'\n{file_contents}')


with DAG(
        dag_id=DAG_ID,
        default_args=DEFAULT_ARGS,
        description='Print contents of airflow.cfg to logs',
        dagrun_timeout=timedelta(hours=2),
        start_date=days_ago(1),
        schedule_interval='@once',
        tags=['python']
) as dag:
    get_airflow_cfg_operator = PythonOperator(task_id='get_airflow_cfg_task',
                                              python_callable=print_airflow_cfg,
                                              provide_context=True,
                                              dag=dag)

get_airflow_cfg_operator
