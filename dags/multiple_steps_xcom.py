import json
import os
from datetime import timedelta

from airflow import DAG
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.hooks.S3_hook import S3Hook
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

# ************** AIRFLOW VARIABLES **************
bootstrap_bucket = Variable.get('bootstrap_bucket')
emr_ec2_key_pair = Variable.get('emr_ec2_key_pair')
job_flow_role = Variable.get('job_flow_role')
logs_bucket = Variable.get('logs_bucket')
release_label = Variable.get('release_label')
service_role = Variable.get('service_role')
work_bucket = Variable.get('work_bucket')
# ***********************************************

DAG_ID = os.path.basename(__file__).replace('.py', '')

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
}


def get_object(key, bucket_name, **kwargs):
    hook = S3Hook()
    content_object = hook.get_key(key=key, bucket_name=bucket_name)
    file_content = content_object.get()['Body'].read().decode('utf-8')
    json_content = json.loads(file_content)
    kwargs['ti'].xcom_push(key='json_content', value=json_content)
    print(json_content)
    return


with DAG(
        dag_id=DAG_ID,
        description='Run multiple Spark jobs with Amazon EMR',
        default_args=DEFAULT_ARGS,
        dagrun_timeout=timedelta(hours=2),
        start_date=days_ago(1),
        schedule_interval='@once',
        tags=['emr'],
) as dag:
    get_job_flow_overrides = PythonOperator(
        task_id='get_job_flow_overrides',
        python_callable=get_object,
        provide_context=True,
        op_kwargs={
            'key': 'jobs/job_flow_overrides.json',
            'bucket_name': '{{ var.value.work_bucket }}'
        },
        aws_conn_id='aws_default'
    )

    get_emr_steps = PythonOperator(
        task_id='get_emr_steps',
        python_callable=get_object,
        provide_context=True,
        op_kwargs={
            'key': 'jobs/emr_steps.json',
            'bucket_name': '{{ var.value.work_bucket }}'
        },
        aws_conn_id='aws_default'
    )

    cluster_creator = EmrCreateJobFlowOperator(
        task_id='create_job_flow',
        job_flow_overrides="{{ task_instance.xcom_pull(task_ids='get_job_flow_overrides', key=json.loads(json_content)) }}"
    )

    step_adder = EmrAddStepsOperator(
        task_id='add_steps',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
        aws_conn_id='aws_default',
        steps="{{ task_instance.xcom_pull(task_ids='get_emr_steps', key=json.loads(json_content)) }}"
    )

    step_checker = EmrStepSensor(
        task_id='watch_step',
        job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[0] }}",
        aws_conn_id='aws_default'
    )

    get_job_flow_overrides >> get_emr_steps >> cluster_creator >> step_adder >> step_checker
