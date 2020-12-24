import json
import os
from datetime import timedelta

import boto3
from airflow import DAG
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.operators.emr_create_job_flow_operator import EmrCreateJobFlowOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.models import Variable
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
    'email': ["{{ dag_run.conf['airflow_email'] }}"],
    'email_on_failure': ["{{ dag_run.conf['email_on_failure'] }}"],
    'email_on_retry': ["{{ dag_run.conf['email_on_retry'] }}"],
}


def get_object(object_name):
    """
    Load EMR Steps from a separate JSON-format file
    """

    s3_client = boto3.client('s3')

    data = s3_client.get_object(Bucket=work_bucket, Key=f'jobs/{object_name}')
    json_data = data['Body'].read().decode('utf-8')
    return json.loads(json_data)


with DAG(
        dag_id=DAG_ID,
        description='Run multiple Spark jobs with Amazon EMR',
        default_args=DEFAULT_ARGS,
        dagrun_timeout=timedelta(hours=2),
        start_date=days_ago(1),
        schedule_interval='@once',
        tags=['emr'],
) as dag:
    cluster_creator = EmrCreateJobFlowOperator(
        task_id='create_job_flow',
        job_flow_overrides=get_object('job_flow_overrides.json')
    )

    step_adder = EmrAddStepsOperator(
        task_id='add_steps',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
        aws_conn_id='aws_default',
        steps=get_object('emr_steps.json'),
    )

    step_checker = EmrStepSensor(
        task_id='watch_step',
        job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[0] }}",
        aws_conn_id='aws_default',
    )

    cluster_creator >> step_adder >> step_checker
