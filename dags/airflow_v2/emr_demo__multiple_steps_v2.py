import json
import os
from datetime import timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.amazon.aws.operators.emr_add_steps import EmrAddStepsOperator
from airflow.providers.amazon.aws.operators.emr_create_job_flow import (
    EmrCreateJobFlowOperator,
)
from airflow.providers.amazon.aws.sensors.emr_step import EmrStepSensor
from airflow.utils.dates import days_ago

# ************** AIRFLOW VARIABLES **************
bootstrap_bucket = Variable.get("bootstrap_bucket")
emr_ec2_key_pair = Variable.get("emr_ec2_key_pair")
job_flow_role = Variable.get("job_flow_role")
logs_bucket = Variable.get("logs_bucket")
release_label = Variable.get("release_label")
service_role = Variable.get("service_role")
work_bucket = Variable.get("work_bucket")
# ***********************************************

DAG_ID = os.path.basename(__file__).replace(".py", "")

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
}


def get_object(key, bucket_name):
    """
    Load S3 object as JSON
    """

    hook = S3Hook()
    content_object = hook.read_key(key=key, bucket_name=bucket_name)
    return json.loads(content_object)


with DAG(
    dag_id=DAG_ID,
    description="Run multiple Spark jobs with Amazon EMR",
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=2),
    start_date=days_ago(1),
    schedule_interval=None,
    tags=["emr", "spark", "pyspark"],
) as dag:
    begin = DummyOperator(task_id="begin")

    end = DummyOperator(task_id="end")

    cluster_creator = EmrCreateJobFlowOperator(
        task_id="create_job_flow",
        job_flow_overrides=get_object(
            "job_flow_overrides/job_flow_overrides.json", work_bucket
        ),
    )

    step_adder = EmrAddStepsOperator(
        task_id="add_steps",
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
        aws_conn_id="aws_default",
        steps=get_object("emr_steps/emr_steps.json", work_bucket),
    )

    step_checker = EmrStepSensor(
        task_id="watch_step",
        job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[0] }}",
        aws_conn_id="aws_default",
    )

    begin >> cluster_creator >> step_adder >> step_checker >> end
