import json
import os
from datetime import timedelta

from airflow import DAG
from airflow.contrib.hooks.aws_sns_hook import AwsSnsHook
from airflow.contrib.operators.sns_publish_operator import SnsPublishOperator
from airflow.utils.dates import days_ago

DAG_ID = os.path.basename(__file__).replace('.py', '')

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False
}


def sns_publish(key, bucket_name):
    """
    Send message to an SNS Topic
    """

    hook = AwsSnsHook()
    content_object = hook.get_key(key=key, bucket_name=bucket_name)
    file_content = content_object.get()['Body'].read().decode('utf-8')
    return json.loads(file_content)


"""
 # Sends a test message from Amazon SNS from Amazon MWAA.
"""

with DAG(
        dag_id=DAG_ID,
        description='Send a test message to an SNS Topic',
        default_args=DEFAULT_ARGS,
        dagrun_timeout=timedelta(hours=2),
        start_date=days_ago(1),
        schedule_interval=None,
        tags=['sns']
) as dag:
    dag.doc_md = __doc__
    sns_publish = SnsPublishOperator(
        task_id='publish_sns_message',
        target_arn='{{ var.value.sns_topic }}',
        message='Test message sent to Amazon SNS from Amazon MWAA.',
        aws_conn_id='aws_default',
        subject='Test Message from Amazon MWAA'
    )

    sns_publish
