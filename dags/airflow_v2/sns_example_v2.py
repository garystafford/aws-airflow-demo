import os
from datetime import timedelta

from airflow import DAG
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.providers.amazon.aws.operators.sns import SnsPublishOperator
from airflow.utils.dates import days_ago

# ************** AIRFLOW VARIABLES **************
sns_topic = Variable.get("sns_topic")
# ***********************************************

DAG_ID = os.path.basename(__file__).replace(".py", "")

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
}

"""
 # Sends a test message from Amazon SNS from Amazon MWAA.
"""

with DAG(
    dag_id=DAG_ID,
    description="Send a test message to an SNS Topic",
    default_args=DEFAULT_ARGS,
    dagrun_timeout=timedelta(hours=2),
    start_date=days_ago(1),
    schedule_interval=None,
    tags=["sns"],
) as dag:
    begin = DummyOperator(task_id="begin")

    end = DummyOperator(task_id="end")

    sns_publish = SnsPublishOperator(
        task_id="publish_sns_message",
        target_arn=sns_topic,
        message="Test message sent to Amazon SNS from Amazon MWAA.",
        aws_conn_id="aws_default",
        subject="Test Message from Amazon MWAA",
    )

    begin >> sns_publish >> end
