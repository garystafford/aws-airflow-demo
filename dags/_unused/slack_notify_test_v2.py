import os
from datetime import timedelta

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.dummy import DummyOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.utils.dates import days_ago

DAG_ID = os.path.basename(__file__).replace(".py", "")

DEFAULT_ARGS = {
    "owner": "garystafford",
    "depends_on_past": False,
    "retries": 0,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "postgres_conn_id": "amazon_redshift_dev"
}


def slack_failure_notification(context):
    slack_msg = """
            :red_circle: Task Failed. 
            *Task*: {task}  
            *Dag*: {dag} 
            *Execution Time*: {exec_date}  
            *Log Url*: {log_url} 
            """.format(
        task=context.get('task_instance').task_id,
        dag=context.get('task_instance').dag_id,
        ti=context.get('task_instance'),
        exec_date=context.get('execution_date'),
        log_url=context.get('task_instance').log_url,
    )
    failed_alert = SlackWebhookOperator(
        task_id='slack_notification',
        http_conn_id='slack_webhook',
        message=slack_msg)

    return failed_alert.execute(context=context)


def slack_success_notification(context):
    slack_msg = """
            :white_check_mark: Task Failed. 
            *Task*: {task}  
            *Dag*: {dag} 
            *Execution Time*: {exec_date}  
            *Log Url*: {log_url} 
            """.format(
        task=context.get('task_instance').task_id,
        dag=context.get('task_instance').dag_id,
        ti=context.get('task_instance'),
        exec_date=context.get('execution_date'),
        log_url=context.get('task_instance').log_url,
    )
    success_alert = SlackWebhookOperator(
        task_id='slack_notification',
        http_conn_id='slack_webhook',
        message=slack_msg)

    return success_alert.execute(context=context)


with DAG(
        dag_id=DAG_ID,
        description="Test Slack notifications",
        default_args=DEFAULT_ARGS,
        dagrun_timeout=timedelta(minutes=5),
        start_date=days_ago(1),
        schedule_interval=None,
        tags=["slack", "test"]
) as dag:
    begin = DummyOperator(
        task_id="begin"
    )

    end = DummyOperator(
        task_id="end"
    )

    slack_failure_notification_test = DummyOperator(
        task_id="slack_failure_notification_test",
        on_success_callback=slack_failure_notification
    )

    slack_success_notification_test = DummyOperator(
        task_id="slack_success_notification_test",
        on_success_callback=slack_success_notification
    )

    chain(
        begin,
        slack_failure_notification_test,
        slack_success_notification_test,
        end
    )
