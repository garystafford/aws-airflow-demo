import os
from datetime import timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# expects command, e.g.:
# {
#     "bash_command": "whoami; pwd; df -h;"
# }

DAG_ID = os.path.basename(__file__).replace(".py", "")

DEFAULT_ARGS = {
    "owner": "garystafford",
    "depends_on_past": False,
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
}

with DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    description="Run a dynamic Bash command using dag_run.conf",
    dagrun_timeout=timedelta(hours=2),
    start_date=days_ago(1),
    schedule_interval=None,
    tags=["utilities", "bash"],
) as dag:
    bash_command_operator = BashOperator(
        task_id="run_bash_command", bash_command="{{ dag_run.conf['bash_command'] }}"
    )

bash_command_operator
