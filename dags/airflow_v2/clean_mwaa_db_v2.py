import os
from datetime import timedelta

from airflow import settings
from airflow.jobs.base_job import BaseJob
from airflow.models import DAG, DagModel, DagRun, ImportError, Log, SlaMiss, \
    RenderedTaskInstanceFields, TaskFail, TaskInstance, TaskReschedule, Variable, XCom
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False
}

DEFAULT_MAX_AGE_IN_DAYS = 31

OBJECTS_TO_CLEAN = [[BaseJob, BaseJob.latest_heartbeat],
                    [DagModel, DagModel.last_scheduler_run],
                    [DagRun, DagRun.execution_date],
                    [ImportError, ImportError.timestamp],
                    [Log, Log.dttm],
                    [SlaMiss, SlaMiss.execution_date],
                    [RenderedTaskInstanceFields, RenderedTaskInstanceFields.execution_date],
                    [TaskFail, TaskFail.execution_date],
                    [TaskInstance, TaskInstance.execution_date],
                    [TaskReschedule, TaskReschedule.execution_date],
                    [XCom, XCom.execution_date],
                    ]


def cleanup_db_fn(**kwargs):
    session = settings.Session
    print('session: ', str(session))

    oldest_date = days_ago(int(Variable.get('max_metadb_storage_days', default_var=DEFAULT_MAX_AGE_IN_DAYS)))
    print('oldest_date: ', oldest_date)

    for x in OBJECTS_TO_CLEAN:
        query = session.query(x[0]).filter(x[1] <= oldest_date)
        print(str(x[0]), ': ', str(query.all()))
        query.delete(synchronize_session=False)

    session.commit()

    return 'OK'


with DAG(
        dag_id=os.path.basename(__file__).replace('.py', ''),
        description='Cleanup Amazon MWAA metadata database',
        default_args=DEFAULT_ARGS,
        dagrun_timeout=timedelta(hours=2),
        start_date=days_ago(1),
        schedule_interval='@daily',
        tags=['db']
) as dag:
    cleanup_db = PythonOperator(
        task_id='cleanup_db',
        python_callable=cleanup_db_fn,
        provide_context=True
    )
