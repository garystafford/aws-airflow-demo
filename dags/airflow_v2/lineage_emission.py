"""Lineage Emission

This example demonstrates how to emit lineage to DataHub within an Airflow DAG.
Also, lists contents of contents of airflow.cfg and all env vars.

Based on reference DataHub DAG:
https://github.com/linkedin/datahub/blob/master/metadata-ingestion/src/datahub_provider/example_dags/lineage_emission_dag.py
"""

import os
from datetime import timedelta

import datahub.emitter.mce_builder as builder
from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from datahub_provider.operators.datahub import DatahubEmitterOperator

# override airflow.cfg file's properties in MWAA
os.environ["AIRFLOW__LINEAGE__BACKEND"] = "datahub_provider.lineage.datahub.DatahubLineageBackend"
os.environ["AIRFLOW__LINEAGE__DATAHUB_KWARGS"] = \
    '{"datahub_conn_id":"datahub_rest","cluster":"dev","capture_ownership_info":true,"capture_tags_info":true,"graceful_exceptions":true}'

DAG_ID = os.path.basename(__file__).replace(".py", "")


# print contents of airflow.cfg
def print_airflow_cfg():
    with open(f"{os.getenv('AIRFLOW_HOME')}/airflow.cfg", "r") as airflow_cfg:
        file_contents = airflow_cfg.read()
        print(f"\n{file_contents}")


# print all env vars
def print_env_vars():
    print("\n".join([f"{k}: {v}" for k, v in sorted(os.environ.items())]))


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email": ["jdoe@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "execution_timeout": timedelta(minutes=120),
}

with DAG(
        dag_id=DAG_ID,
        default_args=default_args,
        description="An example DAG demonstrating lineage emission within an Airflow DAG.",
        schedule_interval=None,
        start_date=days_ago(1),
        catchup=False,
        dagrun_timeout=timedelta(minutes=5),
        tags=["datahub demo"],

) as dag:
    emit_lineage_task = DatahubEmitterOperator(
        task_id="emit_lineage",
        datahub_conn_id="datahub_rest",
        mces=[
            builder.make_lineage_mce(
                upstream_urns=[
                    builder.make_dataset_urn("glue", "synthea_patient_datahub.patients"),
                    builder.make_dataset_urn("glue", "synthea_patient_datahub.conditions"),
                ],
                downstream_urn=builder.make_dataset_urn(
                    "glue", "synthea_patient_datahub.sinusitis_glue",
                )
            )
        ],
    )
    get_airflow_cfg_operator = PythonOperator(
        task_id="get_airflow_cfg_task", python_callable=print_airflow_cfg
    )
    get_print_env_vars_operator = PythonOperator(
        task_id="get_print_env_vars_task", python_callable=print_env_vars
    )

chain(
    emit_lineage_task,
    get_airflow_cfg_operator,
    get_print_env_vars_operator
)
