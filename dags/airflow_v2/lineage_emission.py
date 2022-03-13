"""Lineage Emission
This example demonstrates how to emit lineage to DataHub within an Airflow DAG.
Based on reference DataHub DAG:
https://github.com/linkedin/datahub/blob/master/metadata-ingestion/src/datahub_provider/example_dags/lineage_emission_dag.py
"""

import os
from datetime import timedelta

import datahub.emitter.mce_builder as builder
from airflow import DAG
from airflow.utils.dates import days_ago
from datahub_provider.operators.datahub import DatahubEmitterOperator

# override airflow.cfg file's properties in MWAA
os.environ["AIRFLOW__LINEAGE__BACKEND"] = "datahub_provider.lineage.datahub.DatahubLineageBackend"
os.environ["AIRFLOW__LINEAGE__DATAHUB_KWARGS"] = '{"datahub_conn_id": "datahub_rest","cluster": "dev","capture_ownership_info": true,"capture_tags_info": true,"graceful_exceptions": true }'

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
        "datahub_lineage_emission_example",
        default_args=default_args,
        description="An example DAG demonstrating lineage emission within an Airflow DAG.",
        schedule_interval=timedelta(days=1),
        start_date=days_ago(2),
        catchup=False,
) as dag:
    emit_lineage_task = DatahubEmitterOperator(
        task_id="emit_lineage",
        datahub_conn_id="datahub_rest",
        mces=[
            builder.make_lineage_mce(
                upstream_urns=[
                    builder.make_dataset_urn("redshift", "mydb.schema.tableA"),
                    builder.make_dataset_urn("redshift", "mydb.schema.tableB"),
                ],
                downstream_urn=builder.make_dataset_urn(
                    "redshift", "mydb.schema.tableC"
                ),
            )
        ],
    )

    emit_lineage_task
