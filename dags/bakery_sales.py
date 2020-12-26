import os
from datetime import timedelta

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

SPARK_STEPS = [
    {
        'Name': 'Bakery Sales',
        'ActionOnFailure': 'CONTINUE',
        'HadoopJarStep': {
            'Jar': 'command-runner.jar',
            'Args': [
                'spark-submit',
                '--deploy-mode',
                'cluster',
                '--master',
                'yarn',
                '--conf',
                'spark.yarn.submit.waitAppCompletion=true',
                's3a://{{ var.value.work_bucket }}/analyze/bakery_sales_ssm.py'
            ]
        }
    }
]

JOB_FLOW_OVERRIDES = {
    'Name': 'demo-cluster-airflow',
    'ReleaseLabel': '{{ var.value.release_label }}',
    'LogUri': 's3n://{{ var.value.logs_bucket }}',
    'Applications': [
        {
            'Name': 'Spark'
        },
    ],
    'Instances': {
        'InstanceFleets': [
            {
                'Name': 'MASTER',
                'InstanceFleetType': 'MASTER',
                'TargetSpotCapacity': 1,
                'InstanceTypeConfigs': [
                    {
                        'InstanceType': 'm5.xlarge',
                    },
                ]
            },
            {
                'Name': 'CORE',
                'InstanceFleetType': 'CORE',
                'TargetSpotCapacity': 2,
                'InstanceTypeConfigs': [
                    {
                        'InstanceType': 'r5.xlarge',
                    },
                ],
            },
        ],
        'KeepJobFlowAliveWhenNoSteps': False,
        'TerminationProtected': False,
        'Ec2KeyName': '{{ var.value.emr_ec2_key_pair }}',
    },
    'BootstrapActions': [
        {
            'Name': 'string',
            'ScriptBootstrapAction': {
                'Path': 's3://{{ var.value.bootstrap_bucket }}/bootstrap_actions.sh',
            }
        },
    ],
    'Configurations': [
        {
            'Classification': 'spark-hive-site',
            'Properties': {
                'hive.metastore.client.factory.class': 'com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory'
            }
        }

    ],
    'VisibleToAllUsers': True,
    'JobFlowRole': '{{ var.value.job_flow_role }}',
    'ServiceRole': '{{ var.value.service_role }}',
    'EbsRootVolumeSize': 32,
    'StepConcurrencyLevel': 1,
    'Tags': [
        {
            'Key': 'Environment',
            'Value': 'Development'
        },
        {
            'Key': 'Name',
            'Value': 'Airflow EMR Demo Project'
        },
        {
            'Key': 'Owner',
            'Value': 'Data Analytics Team'
        }
    ]
}

with DAG(
        dag_id=DAG_ID,
        description='Analyze Bakery Sales with Amazon EMR',
        default_args=DEFAULT_ARGS,
        dagrun_timeout=timedelta(hours=2),
        start_date=days_ago(1),
        schedule_interval='@once',
        tags=['emr', 'spark', 'pyspark']
) as dag:
    cluster_creator = EmrCreateJobFlowOperator(
        task_id='create_job_flow',
        job_flow_overrides=JOB_FLOW_OVERRIDES
    )

    step_adder = EmrAddStepsOperator(
        task_id='add_steps',
        job_flow_id="{{ task_instance.xcom_pull(task_ids='create_job_flow', key='return_value') }}",
        aws_conn_id='aws_default',
        steps=SPARK_STEPS
    )

    step_checker = EmrStepSensor(
        task_id='watch_step',
        job_flow_id="{{ task_instance.xcom_pull('create_job_flow', key='return_value') }}",
        step_id="{{ task_instance.xcom_pull(task_ids='add_steps', key='return_value')[0] }}",
        aws_conn_id='aws_default'
    )

    cluster_creator >> step_adder >> step_checker
