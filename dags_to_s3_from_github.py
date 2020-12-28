#!/usr/bin/env python3

# Upload DAGs to S3 from GitHub
# Author: Gary A. Stafford (December 2020)

import logging
import os
import shutil

import boto3
import git
from botocore.exceptions import ClientError

logging.basicConfig(format='[%(asctime)s] %(levelname)s - %(message)s', level=logging.INFO)

s3_client = boto3.client('s3')
ssm_client = boto3.client('ssm')


def main():
    params = get_parameters()

    local_path = '/tmp/aws-airflow-demo'
    delete_directory(local_path)

    clone_repo(local_path)

    # upload dags
    path = f'{local_path}/dags'
    bucket_name = params['airflow_bucket']
    upload_directory(path, bucket_name)

    # upload emr config
    path = f'{local_path}/job_flow_overrides'
    bucket_name = params['work_bucket']
    upload_directory(path, bucket_name)

    # upload emr steps
    path = f'{local_path}/emr_steps'
    bucket_name = params['work_bucket']
    upload_directory(path, bucket_name)


def delete_directory(local_path):
    try:
        shutil.rmtree(local_path)
    except OSError as e:
        logging.error(f'Error: {local_path} : {e.strerror}')


def clone_repo(local_path):
    repo = git.Repo.clone_from('https://github.com/garystafford/aws-airflow-demo',
                               to_path=local_path,
                               branch='main')

    repo.remotes.origin.pull()


def upload_directory(path, bucket_name):
    """Uploads DAGs to Amazon S3"""

    for root, dirs, files in os.walk(path):
        for file in files:
            try:
                if file.endswith('.py') or file.endswith('.json'):
                    file_directory = os.path.basename(os.path.dirname(os.path.join(root, file)))
                    key = f'{file_directory}/{file}'
                    s3_client.upload_file(os.path.join(root, file), bucket_name, key)
                    print(f"File '{key}' uploaded to bucket '{bucket_name}' as key '{key}'")
            except ClientError as e:
                logging.error(e)


def get_parameters():
    """Load parameter values from AWS Systems Manager (SSM) Parameter Store"""

    params = {
        'airflow_bucket': ssm_client.get_parameter(Name='/emr_demo/airflow_bucket')['Parameter']['Value'],
        'work_bucket': ssm_client.get_parameter(Name='/emr_demo/work_bucket')['Parameter']['Value']
    }

    return params


if __name__ == '__main__':
    main()