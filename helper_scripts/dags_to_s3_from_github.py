#!/usr/bin/env python3

# Clone GitHub repo and upload DAGs and associated configs to S3
# Author: Gary A. Stafford (December 2020)

import logging
import os

import boto3
from botocore.exceptions import ClientError
from git import Repo, exc

logging.basicConfig(format='[%(asctime)s] %(levelname)s - %(message)s', level=logging.INFO)

s3_client = boto3.client('s3')
ssm_client = boto3.client('ssm')


def main():
    params = get_parameters()

    dir_path = os.path.dirname(os.path.realpath(__file__))

    local_path = f'{dir_path}/aws-airflow-demo/'

    # clone repo to tmp location
    github_repo = 'https://github.com/garystafford/aws-airflow-demo'
    branch = 'main'
    clone_repo(github_repo, branch, local_path)

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
    upload_directory(path, bucket_name)


def clone_repo(github_repo, branch, local_path):
    """ Clone GitHub repository locally"""

    try:
        repo = Repo.clone_from(url=github_repo, to_path=local_path, branch='main', depth=1)
        repo.remotes.origin.pull()
        logging.info(f"GitHub repository '{github_repo}', branch '{branch}' cloned to '{local_path}'")
    except exc.CacheError as e:
        logging.error(e)


def upload_directory(path, bucket_name):
    """Uploads files to Amazon S3"""

    for root, dirs, files in os.walk(path):
        for file in files:
            try:
                if file.endswith('.py') or file.endswith('.json'):
                    file_directory = os.path.basename(os.path.dirname(os.path.join(root, file)))
                    key = f'{file_directory}/{file}'
                    s3_client.upload_file(os.path.join(root, file), bucket_name, key)
                    logging.info(f"File '{key}' uploaded to bucket '{bucket_name}' as key '{key}'")
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
