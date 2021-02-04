#!/usr/bin/env python3

# MWAA: Trigger an Apache Airflow DAG using SDK
# Author: Gary A. Stafford (February 2021)

import logging

import boto3
import requests

logging.basicConfig(format='[%(asctime)s] %(levelname)s - %(message)s', level=logging.INFO)

mwaa_client = boto3.client('mwaa')

ENVIRONMENT_NAME = 'MyAirflowEnvironment'
DAG_NAME = 'get_env_vars'
CONFIG = '{"foo": "bar"}'


def main():
    response = mwaa_client.create_cli_token(
        Name=ENVIRONMENT_NAME
    )

    logging.info('response: ' + str(response))

    token = response['CliToken']
    url = 'https://{0}/aws_mwaa/cli'.format(response['WebServerHostname'])
    headers = {'Authorization': 'Bearer ' + token, 'Content-Type': 'text/plain'}
    payload = 'trigger_dag {0} --conf {1}'.format(DAG_NAME, CONFIG)

    logging.debug('token: ' + str(token))
    logging.debug('url: ' + str(url))
    logging.debug('headers: ' + str(headers))
    logging.debug('payload: ' + str(payload))

    response = requests.post(url, headers=headers, data=payload)

    logging.info('response: ' + str(response))  # should be <Response [200]>


if __name__ == '__main__':
    main()
