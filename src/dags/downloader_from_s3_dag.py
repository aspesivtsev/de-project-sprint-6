from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from pendulum import datetime

import boto3
import logging
from connection_info import *

bucket = 'sprint6'
files = [
    'users.csv',
    'groups.csv',
    'dialogs.csv',
    'group_log.csv',
]

bash_command_tmpl = """
head -n 10 {{ params.files | join(' ') }}
"""


def download_file_from_s3(bucket_name, filename):
    s = boto3.session.Session()
    s3 = s.client(
        service_name='s3',
        endpoint_url='https://storage.yandexcloud.net',
        aws_access_key_id=Variable.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=Variable.get('AWS_SECRET_ACCESS_KEY'),
    )

    logging.info(f'Downloading {filename} from s3 is in process')
    s3.download_file(Bucket=bucket_name, Key=filename, Filename='/data/' + filename)
    logging.info(f'Successfully downloaded to /data/{filename}!')


dag = DAG(
    dag_id='sprint6_dag_download_data',
    start_date=datetime(2023, 9, 25),
    catchup=False,
    schedule_interval=None,
)

with dag:
    bash_op = BashOperator(
        task_id='print_10_lines_of_each_file',
        bash_command=bash_command_tmpl,
        params={'files': ['/data/' + f for f in files]}
    )

    for f in files:
        task = PythonOperator(
            task_id=f'download_{f}',
            python_callable=download_file_from_s3,
            op_args=(bucket, f),
        )

        task >> bash_op
