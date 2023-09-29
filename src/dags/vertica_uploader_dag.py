import logging
import vertica_python

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from pendulum import datetime
from connection_info import connection_info

files = [
    'users',
    'groups',
    'dialogs',
    'group_log'
]


def upload_file(table):
    logging.info('Connecting to Vertica...')
    with vertica_python.connect(
            password=Variable.get('STV2023081266_VERTICA_PASSWORD'),
            **connection_info
    ) as conn:
        curs = conn.cursor()
        insert_stmt = '''
        truncate table STV2023081266__STAGING.{table};
        copy STV2023081266__STAGING.{table}
        from local '/data/{table}.csv'
        delimiter ','
        skip 1
        '''
        logging.info(f'Uploading /data/{table}.csv -> STV2023081266__STAGING.{table}...')
        curs.execute(insert_stmt.format(table=table))
        conn.commit()
        logging.info(f'Data uploaded Correctly!')


dag = DAG(
    dag_id='sprint6_dag_upload_to_staging',
    start_date=datetime(2023, 9, 25),
    catchup=False,
    schedule_interval=None,
)

with dag:
    for f in files:
        t = PythonOperator(
            task_id=f'load_{f}',
            python_callable=upload_file,
            op_args=(f,),
        )