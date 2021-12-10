from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from datetime import timedelta
import datetime

from pprint import pprint
import sys


def get_sys_path():
    pprint(sys.path)

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'retries': 1,
    'retry_delay': timedelta(seconds=15)
}

with DAG(
    'test_here',
    default_args=default_args,
    description='bla bla bla faccio cose bla bla bla',
    schedule_interval=timedelta(days=1),
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['Translated']
) as dag:
    t1 = PythonOperator(
        task_id='sys_path',
        execution_timeout=timedelta(seconds=10),
        python_callable=get_sys_path
    )