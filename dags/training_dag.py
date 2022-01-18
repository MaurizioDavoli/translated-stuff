"""dag that retrain the classification model once a week with new mail/offer"""
import datetime
from datetime import timedelta

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 5,
    'retry_delay': timedelta(seconds=5)
}

schedule_interval = '0 10 1 * *'


def retrain():
    pass


with DAG(
        'training',
        default_args=default_args,
        description='retrain model',
        start_date=datetime.datetime(2022, 2, 1),
        catchup=False,
        schedule_interval=schedule_interval,
        tags=['Translated']
) as dag:
    t1 = PythonOperator(
        task_id='retrain',
        execution_timeout=timedelta(minutes=20),
        python_callable=retrain
    )
