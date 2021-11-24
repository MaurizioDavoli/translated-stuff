from datetime import datetime, timedelta
from textwrap import dedent

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    # 'email': ['airflow@example.com'],
    # 'email_on_failure': False,
    # 'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'S3-writer',
    default_args=default_args,
    description='bla bla bla bla....',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['Translated'],
) as dag: 

    t1 = BashOperator(
        task_id='read',
        bash_command='read-from-db'
    )

    t2 = BashOperator(
        task_id='test_read',
        bash_command='testttt2'
    )

    t3 = BashOperator(
        task_id='write',
        bash_command='write-to-stre'
    )

    t4 = BashOperator(
        task_id='test_write',
        bash_command='testttt2'
    )

t1 >> t2

t1 >> t3 >> t4


#/var/lib/docker/volumes/translatedstuff_postgres-db-volume/_data