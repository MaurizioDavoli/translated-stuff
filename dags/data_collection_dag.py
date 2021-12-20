"""dag that merge datas from front and offer table and store the into local db"""
import os
from datetime import timedelta
import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from services.front_utility import FrontUtility
from services.merge_utility import merge_db_front, validate_elem
from services.postgres_utility import PostgresUtility


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=15)
}


def collect_data():
    front_tool = FrontUtility(os.environ['FRONT_TOKEN'])
    postgres_tool = PostgresUtility(host='postgres',
                                    port=5432,
                                    name='airflow',
                                    user='airflow',
                                    pswd='airflow')
    to_check_conversations = front_tool.get_tagged_yesterday_parsed_conversations()
    merged_list = merge_db_front(to_check_conversations)
    for elem in merged_list:
        if validate_elem(elem):
            print(postgres_tool.test_ins())
            postgres_tool.add_row(elem)
            print(postgres_tool.test_ins())


with DAG(
    'data_collector',
    default_args=default_args,
    description='collect parsed data',
    schedule_interval=timedelta(days=1),
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['Translated']
) as dag:
    t1 = PythonOperator(
        task_id='test_connection',
        execution_timeout=timedelta(seconds=10),
        python_callable=collect_data
    )
