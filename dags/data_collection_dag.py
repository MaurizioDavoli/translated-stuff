"""dag that merge datas from front and offer table and store the into local db"""
import os
from datetime import timedelta
import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.mysql_hook import MySqlHook

from services.front_utility import FrontUtility
from services.merge_utility import merge_db_front, validate_elem
from services.postgres_utility import PostgresUtility
from services.mysql_utility import MySqlDbUtility

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=15)
}


def collect_data():
    front_tool = FrontUtility(os.environ['FRONT_TOKEN'])

    conn = PostgresHook('STAGING_AM_OFFER_CLASSIFIER_DATASET_RAW_DB_CONNECTION').get_conn()
    postgres_tool = PostgresUtility(conn)

    conn = MySqlHook('STAGING_TRANSLATED_DB_CONNECTION').get_conn()
    mysql_tool = MySqlDbUtility(conn)

    to_check_conversations = front_tool.get_tagged_parsed_last_week_conversations()
    for x in to_check_conversations:
        print(x)
    merged_list = merge_db_front(to_check_conversations, mysql_tool)
    for elem in merged_list:
        if validate_elem(elem):
            postgres_tool.add_row(elem)


with DAG(
    'data_collector',
    default_args=default_args,
    description='collect parsed data',
    schedule_interval='0 2 * * *',
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['Translated']
) as dag:
    t1 = PythonOperator(
        task_id='test_connection',
        execution_timeout=timedelta(seconds=10),
        python_callable=collect_data
    )
