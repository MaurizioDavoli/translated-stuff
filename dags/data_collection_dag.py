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
    'retries': 4,
    'retry_delay': timedelta(seconds=15)
}


def collect_data():
    """collect_data takes mail of one day of the last week from front and merge each one with
        the respective offer in the translated staging db. after this merge these datas are parsed
        and writen on a second db, ready to be preprocessed"""
    front_tool = FrontUtility(os.environ['FRONT_TOKEN'])

    conn = PostgresHook('STAGING_AM_OFFER_CLASSIFIER_DATASET_RAW_DB_CONNECTION').get_conn()
    postgres_tool = PostgresUtility(conn)

    conn = MySqlHook('STAGING_TRANSLATED_DB_CONNECTION').get_conn()
    mysql_tool = MySqlDbUtility(conn)

    to_check_conversations = front_tool.get_tagged_parsed_last_week_conversations()
    # TODO: reduce coupling between mysqltool and this dag, by passig to merge_db_front just the connection
    merged_list = merge_db_front(to_check_conversations, mysql_tool)
    inserted_elements = 0
    print(len(merged_list))
    for elem in merged_list:
        postgres_tool.add_row(elem)
        inserted_elements = inserted_elements + 1
    return inserted_elements


def preproces_data(**kwargs):

    to_copy_lines = kwargs['ti'].xcom_pull(task_ids='collect_raw_data')



    # copy data from db to .csf in folder
    # run preprocessing scritp
    # copy output folder content in another table

    pass


with DAG(
        'data_collector',
        default_args=default_args,
        description='collect parsed data',
        start_date=datetime.datetime(2021, 1, 1),
        catchup=False,
        schedule_interval='@hourly',
        tags=['Translated']
) as dag:
    t1 = PythonOperator(
        task_id='collect_raw_data',
        execution_timeout=timedelta(minutes=20),
        python_callable=collect_data
    )
    t2 = PythonOperator(
        task_id='preproces_data',
        execution_timeout=timedelta(minutes=20),
        python_callable=preproces_data
    )

    t1 >> t2
