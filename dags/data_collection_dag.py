"""dag that merge datas from front and offer table and store the into local db"""
import os
from datetime import timedelta
import datetime
from pytz import timezone
import logging


from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.mysql_hook import MySqlHook

from services.front_utility import FrontUtility
from services.merge_utility import merge_db_front, validate_elem
from services.postgres_utility import PostgresUtility
from services.mysql_utility import MySqlDbUtility
from prepro.preprocessing_utility import preproces_last_loaded

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(seconds=5)
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
    for elem in merged_list:
        postgres_tool.add_row('raw_training_data', elem)
        inserted_elements = inserted_elements + 1
    return inserted_elements


def error_task_t1(context):
    instance = context['task_instance']
    to_save_date = instance.start_date.replace(microsecond=0, second=0).\
        astimezone(timezone('Europe/Rome')).\
        replace(tzinfo=None)

    conn = PostgresHook('STAGING_AM_OFFER_CLASSIFIER_DATASET_RAW_DB_CONNECTION').get_conn()
    postgres_tool = PostgresUtility(conn)
    postgres_tool.add_row('errors_table', [to_save_date, ])
    logging.error('something went wrong but take it easy')


def preproces_data(**kwargs):

    to_copy_lines = [kwargs['ti'].xcom_pull(task_ids='collect_raw_data'), ]

    conn = PostgresHook('STAGING_AM_OFFER_CLASSIFIER_DATASET_RAW_DB_CONNECTION').get_conn()
    postgres_tool = PostgresUtility(conn)

    last_added = postgres_tool.get_last_n_email(to_copy_lines)
    preprocessed_list = preproces_last_loaded(last_added)

    for elem in preprocessed_list:
        postgres_tool.add_row('preprocessed_training_data', elem)


def resolve_errors():
    front_tool = FrontUtility(os.environ['FRONT_TOKEN'])

    conn = PostgresHook('STAGING_AM_OFFER_CLASSIFIER_DATASET_RAW_DB_CONNECTION').get_conn()
    postgres_tool = PostgresUtility(conn)

    conn = MySqlHook('STAGING_TRANSLATED_DB_CONNECTION').get_conn()
    mysql_tool = MySqlDbUtility(conn)

    for date in postgres_tool.get_errors():
        to_check_conversations = front_tool.get_parsed_tagged_one_hour_range_conversation(date[0])
        for row in to_check_conversations:
            print(row)



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
        python_callable=collect_data,
        on_failure_callback=error_task_t1
    )
    t2 = PythonOperator(
        task_id='preproces_data',
        execution_timeout=timedelta(minutes=20),
        python_callable=preproces_data
    )
    t3 = PythonOperator(
        task_id='error_solver',
        execution_timeout=timedelta(minutes=20),
        python_callable=resolve_errors
    )

    t1 >> t2 >> t3
