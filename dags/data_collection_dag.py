"""dag that merge datas from front and offer table and store the into local db"""
import os
from datetime import timedelta
import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from FrontUtility import FrontUtility
from MySqlDbUtility import MySqlDbUtility


default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'retries': 1,
    'retry_delay': timedelta(seconds=15)
}


def get_front_conversations():
    front_tool = FrontUtility(os.environ['FRONT_TOKEN'])
    yesterday = datetime.datetime.now() - timedelta(days=1)
    yesterday_ok = datetime.datetime(yesterday.year, yesterday.month, yesterday.day)
    to_check_conversations = {}
    try:
        response = front_tool.get_conversations(yesterday_ok, tag='tag_1j5rie')
        to_check_conversations = response['_results']
        print(response['_total'])
    except RecursionError:
        print("API ERROR")
    return to_check_conversations


def look_for_merge_in_db(arrival_date):
    mysql_tool = MySqlDbUtility(os.environ['MYSQL_DB_USER'],
                                os.environ['MYSQL_DB_PSWD'],
                                os.environ['MYSQL_DB_HOST'],
                                os.environ['MYSQL_DB_NAME'])
    return mysql_tool.get_offer(arrival_date)


def test_connection():
    to_check_conversations = get_front_conversations()
    for msg in to_check_conversations:
        print(" ->"+str(msg['created_at'])+" "+msg['id']+msg['subject'])

        arrival_date = datetime.datetime.utcfromtimestamp(msg['created_at']).replace(microsecond=0)
        response = look_for_merge_in_db(arrival_date)
        if response:
            # TODO
            pass


with DAG(
    'data_collector',
    default_args=default_args,
    description='bla bla bla faccio cose bla bla bla',
    schedule_interval=timedelta(days=1),
    start_date=datetime.datetime(2021, 1, 1),
    catchup=False,
    tags=['Translated']
) as dag:
    t1 = PythonOperator(
        task_id='test_connection',
        execution_timeout=timedelta(seconds=10),
        python_callable=test_connection
    )
