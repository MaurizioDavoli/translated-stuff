"""dag that merge datas from front and offer table and store the into local db"""
import os
from datetime import timedelta
import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from services.front_utility import FrontUtility
from services.mysql_utility import MySqlDbUtility
from services.merge_utility import merge_db_front


default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'retries': 1,
    'retry_delay': timedelta(seconds=15)
}


def test_connection():
    # create a tool for handling front api
    front_tool = FrontUtility(os.environ['FRONT_TOKEN'])
    mysql_tool = MySqlDbUtility(os.environ['MYSQL_DB_USER'],
                                os.environ['MYSQL_DB_PSWD'],
                                os.environ['MYSQL_DB_HOST'],
                                os.environ['MYSQL_DB_NAME'])
    # logic below
    to_check_conversations = front_tool.get_tagged_yesterday_parsed_conversations()
    for front_obj in to_check_conversations:
        db_obj = mysql_tool.get_parsed_offer(front_obj[4], front_obj[5][0][1])
        merged_list = []
        if db_obj:
            merged_list = merge_db_front(db_obj, front_obj)
            # Todo: write amazing list to db
        if merged_list:
            print(merged_list)


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
