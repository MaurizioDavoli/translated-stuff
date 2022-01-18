from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.mysql_hook import MySqlHook

from datetime import timedelta
import datetime

from pprint import pprint
import sys
import os

from services.postgres_utility import PostgresUtility
from services.mysql_utility import MySqlDbUtility

def get_sys_path():
    pprint(sys.path)


def get_project_info():
    # stream = os.popen('rm test_remote_creation_dag.py')
    # print(stream.read())
    stream = os.popen('ls -l ./dags')
    print(stream.read())
    stream = os.popen('cat ./dags/test_remote_creation_dag.py')
    print(stream.read())
    stream = os.popen('ls -l')
    print(stream.read())
    stream = os.popen('ls -l ./services')
    print(stream.read())


def test_connection_postgres():
    conn = PostgresHook('LOCAL_POSTGRES').get_conn()
    postgres_tool = PostgresUtility(conn)
    print("a")
    #print(pg_hook)
    print("b")
    print(conn)
    print("c")
    print(postgres_tool.test_ins())
    print("d")
    print("e")


def test_connection_mysql():
    conn = MySqlHook('STAGING_DB_CONNECTION').get_conn()
    mysql_tool = MySqlDbUtility(conn)
    print("a")
    #print(pg_hook)
    print("b")
    print(conn)
    print("c")
    startdate = datetime.datetime(2021, 10, 10)
    enddate = datetime.datetime(2021, 10, 11)
    print(mysql_tool.get_offers_between(startdate, enddate))
    print("d")
    print("e")


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
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
    t2 = PythonOperator(
        task_id='prj_info',
        execution_timeout=timedelta(seconds=10),
        python_callable=get_project_info
    )
    t3 = PythonOperator(
        task_id='connection_test_postgres',
        execution_timeout=timedelta(seconds=10),
        python_callable=test_connection_postgres
    )
    t4 = PythonOperator(
        task_id='connection_test_mysql',
        execution_timeout=timedelta(seconds=10),
        python_callable=test_connection_mysql
    )