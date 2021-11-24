
from datetime import datetime, timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

# Lib for interact with posgress db
import psycopg2


def db_connection():
    connection = psycopg2.connect(
        host="localhost",
        port="5432",
        database="airflow",
        user="airflow",
        password="airflow")
    connection.close()
    

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'DB-population',
    default_args=default_args,
    description='popola il db',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['Translated'],
) as dag: 

    t1 = PythonOperator(
        depends_on_past=True,
        task_id='connect_to_db',
        execution_timeout=timedelta(seconds=10),
        python_callable = db_connection
    )

    t2 = BashOperator(
        task_id='add_elements',
        bash_command='echo ciaone',
        
    )

    t3 = BashOperator(
        task_id='test',
        bash_command='echo ciaonissimo',
        
    )

t1 >> t2 >> t3



