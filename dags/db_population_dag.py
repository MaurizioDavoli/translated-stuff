"""tool that add random number off random people
    to the pg db"""
from datetime import datetime, timedelta

from random import randrange

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.python_operator import PythonOperator

# Lib for interact with posgress db
import psycopg2

from faker import Faker


def open_connection():
    """open the connection"""
    try:
        connection  = psycopg2.connect(
            host="postgres",
            port=5432,
            database="airflow",
            user="airflow",
            password="airflow")
        return connection
    except psycopg2.DatabaseError as error:
        print(error)
        return None

def close_connection():
    """close the connection"""
    psycopg2.connect(
        host="postgres",
        port=5432,
        database="airflow",
        user="airflow",
        password="airflow").close()


def add_element():
    """add a random element to the local pg db"""
    conn = open_connection()
    cur = conn.cursor()
    random_number_to_add = randrange(10)
    fake = Faker()
    i=0
    while i < random_number_to_add:
        name = fake.name()
        address = fake.address()
        cur.execute("""
            INSERT INTO people (name, address)
            VALUES ( %s, %s);
            """,
            (name,address ))
        i=i+1
    conn.commit()
    cur.close()
    close_connection()


def test_ins():
    """do not remember why I write this
        to check!!"""
    conn = open_connection()
    cur = conn.cursor()
    test_query = cur.execute("""
        SELECT * FROM people
        """)
    close_connection()
    return str(test_query)


default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'retries': 1,
    'retry_delay': timedelta(seconds=15)
}

with DAG(
    'DB-population',
    default_args=default_args,
    description='popola il db',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['Translated']
) as dag:
    t1 = PythonOperator(
        task_id='work',
        execution_timeout=timedelta(seconds=10),
        python_callable = add_element
    )
    t2 = PythonOperator(
        task_id='test',
        python_callable = test_ins
    )

t1 >> t2
