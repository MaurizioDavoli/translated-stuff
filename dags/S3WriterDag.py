from datetime import datetime, timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.python_operator import PythonOperator
from airflow.models import Variable

import boto3
from botocore.exceptions import ClientError

import psycopg2

BUCKET_NAME = 'test-translated-bucket'

def upload_file(file_path,file_name):
    s3_client = boto3.client('s3')
    return s3_client.upload_file(file_path,BUCKET_NAME,file_name)

def open_connection():
    try:
        connection  = psycopg2.connect(
            host="postgres",
            port=5432,
            database="airflow",
            user="airflow",
            password="airflow")
        return connection
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
        return None

def close_connection():
    psycopg2.connect(
        host="postgres",
        port=5432,
        database="airflow",
        user="airflow",
        password="airflow").close() 

#get the latest id from the local db
def get_latest_id_ps():
    conn = open_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT max(product_id) FROM people;
        """)
    id = cur.fetchone()[0]
    return id

def get_object_file(id):
    conn = open_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT *
        FROM people
        WHERE product_id = (%s) ;
        """, (id,))

    result = cur.fetchone()
    people_id = str(result[0])
    name =  result[1]
    address = result[2]
    file = open(people_id+".txt", "wb")
    file.write((name+"\n"+address).encode('ascii'))
    file.close()
    return file
    


    
#get the latest id from the s3 bucket 
def get_latest_id(**kwargs):
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(BUCKET_NAME)
    obj_set = bucket.objects.all()
    latest_id = 0
    for obj in obj_set:
        id = int(obj.key[:-4])
        if id > latest_id:
            latest_id = id
    kwargs['ti'].xcom_push(key='latest-id', value=latest_id)
    return latest_id


def tranfer_data(**kwargs):
    id = kwargs['ti'].xcom_pull(key='latest-id')+1
    latest_id = get_latest_id_ps()
    
    while (id <= latest_id): 
        to_upload = get_object_file(str(id))
        upload_file(to_upload.name,to_upload.name)
        id = id+1
    

    

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'retries': 1,
    'retry_delay': timedelta(seconds=15)
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

    t1 = PythonOperator(
        task_id='get-latest-id',
        python_callable = get_latest_id
    )
    t2 = PythonOperator(
        task_id='read-write',
        python_callable = tranfer_data
    )

t1 >> t2
