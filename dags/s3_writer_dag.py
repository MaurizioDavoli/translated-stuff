"""main project"""
from datetime import datetime, timedelta

# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG

# Operators; we need this to operate!
from airflow.operators.python_operator import PythonOperator

# py library for handle aws
import boto3

# py library for handle postgres dbs
import psycopg2

BUCKET_NAME = 'test-translated-bucket'


# TODO: await the end of the upload
def upload_file(file_path,file_name_key):
    """support method to upload a binary obj to an s3 bucket"""
    s3_client = boto3.client('s3')
    return s3_client.upload_file(file_path,BUCKET_NAME,file_name_key)


def open_connection():
    """open a connection to a postgres bd"""
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
    """close connection with posgres db """
    psycopg2.connect(
        host="postgres",
        port=5432,
        database="airflow",
        user="airflow",
        password="airflow").close()


def get_latest_id_ps():
    """get the latest id from the local db (posgress)"""
    conn = open_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT max(product_id) FROM people;
        """)
    latest_id = cur.fetchone()[0]
    return latest_id


def get_object_file(latest_id):
    """ return a file from the posgres db binary encoded
        binary encoding is preparatory for upload it in s3"""
    conn = open_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT *
        FROM people
        WHERE product_id = (%s) ;
        """, (latest_id,))

    result = cur.fetchone()
    people_id = str(result[0])
    name = result[1]
    address = result[2]
    with open(people_id+".txt", "wb") as file:
        file.write((str(people_id)+"\n"+name+"\n"+address).encode('ascii'))
        file.close()
        return file


# TODO: read the id from file and NOT from file name
def get_latest_id(**kwargs):
    """get the latest id from the s3 bucket"""
    s3_resource = boto3.resource("s3")
    bucket = s3_resource.Bucket(BUCKET_NAME)
    obj_set = bucket.objects.all()
    latest_id = 0
    for obj in obj_set:
        obj_id = int(obj.key[:-4])
        if obj_id > latest_id:
            latest_id = obj_id
    kwargs['ti'].xcom_push(key='latest-id', value=latest_id)
    return latest_id


def tranfer_data(**kwargs):
    """upload to s3 all new elements of the postgres db"""
    obj_id = kwargs['ti'].xcom_pull(key='latest-id')+1
    latest_id = get_latest_id_ps()
    if latest_id is not None:
        while obj_id <= latest_id:
            to_upload = get_object_file(str(obj_id))
            upload_file(to_upload.name,to_upload.name)
            obj_id = obj_id+1


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
