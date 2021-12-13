import boto3
from botocore.exceptions import ClientError

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from datetime import timedelta, datetime

BUCKET_NAME = 'test-translated-bucket'


def load_dags():
    s3_client = boto3.client('s3', region_name="eu-west-3")
    obj_set = s3_client.list_objects_v2(Bucket=BUCKET_NAME)
    for elem in obj_set['Contents']:
        if str(elem['Key']).endswith(".py"):
            with open('dags/'+elem['Key'], 'wb') as file:
                try:
                    s3_client.download_fileobj(BUCKET_NAME, elem['Key'], file)
                except ClientError as e:
                    if e.response['Error']['Code'] == "404":
                        print("The object does not exist.")
                    else:
                        raise
                file.close()


default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'retries': 1,
    'retry_delay': timedelta(seconds=15)
}

with DAG(
    's3_loader',
    default_args=default_args,
    description='bla bla bla cerco dag bla bla bla',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['Translated']
) as dag:
    t1 = PythonOperator(
        task_id='s3_load',
        execution_timeout=timedelta(seconds=10),
        python_callable=load_dags
    )
