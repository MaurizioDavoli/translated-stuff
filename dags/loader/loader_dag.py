"""
this dag delete from the services and dag folder the file that are not in the s3 bucket
after this it load or upload the file that it found in the same bucket
"""
import boto3
from botocore.exceptions import ClientError

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from datetime import timedelta, datetime

from os import walk
import logging


BUCKET_NAME = 'test-translated-bucket'
# BUCKET_NAME = os.environ['AWS_BUCKET']

# these file names are ignored by the dag loader
CONFIG_DAGS_NAME = ['__init__.py', 'loader_dag.py']

PATH_DICT = {'dags': {'local': './dags/', 's3': 'dags/'},
             'services': {'local': './services/', 's3': 'services/'}}


def get_xcoms_list(type_file, kwargs):
    str_list = str(kwargs.xcom_pull(key=type_file+'/file_in_bucket'))
    if str_list != '':
        file_list = str_list.split("', '")
        return file_list
    else:
        return None


def get_s3_file_names(prefix):
    """get the name of the file in the bucket prefix"""
    s3_client = boto3.client('s3', region_name="eu-west-3")
    try:
        obj_set = s3_client.list_objects_v2(Bucket=BUCKET_NAME,
                                            Prefix=prefix)
        s3_dags = []
        for elem in obj_set['Contents']:
            if str(elem['Key']).endswith(".py"):
                s3_dags.append(elem['Key'][len(prefix):])
        return s3_dags
    except ClientError as e:
        logging.error(e)
        return None


def get_local_file_names(prefix):
    """get the name of the file in the local prefix"""
    local_dags = []
    for (dirpath, dirnames, filenames) in walk(prefix):
        for file_name in filenames:
            if file_name.endswith('.py') and file_name not in CONFIG_DAGS_NAME:
                local_dags.append(file_name)
    return local_dags


def delete_utility(prefix_local, kwarg, type_name):
    """delete not found file in the given prefix"""
    local_dags = get_local_file_names(prefix_local)
    s3_elem = get_xcoms_list(type_name, kwarg)
    if s3_elem is None:
        logging.error("SOMENTHING WENT WRONG")
        return
    for file_name in local_dags:
        if file_name not in s3_elem:
            logging.error("DELETING -->"+file_name)
            # system('rm '+prefix_local+file_name)


def load_utility(prefix_s3, prefix_local, kwarg, type_name):
    """load or update the local files in the given prefix"""
    s3_elem = get_xcoms_list(type_name, kwarg)
    if s3_elem is None:
        logging.error("SOMENTHING WENT WRONG")
        return
    for elem in s3_elem:
        with open(prefix_local+elem, 'wb') as file:
            try:
                s3_client = boto3.client('s3', region_name="eu-west-3")
                s3_client.download_fileobj(BUCKET_NAME, prefix_s3+elem, file)
            except ClientError as e:
                if e.response['Error']['Code'] == "404":
                    logging.error("The object does not exist.")
                else:
                    raise
            file.close()


def get_from_s3(**kwargs):
    for elem in PATH_DICT:
        str_list = str(get_s3_file_names(PATH_DICT[elem]['s3']))[2:-2]
        kwargs['ti'].xcom_push(key=elem+'/file_in_bucket', value=str_list)
        get_xcoms_list(elem, kwargs['ti'])


def delete_task(**kwargs):
    """delete not found file"""
    for elem in PATH_DICT:
        delete_utility(PATH_DICT[elem]['local'], kwargs['ti'], elem)


def load_task(**kwargs):
    """load or update the local files"""
    for elem in PATH_DICT:
        load_utility(PATH_DICT[elem]['s3'], PATH_DICT[elem]['local'], kwargs['ti'], elem)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'provide_context': True,
    'retries': 5,
    'retry_delay': timedelta(seconds=5),
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
        task_id='get_from_s3',
        execution_timeout=timedelta(seconds=10),
        python_callable=get_from_s3
    )
    t2 = PythonOperator(
        task_id='s3_delete',
        execution_timeout=timedelta(seconds=10),
        python_callable=delete_task
    )
    t3 = PythonOperator(
        task_id='s3_load',
        execution_timeout=timedelta(seconds=10),
        python_callable=load_task
    )

t1 >> (t2, t3)
