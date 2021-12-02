"""test tool for main project"""
import os

import boto3
import psycopg2


def open_connection():
    """open a connection to ps db"""
    try:
        connection = psycopg2.connect(
            host="localhost",
            port=5432,
            database="airflow",
            user="airflow",
            password="airflow")
        return connection
    except psycopg2.DatabaseError as error:
        print(error)
        return None


def close_connection():
    """close connection to ps db"""
    psycopg2.connect(
        host="postgres",
        port=5432,
        database="airflow",
        user="airflow",
        password="airflow").close()


def get_data_from_postgres(id_obj):
    """parse data from the ps db"""
    conn = open_connection()
    cur = conn.cursor()
    cur.execute("""
        SELECT *
        FROM people
        WHERE product_id = (%s) ;
        """, (id_obj,))
    result = cur.fetchone()
    people_id = str(result[0])
    name = result[1]
    address = result[2]
    return people_id, name, address


def get_file_from_s3(id_obj):
    """parse data from s3"""
    s3_client = boto3.client('s3')
    with open("document.bin", "wb") as file:
        s3_client.download_file('test-translated-bucket', id_obj+'.txt', 'document.bin')
        file.close()

    with open("document.bin", "rb") as file2:
        cont = file2.read().decode("ascii")
        people_id = cont.partition('\n')[0]
        name = cont.partition('\n')[2].partition('\n')[0]
        address = cont.partition('\n')[2].partition('\n')[2]
        file2.close()
    os.remove("document.bin")
    return people_id, name, address


def test_transmission():
    """test ps data == s3 data"""
    check_over = False
    id_obj = 1
    while not check_over:
        try:
            postgres_data = get_data_from_postgres(id_obj)
            s3_data = get_file_from_s3(str(id_obj))
            print("\n*")
            print(postgres_data)
            print(s3_data)
            print("*\n")

            assert postgres_data[0] == s3_data[0]

            id_obj = id_obj + 1
        except TypeError:
            check_over = True


print("start")
test_transmission()
