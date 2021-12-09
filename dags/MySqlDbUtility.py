"""tool to get simple operation with the staging db"""
import mysql.connector

USER = None
PSWD = None
HOST = None
NAME = None


def _get_connection():
    cnx = None
    try:
        cnx = mysql.connector.connect(user=USER,
                                      password=PSWD,
                                      host=HOST,
                                      database=NAME)
    except mysql.connector.Error as err:
        print(err)
    return cnx


def _execute_query(query, params=None):
    cnx = _get_connection()
    cursor = cnx.cursor()
    try:
        cursor.execute(query, params)
    except Exception as error:
        print(error)
    response = cursor.fetchall()
    cursor.close()
    cnx.close()
    return response


class MySqlDbUtility:

    def __init__(self, user, pswd, host, name):
        global USER
        USER = user
        global PSWD
        PSWD = pswd
        global HOST
        HOST = host
        global NAME
        NAME = name
        pass

    @staticmethod
    def get_offers_between(start_date, end_date):
        query = "SELECT * FROM customers_offers_stats WHERE arrival_datetime BETWEEN %s AND %s"
        params = (start_date, end_date,)
        return _execute_query(query, params=params)

    @staticmethod
    def get_source():
        query = "SELECT source_channel FROM customers_offers_stats"
        return _execute_query(query)

    @staticmethod
    def get_offer(creation_date):
        query = "SELECT * FROM customers_offers_stats WHERE arrival_datetime = %s"
        params = (creation_date,)
        return _execute_query(query, params=params)
