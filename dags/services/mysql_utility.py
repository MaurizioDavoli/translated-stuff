"""tool to get simple operation with the staging db"""
import mysql.connector
from mysql.connector.errors import InterfaceError

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
    response = ["empty", ]
    try:
        cnx = _get_connection()
        cursor = cnx.cursor()
        cursor.execute(query, params)
        response = cursor.fetchall()
        cursor.close()
        cnx.close()
    except InterfaceError as error:
        print(error)
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
    def get_offer(creation_date, sender_mail):
        date_or = creation_date
        date_no_sec = creation_date.replace(second=0)
        query = "SELECT * " \
                "FROM customers_offers_stats " \
                "WHERE requester_email = %s " \
                "AND  (arrival_datetime = %s OR arrival_datetime = %s)"
        params = (sender_mail, date_or, date_no_sec,)
        return _execute_query(query, params=params)

    def get_parsed_offer(self, creation_date, sender_mail):
        offer = self.get_offer(creation_date, sender_mail)
        parsed_offer = []
        if offer:
            offer = offer[0]
            parsed_offer.append((offer[0],
                                 offer[5],
                                 offer[7],
                                 offer[8],
                                 offer[9],
                                 offer[11],
                                 offer[12],
                                 offer[18],
                                 offer[20]
                                 ))
        return parsed_offer
