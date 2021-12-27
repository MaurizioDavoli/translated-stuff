"""my sql query tool"""
import logging
from mysql.connector.errors import InterfaceError


class MySqlDbWorker:

    connection = None

    def __init__(self, connection):
        self.connection = connection

    def execute_query(self, query, params=None):
        """:return the full response of a query sent to the db"""
        response = ["empty", ]
        try:
            cnx = self.connection
            cursor = cnx.cursor()
            cursor.execute(query, params)
            response = cursor.fetchall()
            cursor.close()
        except InterfaceError as error:
            logging.error(error)
        return response

    def get_offers_between(self, start_date, end_date):
        """:return the offers in a given range
           not in use at the moment, for test scope"""
        query = "SELECT * FROM customers_offers_stats WHERE arrival_datetime BETWEEN %s AND %s"
        params = (start_date, end_date,)
        return self.execute_query(query, params=params)

    def get_offer(self, creation_date, sender_mail):
        """:return a offer in the db if present"""
        date_or = creation_date
        date_no_sec = creation_date.replace(second=0)
        query = "SELECT * " \
                "FROM customers_offers_stats " \
                "WHERE requester_email = %s " \
                "AND  (arrival_datetime = %s OR arrival_datetime = %s)"
        params = (sender_mail, date_or, date_no_sec,)
        return self.execute_query(query, params=params)
