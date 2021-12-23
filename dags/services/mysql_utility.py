"""tool to get simple operation with the staging db"""
import logging
from mysql.connector.errors import InterfaceError

CONNECTION = None


def _execute_query(query, params=None):
    """:return the full response of a query sent to the db"""
    response = ["empty", ]
    try:
        cnx = CONNECTION
        cursor = cnx.cursor()
        cursor.execute(query, params)
        response = cursor.fetchall()
        cursor.close()
    except InterfaceError as error:
        logging.error(error)
    return response


class MySqlDbUtility:

    def __init__(self, connection):
        global CONNECTION
        CONNECTION = connection
        pass

    @staticmethod
    def get_offers_between(start_date, end_date):
        """:return the offers in a given range
           not in use at the moment, for test scope"""
        query = "SELECT * FROM customers_offers_stats WHERE arrival_datetime BETWEEN %s AND %s"
        params = (start_date, end_date,)
        return _execute_query(query, params=params)

    @staticmethod
    def get_offer(creation_date, sender_mail):
        """:return a offer in the db if present"""
        date_or = creation_date
        date_no_sec = creation_date.replace(second=0)
        query = "SELECT * " \
                "FROM customers_offers_stats " \
                "WHERE requester_email = %s " \
                "AND  (arrival_datetime = %s OR arrival_datetime = %s)"
        params = (sender_mail, date_or, date_no_sec,)
        return _execute_query(query, params=params)

    def get_parsed_offer(self, creation_date, sender_mail):
        """:return offer info in a friendly format"""
        offer = self.get_offer(creation_date, sender_mail)
        parsed_offer = []
        if offer:
            offer = offer[0]
            parsed_offer.append((offer[0],   # id
                                 offer[3],   # id_project
                                 offer[8],   # author_name
                                 offer[1],   # important
                                 offer[2],   # notarization
                                 offer[4],   # id_auto_request
                                 offer[5],   # id_customer
                                 offer[6],   # customer_type
                                 offer[8],   # requester_name
                                 offer[9],   # requester_tel
                                 offer[10],  # lang
                                 offer[11],  # am
                                 offer[13],  # am_fetch_datetime
                                 offer[14],  # response_date
                                 offer[15],  # target_response_date
                                 offer[16],  # sys_response_date
                                 offer[19],  # fax_confirm_request
                                 offer[22],  # fax_confirm_date
                                 offer[23],  # first_offer
                                 offer[24],  # current_offer
                                 offer[25],  # accepted_offer
                                 offer[28]   # suggested_translator
                                 ))
        return parsed_offer
