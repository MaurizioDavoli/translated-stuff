"""tool to get simple the operation with a postgress db"""
import logging


class PostgresUtility:

    connection = None

    def __init__(self, connection):
        self.connection = connection
        pass

    def execute_query(self, query, params=None, request_type='GET'):
        """:return the full response of a query sent to the db"""
        response = None
        try:
            cnx = self.connection
            cursor = cnx.cursor()
            cursor.execute(query, params)
            if request_type == 'GET':
                response = cursor.fetchall()
            if request_type == 'POST':
                cnx.commit()
            cursor.close()
        except Exception as error:
            logging.error(error)
        return response

    def add_row(self, table, element):
        """add a row to raw_training_data table"""
        var_string = ("%s," * len(element))[:-1]
        query_string = "INSERT INTO "+table+" VALUES (%s);" % var_string
        self.execute_query(query_string, element, 'POST')

    def get_last_n_email(self, numeber_of_email):
        query = "SELECT * FROM raw_training_data ORDER BY id DESC LIMIT %s"
        return self.execute_query(query, numeber_of_email)
