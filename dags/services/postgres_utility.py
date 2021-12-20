import psycopg2

USER = None
PSWD = None
HOST = None
NAME = None
PORT = None


def _open_connection():
    """open the connection"""
    try:
        connection = psycopg2.connect(
            host=HOST,
            port=PORT,
            database=NAME,
            user=USER,
            password=PSWD)
        return connection
    except psycopg2.DatabaseError as error:
        print(error)
        return None


class PostgresUtility:

    def __init__(self, host, port, name, user, pswd):
        global USER
        USER = user
        global PSWD
        PSWD = pswd
        global HOST
        HOST = host
        global NAME
        NAME = name
        global PORT
        PORT = port
        pass

    @staticmethod
    def add_row(element):
        """add a row to raw_training_data table"""
        conn = _open_connection()
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO raw_training_data (id_front, sender,subject)
            VALUES ( %s, %s, %s);
            """, (element[0], element[5], element[1]))
        conn.commit()
        cur.close()
        conn.close()

    @staticmethod
    def test_ins():
        """query all from a table"""
        conn = _open_connection()
        cur = conn.cursor()
        cur.execute("""
            SELECT * FROM raw_training_data
            """)
        result = cur.fetchall()
        cur.close()
        conn.close()
        return str(result)



