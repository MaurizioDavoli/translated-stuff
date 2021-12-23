CONNECTION = None


class PostgresUtility:

    def __init__(self, connection):
        global CONNECTION
        CONNECTION = connection
        pass

    @staticmethod
    def add_row(element):
        """add a row to raw_training_data table"""
        conn = CONNECTION
        cur = conn.cursor()
        var_string = ", ".join("%s" * len(element))
        query_string = "INSERT INTO raw_training_data VALUES (%s);" % var_string
        print(query_string)
        cur.execute(query_string, element)
        conn.commit()
        cur.close()

    @staticmethod
    def test_ins():
        """query all from a table"""
        conn = CONNECTION
        cur = conn.cursor()
        cur.execute("""
            SELECT * FROM raw_training_data
            """)
        result = cur.fetchall()
        cur.close()
        return str(result)
