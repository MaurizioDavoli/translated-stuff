"""tool to get simple the operation with a postgress db"""


class PostgresUtility:

    connection = None

    def __init__(self, connection):
        self.connection = connection
        pass

    def add_row(self, element):
        """add a row to raw_training_data table"""
        conn = self.connection
        cur = conn.cursor()
        var_string = ("%s," * len(element))[:-1]
        query_string = "INSERT INTO raw_training_data VALUES (%s);" % var_string
        cur.execute(query_string, element)
        conn.commit()
        cur.close()

    def test_ins(self):
        """query all from a table"""
        conn = self.connection
        cur = conn.cursor()
        cur.execute("""
            SELECT * FROM raw_training_data
            """)
        result = cur.fetchall()
        cur.close()
        return str(result)
