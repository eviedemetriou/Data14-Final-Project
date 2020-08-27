import pyodbc

class ProjectDatabase:
    def __init__(self):
        self.server = '35.179.41.219'
        self.database = 'SpartaGlobal'
        self.username = 'SA'
        self.password = 'Passw0rd2018'
        self.connection_string = "DRIVER={ODBC Driver 17 for SQL Server};"
        self.connection_string += f"SERVER={self.server};"
        self.connection_string += f"DATABASE={self.database};"
        self.connection_string += f"UID={self.username};"
        self.connection_string += f"PWD={self.password}"
        self.sparta = pyodbc.connect(self.connection_string)
        self.cursor = self.sparta.cursor()

    def _sql_query(self, sql_query):
        return self.cursor.execute(sql_query)

    def create_keyless_table(self, table_name, table_dict):
        # Creates a table with no keys
        all_lines = []
        for column in table_dict:
            all_lines.append(f"{column} {table_dict[column][0]} {table_dict[column][1]}")
        query = f"""
                USE SpartaGlobal
                CREATE TABLE {table_name}
                (
                """
        query += ',\n'.join(all_lines)
        query += ');'
        self._sql_query(query)


new = ProjectDatabase()
new.create_keyless_table('Location', {
                                    'LocationID': ['INT', 'NOT NULL'],
                                    'LocationName': ['VARCHAR(25)', 'NOT NULL']
                                     })
