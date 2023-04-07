import datetime
import sqlite3

class Database:
    def __init__(self, connection_string):
        self.connection_string = connection_string
        self.connection = None
        self.delimiter = "::"
        self.connect()

    def connect(self):
        # Connect to the database using the connection string
        pass
    
    def discover(self):
        # Discover the schema of the database and store the discovered data
        discovery_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

        columns = []
        tables = self.get_tables()
        for table in tables:
            table_columns = self.get_columns(table)
            for column in table_columns:
                col_type = self.get_type(table, column)
                column_uuid = self.uuid(table, column, col_type)
                column_data = {'uuid': column_uuid, 'discovery_date': discovery_date}
                columns.append(column_data)
        return columns


    
    def get_tables(self):
        # Get a list of all tables in the database
        pass
    
    def get_columns(self, table):
        # Get a list of all columns in the given table
        pass
    
    def get_type(self, table, column):
        # Get the type of the given column in the given table
        pass

    def status(self):
        tables = self.get_tables()
        for table in tables:
            print(f"Table: {table}")
            columns = self.get_columns(table)
            for column in columns:
                col_type = self.get_type(table, column)
                print(f"\tColumn: {column} ({col_type})")

    def uuid(self, table, column, type):
        return table + self.delimiter + column + self.delimiter + type
        

class SQLiteDatabase(Database):
    def connect(self):
        # Connect to the SQLite database using the connection string
        self.connection = sqlite3.connect(self.connection_string)
        self.connection.execute('SELECT 1')
        
    def get_tables(self):
        # Get a list of all tables in the database
        tables = []
        cursor = self.connection.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        for row in cursor.fetchall():
            tables.append(row[0])
        return tables
    
    def get_columns(self, table):
        # Get a list of all columns in the given table
        columns = []
        cursor = self.connection.cursor()
        cursor.execute(f"PRAGMA table_info({table})")
        for row in cursor.fetchall():
            columns.append(row[1])
        return columns
    
    def get_type(self, table, column):
        # Get the type of the given column in the given table
        cursor = self.connection.cursor()
        cursor.execute(f"PRAGMA table_info({table})")
        for row in cursor.fetchall():
            if row[1] == column:
                return row[2]
        return None
