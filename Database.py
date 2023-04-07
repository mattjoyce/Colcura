import datetime
import sqlite3

class Metadata:
    def __init__(self, config):
        self.config = config
        
    def get_metadata(self, uuid):
        # This is the base implementation, which returns an empty dictionary
        return {}

class DiscoveryDateMetadata(Metadata):
    def get_metadata(self, uuid):
        # This implementation adds the discovery date to the metadata
        discovery_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        return {'discovery_date': discovery_date}

class MyTag1Metadata(Metadata):
    def get_metadata(self, uuid):
        # This implementation adds the 'mytag1' tag to the metadata
        return {'tag': 'mytag1'}

class Database:
    def __init__(self, connection_string, metadata_config):
        self.connection_string = connection_string
        self.connection = self.connect()
        self.delimiter='::'
        self.metadata_classes = []
        metadata_list = [metadata_name.strip() for metadata_name in metadata_config.split(",")]
        for metadata_name in metadata_list:
            print(metadata_name)
            metadata_class = globals()[metadata_name + 'Metadata']
            metadata_instance = metadata_class(metadata_name)
            self.metadata_classes.append(metadata_instance)
            
    def discover(self):
        # Discover the schema of the database and store the discovered data
        columns = []
        tables = self.get_tables()
        for table in tables:
            table_columns = self.get_columns(table)
            for column in table_columns:
                col_type= self.get_type(table, column)
                column_uuid = self.uuid(table, column, col_type)
                column_data = {'uuid': column_uuid}
                for metadata_obj in self.metadata_classes:
                    metadata = metadata_obj.get_metadata(column_uuid)
                    if(metadata):
                        column_data.update(metadata)
                columns.append(column_data)
        return columns
    
    def connect(self):
        return None

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
        connection = sqlite3.connect(self.connection_string)
        connection.execute('SELECT 1')
        return connection
        
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
