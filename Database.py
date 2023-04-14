import csv
import logging
import sqlite3
from Metadata import NodeTypeMetadata, CaptureDateMetadata, FindColumnMetadata, FindTableMetadata, MyTag1Metadata, FindAndTagMetadata

class Database:
    def __init__(self, capture_event, logger):
        """
        A base class for defining a database connection, schema discovery, and metadata discovery.
        :param db_config: a dictionary containing the configuration parameters for the database connection and metadata discovery
        :param logger: a logger instance
        """
        self.db_config = capture_event['database_config']
        self.delimiter = self.db_config.get('UUID_DELIMITER', '::')
        self.connection_string = self.db_config["connection_string"]
        logging.info(f"Connection String : {self.db_config['connection_string']}")
        logging.info(f"DB Name : {self.db_config['name']}")
        self.connection = self.connect()
        self.objects = []
        #first add the database container with name
        self.objects.append({"uuid": self.uuid(database=self.db_config['name']) })
             
        # add mandatory metadata classes        
        self.metadata_classes = [NodeTypeMetadata("type", capture_event, logger), 
                                 CaptureDateMetadata("type", capture_event, logger)] 
        
        metadata_list_from_config = self.db_config.get('metadata', "").split(',')
        # get the string containing comma separated names from the yaml config, and split to a list
        
        self.metadata_config = [metadata_name.strip() for metadata_name in metadata_list_from_config]        
        
        # Instantiate metadata classes based on configuration and add them to a list
        for metadata_name in self.metadata_config:
            logging.info(f"Metadata Extension : {metadata_name}")
            metadata_class = globals()[metadata_name + 'Metadata']
            metadata_instance = metadata_class(metadata_name,capture_event,self.logger)
            self.metadata_classes.append(metadata_instance)
    
    def set_metadata(self):
        for obj in self.objects:
            # Add metadata to the object
            for metadata_class in self.metadata_classes:
                metadata = metadata_class.derive_metadata(obj['uuid'])
                if metadata:
                    if isinstance(metadata, list):
                        for item in metadata:
                            obj.update(item)
                    else:
                        obj.update(metadata)


            
    def discover(self):
        """
        Discovers the schema of the database and stores the discovered metadata.
        :return: a list of dictionaries containing the metadata for each column in the database schema.
        """
        database=self.db_config.get('name')
        tables = self.get_tables()
        for table in tables:
            self.objects.append({"uuid": self.uuid(database,table) })
            table_columns = self.get_columns(table)
            for column in table_columns:
                col_type= self.get_type(table, column)
                column_uuid = self.uuid(database, table, column, col_type)
                column_data = {'uuid': column_uuid}


                self.objects.append(column_data)
        return self.objects
    
    def connect(self):
        """
        Connects to the database using the provided connection string.
        :return: a database connection object.
        """
        return None

    def get_tables(self):
        """
        Gets a list of all tables in the database schema.
        :return: a list of strings representing the names of each table.
        """
        pass
    
    def get_columns(self, table):
        """
        Gets a list of all columns in the given table.
        :param table: the name of the table.
        :return: a list of strings representing the names of each column in the table.
        """
        pass
    
    def get_type(self, table, column):
        """
        Gets the type of the given column in the given table.
        :param table: the name of the table.
        :param column: the name of the column.
        :return: a string representing the data type of the column.
        """
        pass

    def status(self):
        """
        Prints the status of the database schema, including all tables and columns.
        """
        tables = self.get_tables()
        for table in tables:
            print(f"Table: {table}")
            columns = self.get_columns(table)
            for column in columns:
                col_type = self.get_type(table, column)
                print(f"\tColumn: {column} ({col_type})")

    def uuid(self, database, table=None, column=None, column_type=None):
        """
        Generates a unique identifier for a database object.
        :param database: the name of the database.
        :param table: (optional) the name of the table.
        :param column: (optional) the name of the column.
        :param column_type: (optional) the data type of the column.
        :return: a string representing a unique identifier for the specified database object.
        """
        if database:
            uuid = database
            if table:
                uuid += self.delimiter + table
                if column and column_type:
                    uuid += self.delimiter + column + self.delimiter + column_type
            return uuid
        else:
            raise ValueError("Invalid UUID format: check parts")



class SQLiteDatabase(Database):
    def __init__(self, capture_event, logger):
        self.logger = logger
        self.capture_event = capture_event
        super().__init__(capture_event, logger)
        # additional initialization code here, specific to SQLiteDatabase
        
    def connect(self):
        self.connection = sqlite3.connect(self.db_config['connection_string'])
        self.cursor = self.connection.cursor()
        self.name = self.db_config['name']  
        logging.info(f"Connection : {self.connection}")
        return self.connection
        
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




class CSVDatabase(Database):
    def __init__(self, csv_file_path, metadata_config):
        self.csv_file_path = csv_file_path
        self.headers = []
        self._load_headers()
        super().__init__(csv_file_path, metadata_config)    

    def _load_headers(self):
        with open(self.csv_file_path, newline='') as csvfile:
            csvreader = csv.reader(csvfile, delimiter=',', quotechar='|')
            self.headers = next(csvreader)  # Read the first row as headers

    def get_tables(self):
        # In the case of a CSV file, we only have one "table"
        return [self.csv_file_path]

    def get_columns(self, table):
        # Return the headers as columns for the CSV "table"
        return self.headers

    def get_type(self, table, column):
        # For CSV files, we don't have specific data types for each column
        # We can return a generic type, such as 'String'
        return 'String'

    def connect(self):
        # In the case of a CSV file, there's no need for a database connection
        return None
