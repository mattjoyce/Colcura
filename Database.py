import datetime
import sqlite3
import json
import csv
import logging

class Metadata:
    def __init__(self, name, config, logger=None):
        """
        Metadata superclass for creating metadata classes for different database objects
        :param name: the name of the metadata
        :param config: a dictionary containing configuration parameters
        :param logger: a logger instance
        """
        self.name = name
        self.config = config
        self.logger = logger
        
    def get_metadata(self, uuid):
        """
        Base implementation for metadata retrieval, which returns an empty dictionary.
        :param uuid: unique identifier for the database object
        """
        return {}


class DiscoveryDateMetadata(Metadata):
    def __init__(self, name, config, logger=None):
        """
        Subclass of Metadata that adds the discovery date to the metadata.
        :param name: the name of the metadata
        :param config: a dictionary containing configuration parameters
        :param logger: a logger instance
        """
        super().__init__(name, config, logger)
        
    def get_metadata(self, uuid):
        """
        Adds the discovery date to the metadata.
        :param uuid: unique identifier for the database object
        """
        discovery_date = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        return {'discovery_date': discovery_date}


class MyTag1Metadata(Metadata):
    def __init__(self, name, config, logger=None):
        """
        Subclass of Metadata that adds the 'mytag1' tag to the metadata.
        :param name: the name of the metadata
        :param config: a dictionary containing configuration parameters
        :param logger: a logger instance
        """
        super().__init__(name, config, logger)

    def get_metadata(self, uuid):
        """
        Adds the 'mytag1' tag to the metadata.
        :param uuid: unique identifier for the database object
        """
        return {'tag': 'mytag1'}

class FindTableMetadata(Metadata):
    def __init__(self, name, config, logger=None):
        """
        Subclass of Metadata that adds the 'hot_table' flag to the metadata for tables that match a specified criteria.
        :param name: the name of the metadata
        :param config: a dictionary containing configuration parameters
        :param logger: a logger instance
        """
        super().__init__(name, config, logger)
        
    def get_metadata(self, uuid):
        """
        Adds the 'hot_table' flag to the metadata for tables that match a specified criteria.
        :param uuid: unique identifier for the database object
        """
        db_name, table_name, column_name, data_type = uuid.split(Database.DELIMITER)
        if table_name=="table_0":
            return {"hot_table": True}
        return None

class FindColumnMetadata(Metadata):
    def __init__(self, name, config, logger=None):
        """
        Subclass of Metadata that adds the 'hot_column' flag to the metadata for columns that match a specified criteria.
        :param name: the name of the metadata
        :param config: a dictionary containing configuration parameters
        :param logger: a logger instance
        """
        super().__init__(name, config, logger)
        
    def get_metadata(self, uuid):
        """
        Adds the 'hot_column' flag to the metadata for columns that match a specified criteria.
        :param uuid: unique identifier for the database object
        :return: a dictionary containing the 'hot_column' flag if the criteria is met, otherwise returns None
        """
        db_name, table_name, column_name, data_type = uuid.split(Database.DELIMITER)
        if 'column_0' in column_name:
            return {"hot_column": True}
        return None

# class GPTPIIMetadata:
#     def __init__(self, name):
#         self.name = name
    
#     def get_metadata(self, uuid):
#         table_name, column_name, data_type = uuid.split(Database.DELIMITER)
#         import openai
#         with open("key_openai.txt", "r") as f:
#           openai.api_key = f.read().strip()
#         messages=[{"role":"system", "content":"You job is to find possible PII in database schema"}]
#         prompt=f"""Assess probability (Not, Low, Medium, High) that the following table definition is used to store PII :
#          Table Name : {table_name} 
#          Column Name : {column_name}
#          Data Type : {data_type}
#          Answer using json only, in this format {{"PII":"Answer"}}"""
#         messages=[{"role":"user","content":prompt}]
#         response = openai.ChatCompletion.create(
#           model="gpt-4",  #TODO move to config
#           messages=messages,
#           max_tokens=100,
#           stop=None,
#           temperature=0.0
#         )
#         for choice in response.choices:
#             if "text" in choice:
#                 print(choice.text)
#                 return choice.text
        
#         print(response.choices[0].message.content)
#         return json.loads(response.choices[0].message.content)


class Database:
    DELIMITER='::'
    def __init__(self, db_config, logger):
        """
        A base class for defining a database connection, schema discovery, and metadata discovery.
        :param db_config: a dictionary containing the configuration parameters for the database connection and metadata discovery
        :param logger: a logger instance
        """
        self.connection_string = db_config["connection_string"]
        logging.info(db_config['name'])
        self.connection = self.connect()
        self.metadata_classes = []
        metadata_list = db_config.get('metadata', "").split(',')
        self.metadata_config = [metadata_name.strip() for metadata_name in metadata_list]        

        # Instantiate metadata classes based on configuration and add them to a list
        for metadata_name in self.metadata_config:
            logging.info(metadata_name)
            metadata_class = globals()[metadata_name + 'Metadata']
            metadata_instance = metadata_class(metadata_name,db_config,self.logger)
            self.metadata_classes.append(metadata_instance)
            
    def discover(self):
        """
        Discovers the schema of the database and stores the discovered metadata.
        :return: a list of dictionaries containing the metadata for each column in the database schema.
        """
        columns = []
        tables = self.get_tables()
        for table in tables:
            table_columns = self.get_columns(table)
            for column in table_columns:
                col_type= self.get_type(table, column)
                column_uuid = self.uuid(self.name, table, column, col_type)
                column_data = {'uuid': column_uuid}

                # Add metadata to the column data
                for metadata_obj in self.metadata_classes:
                    metadata = metadata_obj.get_metadata(column_uuid)
                    if(metadata):
                        column_data.update(metadata)
                columns.append(column_data)
        return columns
    
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

    def uuid(self, database, table, column, type):
        """
        Generates a unique identifier for a database object.
        :param database: the name of the database.
        :param table: the name of the table.
        :param column: the name of the column.
        :param type: the data type of the column.
        :return: a string representing a unique identifier for the specified database object.
        """
        return database + self.DELIMITER + table + self.DELIMITER + column + self.DELIMITER


class SQLiteDatabase(Database):
    def __init__(self, db_config, logger):
        self.logger = logger
        self.db_config = db_config
        super().__init__(db_config, logger)
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
