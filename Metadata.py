import datetime

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
        
    def derive_metadata(self, uuid):
        """
        Base implementation for metadata retrieval, which returns an empty dictionary.
        :param uuid: unique identifier for the database object
        """
        return {}
    
    def get_uuid_parts(self, uuid):
        """
        Split the UUID into its parts and return them as a tuple. Supports UUIDs containing either three or four parts.
        :param uuid: unique identifier for the database object
        :return: a tuple containing the database name (if present), table name, column name, and data type
        """
        delimeter=self.config.get("UUID_DELIMETER","::")
        parts = uuid.split(delimeter)
        if len(parts) == 1:
            return (parts[0], None, None, None)
        elif len(parts) == 2:
            return (parts[0], parts[1], None, None)
        elif len(parts) == 4:
            return (parts[0], parts[1], parts[2], parts[3])
        else:
            raise ValueError(f"Invalid UUID format {uuid}")

class NodeTypeMetadata(Metadata):
    def __init__(self, name, config, logger=None):
        super().__init__(name, config, logger)

    def derive_metadata(self, uuid):
        db_name, table_name, column_name, data_type = self.get_uuid_parts(uuid)
        if not table_name:
            return {"object_type":"database"}
        elif not column_name:
            return {"object_type":"table"}
        else:
            return {"object_type":"column"}


class DiscoveryDateMetadata(Metadata):
    def __init__(self, name, config, logger=None):
        """
        Subclass of Metadata that adds the discovery date to the metadata.
        :param name: the name of the metadata
        :param config: a dictionary containing configuration parameters
        :param logger: a logger instance
        """
        super().__init__(name, config, logger)
        
    def derive_metadata(self, uuid):
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

    def derive_metadata(self, uuid):
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
        
    def derive_metadata(self, uuid):
        """
        Adds the 'hot_table' flag to the metadata for tables that match a specified criteria.
        :param uuid: unique identifier for the database object
        """
        delimeter=self.config.get('UUID_DELIMETER')
        db_name, table_name, column_name, data_type = uuid.split(delimeter)
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
        
    def derive_metadata(self, uuid):
        """
        Adds the 'hot_column' flag to the metadata for columns that match a specified criteria.
        :param uuid: unique identifier for the database object
        :return: a dictionary containing the 'hot_column' flag if the criteria is met, otherwise returns None
        """
        delimeter=self.config.get('UUID_DELIMETER')

        db_name, table_name, column_name, data_type = uuid.split(delimeter)
        if 'column_0' in column_name:
            return {"hot_column": True}
        return None

# class GPTPIIMetadata:
#     def __init__(self, name):
#         self.name = name
    
#     def derive_metadata(self, uuid):
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
