import argparse
import datetime
import json
import logging
import yaml
import datetime
import os

import Database

# TODO: Add support for skipping specific tables in the database.
# This can be done by adding a configuration option in the YAML file for SkipTables,
# which is a comma-separated list of table names to skip during the audit process.

# TODO: Add support for only auditing specific tables in the database.
# This can be done by adding a configuration option in the YAML file for OnlyTables,
# which is a comma-separated list of table names to audit during the audit process.

class Audit:
    def __init__(self, logger):
        self.capture_events = []
        self.objects = []
        self.logger = logger
    
    def add_capture_event(self, timestamp, config, comment=''):
        capture_event = {'time': timestamp, 'config': config, 'comment': comment}
        self.capture_events.append(capture_event)
        self.logger.info(f"Added capture event: {capture_event}")
        return capture_event
    


# Global dictionary to map database types to their corresponding classes
DB_Class_Types = {
    "sqlite": Database.SQLiteDatabase,
    "CSV": Database.CSVDatabase,    
    # Add other database types here as needed
}


def process_database(database_config, logger, no_update):
    """_summary_

    Args:
        database_config (_type_): _description_
        logger (_type_): _description_
        no_update (_type_): _description_

    Raises:
        ValueError: _description_

    Returns:
        _type_: _description_
    """    
    # Get the database type from the configuration
    database_type = database_config['type']
    logging.info(f"Database Type : {database_type}")

    # Get the corresponding class for the database type
    if database_type not in DB_Class_Types:
        raise ValueError(f"Unsupported database type '{database_type}'")

    print(database_config)
    

    db_class = DB_Class_Types[database_type]
    logging.debug(f"DB Class : {db_class}")
    db = db_class(database_config, logger)

    # Crawl the database and update the last seen date if necessary
    # if not no_update:
    #     db.update_lastseen()
    # print(f"Auditing {db.connection_string['name']}...")
    db.discover()
    db.set_metadata()
    
    # Return the crawled data as a list of UUIDs
    return db.objects




def main():        
    # Initialize the root logger
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    parser = argparse.ArgumentParser(description='Database audit tool')
    parser.add_argument('--config', type=str, help='Path to YAML configuration file')
    parser.add_argument('--database', type=str, default='All', help='Name of the database to audit')
    parser.add_argument('--no-update', action='store_true', help='Do not update the last seen date')
    parser.add_argument('--sample-config', action='store_true', help='generate a sample configuration file')
    parser.add_argument('-o', '--output', type=str, help='Name of the output file')
    
    args = parser.parse_args()

    if args.sample_config:
        # Generate a sample configuration file
        sample_config = {
            'SQLite DB 1': {
                'type': 'sqlite',
                'connection_string': 'test1.db',
                'metadata': 'discovery_date, mytag1'
            },
            'SQLite DB 2': {
                'type': 'sqlite',
                'connection_string': 'test2.db',
                'metadata': 'discovery_date, mytest1'
            }
        }
        with open('sample_config.yaml', 'w') as f:
            yaml.dump(sample_config, f)
        print('Sample configuration file generated at sample_config.yaml')
    else:
        # Read the YAML configuration file
        with open(args.config, 'r') as f:
            config = yaml.safe_load(f)

        if args.database == 'All':
            # Get a list of all database names from the config
            database_names = list(config.keys())
        else:
            # Add the specified database name to a list
            database_names = [args.database]

        # Audit all databases defined in the configuration file
        for database_name in database_names:    
            # Initialize the Audit object
            audit = Audit(logger)

            # Get the database configuration
            database_config = config[database_name]
            
            # Add the database name to the configuration
            database_config['name'] = database_name
            
            timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
            # Add the capture event to the Audit object
            json_capture_event=audit.add_capture_event(timestamp,database_config,f"Audit of {database_config['name']}")

            # Get the output file name
            if args.output:
                output_file = args.output
            else:
                output_file = f"audit_{database_name}_{timestamp}.json"

            # Capture the data from the database
            data = process_database(database_config, logger, args.no_update)

            # If the output file exists, this is a subsequent capture
            if os.path.isfile(output_file):
                # Load the previous audit data
                with open(output_file, 'r') as f:
                    previous_audit_data = json.load(f)
                # Set the previous audit data in the Audit object
                audit.capture_events.extend(previous_audit_data.get('capture_events', []))
                audit.objects = previous_audit_data.get('objects', [])
                ###
                ###  perform comparison
                ###
            else:
                audit.objects = data

            # Save the audit data to file
            audit_data = {
                "capture_events": audit.capture_events,
                "objects": audit.objects
            }
            with open(output_file, 'w') as f:
                json.dump(audit_data, f, indent=2)
            logger.info(f"Audit data saved to {output_file}")
                
                




if __name__ == '__main__':
    main()
