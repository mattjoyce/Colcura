import argparse
import datetime
import json
import yaml

import Database

# TODO: Add support for skipping specific tables in the database.
# This can be done by adding a configuration option in the YAML file for SkipTables,
# which is a comma-separated list of table names to skip during the audit process.

# TODO: Add support for only auditing specific tables in the database.
# This can be done by adding a configuration option in the YAML file for OnlyTables,
# which is a comma-separated list of table names to audit during the audit process.



# Global dictionary to map database types to their corresponding classes
Types = {
    "sqlite": Database.SQLiteDatabase,
    "CSV": Database.CSVDatabase,    
    # Add other database types here as needed
}


def process_database(config, database_name, no_update):
    # Get the database type from the configuration file
    database_type = config[database_name]['type']

    # Get the corresponding class for the database type
    if database_type not in Types:
        raise ValueError(f"Unsupported database type '{database_type}'")
    db_class = Types[database_type]
    db_string= config[database_name]['connection_string']
    db_metadata = config[database_name].get('metadata', [])
    print(db_metadata)

    # Create a new instance of the database class
    db = db_class(db_string, db_metadata)

    # Get the metadata configuration for the database

    # Crawl the database and update the last seen date if necessary
    # if not no_update:
    #     db.update_lastseen()
    # print(f"Auditing {db.connection_string['name']}...")
    results=db.discover()

    # Return the crawled data as a list of UUIDs
    return results



def main():
    parser = argparse.ArgumentParser(description='Database audit tool')
    parser.add_argument('--config', type=str, help='Path to YAML configuration file')
    parser.add_argument('--database', type=str, default='All', help='Name of the database to audit')
    parser.add_argument('--no-update', action='store_true', help='Do not update the last seen date')
    parser.add_argument('--sample-config', action='store_true', help='generate a sample configuration file' )

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
      discovered_data = []
      for database_name in database_names:
          data = process_database(config, database_name, args.no_update)
          discovered_data.extend(data)

          # Output the crawled data as JSON to a file for the current database
          timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
          output_file = f"audit_{database_name}_{timestamp}.json"
          with open(output_file, 'w') as f:
              json.dump(data, f, indent=2)

      print(f"Audit complete. {len(database_names)} databases audited.")



if __name__ == '__main__':
    main()
