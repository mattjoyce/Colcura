import argparse
import datetime
import json
import yaml

import Database


# Global dictionary to map database types to their corresponding classes
Types = {
    "sqlite": Database.SQLiteDatabase,
    # Add other database types here as needed
}


def process_database(config, database_name, no_update):
    # Get the database type from the configuration file
    database_type = config[database_name]['type']

    # Get the corresponding class for the database type
    if database_type not in Types:
        raise ValueError(f"Unsupported database type '{database_type}'")
    db_class = Types[database_type]

    # Create a new instance of the database class
    db = db_class(config[database_name]['connection_string'])

    # Crawl the database and update the last seen date if necessary
    # if not no_update:
    #     db.update_lastseen()
    # print(f"Auditing {db.connection_string['name']}...")
    results=db.discover()

    # Return the crawled data as a list of UUIDs
    return results



def main():
    parser = argparse.ArgumentParser(description='Database audit tool')
    parser.add_argument('--config', type=str, required=True, help='Path to YAML configuration file')
    parser.add_argument('--database', type=str, default='All', help='Name of the database to audit')
    parser.add_argument('--no-update', action='store_true', help='Do not update the last seen date')
    args = parser.parse_args()

    # Read the YAML configuration file
    with open(args.config, 'r') as f:
        config = yaml.safe_load(f)

    # Audit all databases defined in the configuration file
    discovered_data = []
    if args.database == 'All':
        for database_name in config:
            data = process_database(config, database_name, args.no_update)
            discovered_data.extend(data)
    else:
        data = process_database(config, args.database, args.no_update)
        discovered_data.extend(data)

    # Output the crawled data as JSON to a file
    timestamp = datetime.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')
    output_file = f"audit_{timestamp}.json"
    with open(output_file, 'w') as f:
        json.dump(discovered_data, f, indent=2)

    print(f"Audit complete. {output_file}")


if __name__ == '__main__':
    main()
