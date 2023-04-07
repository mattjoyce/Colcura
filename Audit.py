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
    databases = []
    if args.database == 'All':
        for database in config:
            if 'subclass' in config[database]:
                db_class = globals()[config[database]['subclass']]
                db = db_class(config[database])
            else:
                db = Database(config[database])
            databases.append(db)
    else:
        if 'subclass' in config[args.database]:
            db_class = globals()[config[args.database]['subclass']]
            db = db_class(config[args.database])
        else:
            db = Database(config[args.database])
        databases.append(db)

    # Crawl each database and update the last seen date if necessary
    for db in databases:
        if not args.no_update:
            db.update_lastseen()
        print(f"Auditing {db.connection_string['name']}...")
        db.crawl()

    print("Audit complete.")
