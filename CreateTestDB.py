import sqlite3
import random
import string

# Set the number of tables and columns
n = 2
k = 5

# Set typical PII column names
pii_names = [
    'ssn', 'dob', 'address', 'phone', 'email',
    'patient_id', 'medical_record_number', 'health_plan_id',
    'diagnosis', 'treatment'
]

# Open a connection to a new SQLite database
conn = sqlite3.connect('test2.db')

# Create n tables with k columns of random types
for i in range(n):
    # Generate a random table name
    table_name = 'table_' + str(i)

    # Generate a list of k column names
    column_names = []
    for j in range(k):
        column_name = None
        while column_name is None or column_name in pii_names + column_names:
            if random.random() < 0.5:
                column_name = random.choice(pii_names)
            else:
                column_name = 'column_' + str(j)
        column_names.append(column_name)

    # Generate a list of k column types
    column_types = []
    for j in range(k):
        if column_names[j] in pii_names:
            column_types.append('TEXT')
        else:
            column_types.append(random.choice(['INTEGER', 'REAL', 'TEXT']))

    # Create the table
    create_sql = f'CREATE TABLE {table_name} ({", ".join([f"{column_names[j]} {column_types[j]}" for j in range(k)])})'
    conn.execute(create_sql)

# Commit the changes and close the connection
conn.commit()
conn.close()
