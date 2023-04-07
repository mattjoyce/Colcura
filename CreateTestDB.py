import sqlite3
import random
import string

# Set the number of tables and columns
n = 50
k = 50

# Open a connection to a new SQLite database
conn = sqlite3.connect('test2.db')

# Create n tables with k columns of random types
for i in range(n):
    # Generate a random table name
    table_name = 'table_' + str(i)

    # Generate a list of k column names
    column_names = ['column_' + str(j) for j in range(k)]

    # Generate a list of k column types
    column_types = [random.choice(['INTEGER', 'REAL', 'TEXT']) for j in range(k)]

    # Create the table
    create_sql = f'CREATE TABLE {table_name} ({", ".join([f"{column_names[j]} {column_types[j]}" for j in range(k)])})'
    conn.execute(create_sql)

# Commit the changes and close the connection
conn.commit()
conn.close()
