# META DATA - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

    # Developer details: 
        # Name: Harish S
        # Role: Architect
        # Code ownership rights: Harish S
    # Version:
        # Version: V 1.0 (July 1)
            # Developer: Harish S
     
    # Description: This code enables CRUD operations in Cassandra db
    
# CODE - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - 

# Dependency: 
    # Environment:     
        #Python 3.10.13
        #Cassandra-driver ->pip install cassandra-driver 
        #Pandas 2.2.1 -> pip install Pandas==2.2.1

from cassandra.cluster import Cluster

# Connect to the Cassandra cluster
cluster = Cluster(['localhost'])  # Provide the contact points (IP addresses) of Cassandra nodes
session = cluster.connect()

# Create a keyspace (if not exists)
session.execute("""
    CREATE KEYSPACE IF NOT EXISTS my_keyspace 
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
""")

# Use the keyspace
session.set_keyspace('my_keyspace')

# Create a table (if not exists)
session.execute("""
    CREATE TABLE IF NOT EXISTS my_table (
        id INT PRIMARY KEY,
        name TEXT
    )
""")

# Insert data into the table
session.execute("""
    INSERT INTO my_table (id, name) VALUES (%s, %s)
""", (1, 'John Doe'))

# Retrieve data from the table
rows = session.execute("SELECT * FROM my_table")

# Print the retrieved data
for row in rows:
    print(row.id, row.name)

# Close the connection
cluster.shutdown()