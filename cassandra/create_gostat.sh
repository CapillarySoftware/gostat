echo Creating the 'gostat' Cassandra datastore
cqlsh -f cql/01-create_gostat.cql

echo Creating tables
cqlsh -f cql/02-create_tables.cql

echo Done