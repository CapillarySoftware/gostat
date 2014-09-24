CREATE KEYSPACE IF NOT EXISTS gostat WITH REPLICATION = {
	'class'              : 'SimpleStrategy',
	'replication_factor' : 1
};

