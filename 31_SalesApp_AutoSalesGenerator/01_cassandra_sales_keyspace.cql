-- create a keyspace for sales transaction generator "SalesApp" will be used with python cassandra-driver

CREATE KEYSPACE sales WITH REPLICATION = {'class': 'SimpleStrategy', 'replication_factor': '3'}  AND durable_writes = true;

-- if authentication is enabled on cassandra
CREATE ROLE sales_user WITH SUPERUSER = false AND LOGIN = true AND PASSWORD = 'PassCode' ;
GRANT select ON KEYSPACE sales TO sales_user ;
GRANT modify ON KEYSPACE sales TO sales_user ;
LIST ALL PERMISSIONS OF sales_user ;
