Configure Cassandra Cluster Connection
cassConnectionManager.java Java program to configure connection to Cassandra Cluster.

Apache Cassandra CQL Commands
01_cassandra_sales_keyspace.cql Apache Cassandra CQL commands to create sales keyspace.
02_sales_create_tables.cql Apache Cassandra CQL commands to create tables in sales keyspace.
03_load_data_in_lookup_tables.cql Apache Cassandra CQL insert data into lookup tables in sales keyspace.

SalesApp Java code written to use java cassandra-driver.
SalesApp_GenerateUsers.java Generates Users. Typically executed only 1 time during initial setup.
NOTE: The pom.xml file of SalesApp_GenerateUsers.java is in SalesApp_GenerateUsers_pom.txt
 
uber-SalesApp_GenerateUsers-1.0-SNAPSHOT
executable jar file to run on command prompt
to run the jar file execute the following command on command prompt:
java -jar uber-SalesApp_GenerateUsers-1.0-SNAPSHOT.jar com.mycompany.salesapp_generateusers

SalesApp_GenerateProducts.java Generates Products. Typically executed only 1 time during initial setup. 
NOTE: The pom.xml file of SalesApp_GenerateProducts.java is in SalesApp_GenerateProducts_pom.txt

uber-SalesApp_GenerateProducts-1.0-SNAPSHOT
executable jar file to run on command prompt
to run the jar file execute the following command on command prompt:
java -jar uber-SalesApp_GenerateProducts-1.0-SNAPSHOT.jar com.mycompany.salesapp_generateproducts

SalesApp_GenerateOrders.java Generates Orders. Execute it whenever you want to generate Orders 
NOTE: The pom.xml file of SalesApp_GenerateOrders.java is in SalesApp_GenerateOrders_pom.txt

uber-SalesApp_GenerateOrders-1.0-SNAPSHOT
executable jar file to run on command prompt
to run the jar file execute the following command on command prompt:
java - uber-SalesApp_GenerateOrders-1.0-SNAPSHOT.jar com.mycompany.salesapp_generateorders
