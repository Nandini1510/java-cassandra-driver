Configure Cassandra Cluster Connection
cassConnectionManager.java Java program to configure connection to Cassandra Cluster.

Apache Cassandra CQL Commands
01_cassandra_sales_keyspace.cql Apache Cassandra CQL commands to create sales keyspace.
02_sales_create_tables.cql Apache Cassandra CQL commands to create tables in sales keyspace.
03_load_data_in_lookup_tables.cql Apache Cassandra CQL insert data into lookup tables in sales keyspace.

SalesApp Java code written to use java cassandra-driver.
SalesApp_GenerateUsers.java Generates Users. Typically executed only 1 time during initial setup.
NOTE: The pom.xml file of SalesApp_GenerateUsers.java:
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <groupId>com.mycompany</groupId>
    <artifactId>SalesApp_GenerateUsers</artifactId>
    <version>1.0-SNAPSHOT</version>
    <packaging>jar</packaging>
    <properties>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <maven.compiler.source>1.8</maven.compiler.source>
        <maven.compiler.target>1.8</maven.compiler.target>
    </properties>
        <build>
		<plugins>
			<plugin>
				<groupId>org.apache.maven.plugins</groupId>
				<artifactId>maven-shade-plugin</artifactId>
				<executions>
					<execution>
						<phase>package</phase>
						<goals>
							<goal>shade</goal>
						</goals>
					</execution>
				</executions>
				<configuration>
					<finalName>uber-${artifactId}-${version}</finalName>
				</configuration>
			</plugin>
                        <plugin>
            <groupId>org.apache.maven.plugins</groupId>
            <artifactId>maven-jar-plugin</artifactId>
            <version>2.4</version>
            <configuration>
                <archive>
                    <index>true</index>
                    <manifest>
                        <mainClass>SalesApp_GenerateUsers</mainClass>
                    </manifest>
                </archive>
            </configuration>
        </plugin>
		</plugins>
	</build>
       <dependencies>
        <dependency>
            <groupId>com.datastax.oss</groupId>
            <artifactId>java-driver-core</artifactId>
            <version>4.6.0</version>
        </dependency>
        <dependency>
            <groupId>ch.qos.logback</groupId>
            <artifactId>logback-classic</artifactId>
            <version>1.2.3</version>
        </dependency>
    </dependencies>
</project>
 
uber-SalesApp_GenerateUsers-1.0-SNAPSHOT
executable jar file to run on command prompt
to run the jar file execute the following command on command prompt:
java -jar uber-SalesApp_GenerateUsers-1.0-SNAPSHOT.jar com.mycompany.salesapp_generateusers

SalesApp_GenerateProducts.java Generates Products. Typically executed only 1 time during initial setup. 
NOTE: The pom.xml file of SalesApp_GenerateProducts.java:
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <groupId>com.mycompany</groupId>
    <artifactId>SalesApp_GenerateProducts</artifactId>
    <version>1.0-SNAPSHOT</version>
    <packaging>jar</packaging>
    <properties>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <maven.compiler.source>1.8</maven.compiler.source>
        <maven.compiler.target>1.8</maven.compiler.target>
    </properties>
    <build>
		<plugins>
			<plugin>
				<groupId>org.apache.maven.plugins</groupId>
				<artifactId>maven-shade-plugin</artifactId>
				<executions>
					<execution>
						<phase>package</phase>
						<goals>
							<goal>shade</goal>
						</goals>
					</execution>
				</executions>
				<configuration>
					<finalName>uber-${artifactId}-${version}</finalName>
				</configuration>
			</plugin>
                        <plugin>
            <groupId>org.apache.maven.plugins</groupId>
            <artifactId>maven-jar-plugin</artifactId>
            <version>2.4</version>
            <configuration>
                <archive>
                    <index>true</index>
                    <manifest>
                        <mainClass>SalesApp_GenerateProducts</mainClass>
                    </manifest>
                </archive>
            </configuration>
        </plugin>
		</plugins>
	</build>
    <dependencies>
        <dependency>
            <groupId>com.datastax.oss</groupId>
            <artifactId>java-driver-core</artifactId>
            <version>4.6.0</version>
        </dependency>
        <dependency>
            <groupId>ch.qos.logback</groupId>
            <artifactId>logback-classic</artifactId>
            <version>1.2.3</version>
        </dependency>
    </dependencies>
</project>

uber-SalesApp_GenerateProducts-1.0-SNAPSHOT
executable jar file to run on command prompt
to run the jar file execute the following command on command prompt:
java -jar uber-SalesApp_GenerateProducts-1.0-SNAPSHOT.jar com.mycompany.salesapp_generateproducts

SalesApp_GenerateOrders.java Generates Orders. Execute it whenever you want to generate Orders 
NOTE: The pom.xml file of SalesApp_GenerateProducts.java:
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <groupId>com.mycompany</groupId>
    <artifactId>SalesApp_GenerateOrders</artifactId>
    <version>1.0-SNAPSHOT</version>
    <packaging>jar</packaging>
    <properties>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <maven.compiler.source>1.8</maven.compiler.source>
        <maven.compiler.target>1.8</maven.compiler.target>
    </properties>
    <build>
		<plugins>
			<plugin>
				<groupId>org.apache.maven.plugins</groupId>
				<artifactId>maven-shade-plugin</artifactId>
				<executions>
					<execution>
						<phase>package</phase>
						<goals>
							<goal>shade</goal>
						</goals>
					</execution>
				</executions>
				<configuration>
					<finalName>uber-${artifactId}-${version}</finalName>
				</configuration>
			</plugin>
                        <plugin>
            <groupId>org.apache.maven.plugins</groupId>
            <artifactId>maven-jar-plugin</artifactId>
            <version>2.4</version>
            <configuration>
                <archive>
                    <index>true</index>
                    <manifest>
                        <mainClass>SalesApp_GenerateOrders</mainClass>
                    </manifest>
                </archive>
            </configuration>
        </plugin>
		</plugins>
	</build>
    <dependencies>
        <dependency>
            <groupId>com.datastax.oss</groupId>
            <artifactId>java-driver-core</artifactId>
            <version>4.6.0</version>
        </dependency>
        <dependency>
            <groupId>ch.qos.logback</groupId>
            <artifactId>logback-classic</artifactId>
            <version>1.2.3</version>
        </dependency>
    </dependencies>
</project>
uber-SalesApp_GenerateOrders-1.0-SNAPSHOT
executable jar file to run on command prompt
to run the jar file execute the following command on command prompt:
java - uber-SalesApp_GenerateOrders-1.0-SNAPSHOT.jar com.mycompany.salesapp_generateorders
