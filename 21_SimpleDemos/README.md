Configure Cassandra Cluster Connection
cassConnectionManager.java Java program to configure connection to Cassandra Cluster.

Get Cassandra Cluster Information
getCassClusterInfo.java Java program to retrieve Cassandra Cluster Information using java cassandra-driver.
NOTE: The pom.xml file of getCassClusterInfo.java:

<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <groupId>com.mycompany</groupId>
    <artifactId>getCassClusterInfo</artifactId>
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
                        <mainClass>getCassClusterInfo</mainClass>
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

uber-getCassClusterInfo-1.0-SNAPSHOT.jar 
executable jar file to run on command prompt
to run the jar file execute the following command on command prompt:
java -jar uber-getCassClusterInfo-1.0-SNAPSHOT.jar com.mycompany.getcassclusterinfo

basic INSERT & SELECT
01_cassdemo_emp_cassandraTable.cql Apache Cassandra CQL commands to create demo keyspace and employee table.

readWriteCassEmp.java Java program to demonstrate basic INSERT & SELECT using java cassandra-driver.
NOTE: The pom.xml file of readWriteCassEmp.java:

<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>
    <groupId>com.mycompany</groupId>
    <artifactId>readWriteCassEmp</artifactId>
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
                        <mainClass>readWriteCassEmp</mainClass>
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

uber-readWriteCassEmp-1.0-SNAPSHOT.jar
executable jar file to run on command prompt
to run the jar file execute the following command on command prompt:
java -jar uber-readWriteCassEmp-1.0-SNAPSHOT.jar com.mycompany.readwritecassemp