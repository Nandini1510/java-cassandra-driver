After creating a maven project, add the following dependencies and plugins to the pom.xml file.
NOTE: replace YOUR_MAIN_CLASS_NAME with the name of the main class
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
                        <mainClass>YOUR_MAIN_CLASS_NAME</mainClass>
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

we get the java cassandra driver and the necessary plugins to create a jar file with all the added dependencies.
