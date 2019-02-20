# IoT Spring Boot Dashboard
IoT Spring Boot Dashboard is a Maven application which queries Cassandra Database and pushes data to UI.This application is built using 

- Spring Boot
- jQuery.js
- Bootstrap.js
- Sockjs.js
- Stomp.js
- Chart.js

This project requires following tools and technologies.

- JDK - 1.8
- Maven - 3.3.9
- Cassandra - 2.2.6

Please refer "IoTData.cql" file to create Keyspace and Tables in Cassandra Database, which are required by this application. This is the same file which is available in "iot-spark-processor" project.

You can build and run this application using below commands. Please check resources/iot-springboot.properties for configuration details.

```sh
mvn package
mvn exec:java -Dexec.mainClass="com.iot.app.springboot.dashboard.IoTDataDashboard"
```

Alternate way to run this application is using the “iot-springboot-dashboard-1.0.0.jar” file created by maven. Open command prompt, go to target folder and execute below command.

```sh
java -jar iot-springboot-dashboard-1.0.0.jar
```
Open browser and entre http://localhost:8080 to see the Dashboard
