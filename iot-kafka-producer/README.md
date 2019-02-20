# IoT Kafka Producer
IoT Kafka Producer is a Maven application for generating IoT Data events using Apache Kafka. This project requires following tools and technologies.

- JDK - 1.8
- Maven - 3.3.9
- ZooKeeper - 3.4.8
- Kafka - 2.10-0.10.0.0

You can build and run this application using below commands. Please check resources/iot-kafka.properties for configuration details.

```sh
mvn package
mvn exec:java -Dexec.mainClass="com.iot.app.kafka.producer.IoTDataProducer"

```

Alternate way to run this application is using the “iot-kafka-producer-1.0.0.jar” file created by maven. Open command prompt, go to target folder and execute below command.

```sh
java -jar iot-kafka-producer-1.0.0.jar
```