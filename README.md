# Lambda architecture

![Alt text](diagram.png?raw=true "Lambda architecture")

Read about the project [here](https://medium.com/@alexsandrosouza/lambda-architecture-how-to-build-a-big-data-pipeline-part-1-8b56075e83fe)


Our Lambda project receives real-time IoT Data Events coming from Connected Vehicles, 
then ingested to Spark through Kafka. Using the Spark streaming API, we processed and analysed 
IoT data events and transformed them into vehicle count for different types of vehicles on different routes. 
While simultaneously the data is also stored into HDFS for Batch processing. 
We performed a series of stateless and stateful transformation using Spark streaming API on 
streams and persisted them to Cassandra database tables. In order to get accurate views, 
we also process a batch processing creating a batch view into Cassandra. 
We developed responsive web traffic monitoring dashboard using Spring Boot, 
SockJs and Bootstrap which merge two views from the Cassandra database before pushing to the UI using web socket.


All component parts are dynamically managed using Docker, which means you don't need to worry 
about setting up your local environment, the only thing you need is to have Docker installed.


- JDK - 1.8
- Maven
- ZooKeeper
- Kafka
- Cassandra
- Spark
- Docker
- HDFS



The streaming part of the project was done from iot-traffic-project [InfoQ](https://www.infoq.com/articles/traffic-data-monitoring-iot-kafka-and-spark-streaming)

## How to use


Please refer "IoTData.cql" file to create Keyspace and Tables in Cassandra Database, which are required by this application.

You can build and run this application using below commands. Please check resources/iot-spark.properties for configuration details.

```sh
mvn package
spark-submit --class "com.iot.app.spark.processor.IoTDataProcessor” iot-spark-processor-1.0.0.jar
```


```
kafka-topics --create --zookeeper zookeeper:2181 --replication-factor 1 --partitions 1 --topic iot-data-event

cqlsh --username cassandra --password cassandra  -f /schema.cql


```


* run `docker-compose -p lambda up`
* upload the localhost.csv to the container `docker cp data/spark/input/localhost.csv namenode:/localhost.csv`

From the namenode container: 

* `hdfs dfs -mkdir /user`
* `hdfs dfs -mkdir /user/lambda`
* `hdfs dfs -put localhost.csv /user/lambda/`
* Access the file http://localhost:50075/webhdfs/v1/user/lambda/localhost.csv?op=OPEN&namenoderpcaddress=namenode:8020&offset=0



## Spark
Add spark-master to /etc/hosts

### Submit a job to master
- mvn package
- `spark-submit --class com.apssouza.lambda.App --master spark://spark-master:7077 /Users/apssouza/Projetos/java/lambda-arch/target/lambda-arch-1.0-SNAPSHOT.jar``


### GUI
http://localhost:8080 Master
http://localhost:8081 Slave


## HDFS

Comands https://hortonworks.com/tutorial/manage-files-on-hdfs-via-cli-ambari-files-view/section/1/

Open a file - http://localhost:50070/webhdfs/v1/path/to/file/file.csv?op=open

Web file handle - https://hadoop.apache.org/docs/r1.0.4/webhdfs.html

Gui
http://localhost:50070
http://localhost:50075


## Kafka
  
* kafka-topics --create --topic cordinations --partitions 1 --replication-factor 1 --if-not-exists --zookeeper zookeeper:2181
* kafka-console-producer --request-required-acks 1 --broker-list kafka:9092 --topic cordinations
* kafka-console-consumer --bootstrap-server kafka:9092 --topic cordinations
* kafka-topics --list --zookeeper zookeeper:2181
