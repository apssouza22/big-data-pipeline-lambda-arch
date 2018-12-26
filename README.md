Building robust,highly-scalable batch and real-time systems.
Applying the Lambda Architecture with Spark, Kafka, and Cassandra.


## How to use

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

