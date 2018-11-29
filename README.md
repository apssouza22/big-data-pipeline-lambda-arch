Building robust,highly-scalable batch and real-time systems.
Applying the Lambda Architecture with Spark, Kafka, and Cassandra.

## Spark
Add spark-master to /etc/hosts

### Submit a job to master
- Connect to the master `spark-shell --master spark://spark-master:7077`
- Run `spark-submit jar-path`
- Or Access the Spark master via SSH and run `spark-submit jar-path`

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

