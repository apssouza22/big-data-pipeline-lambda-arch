Building robust,highly-scalable batch and real-time systems.
Applying the Lambda Architecture with Spark, Kafka, and Cassandra.

## Spark
Add spark-master to /etc/hosts

### Submit a job to master
- Connect to the master `spark-shell --master spark://spark-master:7077`
- Run `spark-submit jar-path`

Or

Access the Spark master via SSH and run `spark-submit jar-path`

### GUI
http://localhost:8080 Master
http://localhost:8081 Slave


