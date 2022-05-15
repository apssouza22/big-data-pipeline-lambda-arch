package com.apssouza.iot.batch;

import com.apssouza.iot.common.dto.IoTData;
import com.apssouza.iot.common.dto.POIData;
import com.apssouza.iot.common.ProcessorUtils;
import com.apssouza.iot.common.PropertyFileReader;
import com.datastax.spark.connector.util.JavaApiHelper;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

/**
 * Class responsible to start the process from the parque file
 */
public class BatchProcessor {


    public static void main(String[] args) throws Exception {
        //        String file = "iot-spark-local.properties";
        String fileProp = "iot-spark.properties";
        Properties prop = PropertyFileReader.readPropertyFile(fileProp);
        prop.setProperty("com.iot.app.spark.app.name", "Iot Batch Processor");

        var file = prop.getProperty("com.iot.app.hdfs") + "iot-data-parque";
        String[] jars = {prop.getProperty("com.iot.app.jar")};
        var conf = ProcessorUtils.getSparkConf(prop, "batch-processor");
        conf.setJars(jars);
        var sparkSession = SparkSession.builder().config(conf).getOrCreate();
        //broadcast variables. We will monitor vehicles on Route 37 which are of type Truck
        //Basically we are sending the data to each worker nodes on a Spark cluster.
        var classTag = JavaApiHelper.getClassTag(POIData.class);
        var broadcastPOIValues = sparkSession
                .sparkContext()
                .broadcast(getPointOfInterest(), classTag);

        var dataFrame = getDataFrame(sparkSession, file);
        var rdd = dataFrame.javaRDD().map(BatchProcessor::transformToIotData);
        BatchHeatMapProcessor.processHeatMap(rdd);
        BatchTrafficDataProcessor.processPOIData(rdd, broadcastPOIValues);
        BatchTrafficDataProcessor.processTotalTrafficData(rdd);
        BatchTrafficDataProcessor.processWindowTrafficData(rdd);
        sparkSession.close();
        sparkSession.stop();
    }

    private static POIData getPointOfInterest() {
        //poi data
        POIData poiData = new POIData();
        poiData.setLatitude(33.877495);
        poiData.setLongitude(-95.50238);
        poiData.setRadius(30);//30 km
        poiData.setRoute("Route-37");
        poiData.setVehicle("Truck");
        return poiData;
    }

    private static  IoTData transformToIotData(Row row) {
        return new IoTData(
                    row.getString(6),
                    row.getString(7),
                    row.getString(3),
                    row.getString(1),
                    row.getString(2),
                    row.getDate(5),
                    row.getDouble(4),
                    row.getDouble(0)
            );
    }


    public static Dataset<Row> getDataFrame(SparkSession sqlContext, String file) {
        return sqlContext.read()
                .parquet(file);
    }


    private static SparkConf getSparkConfig(Properties prop, String[] jars) {
        return new SparkConf()
                .setAppName(prop.getProperty("com.iot.app.spark.app.name"))
                .setMaster(prop.getProperty("com.iot.app.spark.master"))
                .set("spark.cassandra.connection.host", prop.getProperty("com.iot.app.cassandra.host"))
                .set("spark.cassandra.connection.port", prop.getProperty("com.iot.app.cassandra.port"))
                .set("spark.cassandra.auth.username", prop.getProperty("com.iot.app.cassandra.username"))
                .set("spark.cassandra.auth.password", prop.getProperty("com.iot.app.cassandra.password"))
                .set("spark.cassandra.connection.keep_alive_ms", prop.getProperty("com.iot.app.cassandra.keep_alive"));
    }

}

