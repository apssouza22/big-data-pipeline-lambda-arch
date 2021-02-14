package com.apssouza.iot.processor;

import com.apssouza.iot.util.PropertyFileReader;
import com.apssouza.iot.dto.IoTData;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class BatchProcessor {


    public static void main(String[] args) throws Exception {
        Properties prop = PropertyFileReader.readPropertyFile("iot-spark.properties");
        String file = prop.getProperty("com.iot.app.hdfs") + "iot-data-parque";
        String[] jars = {prop.getProperty("com.iot.app.jar")};

        SparkConf conf = getSparkConfig(prop, jars);
        final SparkSession sparkSession = SparkSession.builder().config(conf).getOrCreate();
        Dataset<Row> dataFrame = getDataFrame(sparkSession, file);
        JavaRDD<IoTData> rdd = dataFrame.javaRDD().map(BatchProcessor::transformToIotData);
        BatchHeatMapProcessor processor = new BatchHeatMapProcessor();
        processor.processHeatMap(rdd);
        sparkSession.close();
        sparkSession.stop();
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

