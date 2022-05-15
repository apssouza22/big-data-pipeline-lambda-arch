package com.apssouza.iot.common;

import org.apache.spark.SparkConf;

import java.util.Properties;

public class ProcessorUtils {

    public static SparkConf getSparkConf(Properties prop, String appName) {

        var sparkConf = new SparkConf()
                .setAppName(appName)
                .setMaster(prop.getProperty("com.iot.app.spark.master"))
                .set("spark.cassandra.connection.host", prop.getProperty("com.iot.app.cassandra.host"))
                .set("spark.cassandra.connection.port", prop.getProperty("com.iot.app.cassandra.port"))
                .set("spark.cassandra.auth.username", prop.getProperty("com.iot.app.cassandra.username"))
                .set("spark.cassandra.auth.password", prop.getProperty("com.iot.app.cassandra.password"))
                .set("spark.cassandra.connection.keep_alive_ms", prop.getProperty("com.iot.app.cassandra.keep_alive"));

        if ("local".equals(prop.getProperty("com.iot.app.env"))) {
            sparkConf.set("spark.driver.bindAddress", "127.0.0.1");
        }
        return sparkConf;
    }

}
