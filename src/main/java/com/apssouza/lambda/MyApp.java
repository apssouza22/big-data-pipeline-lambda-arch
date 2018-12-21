package com.apssouza.lambda;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;
import sensors.model.Coordinate;
import sensors.model.Measurement;

import java.io.IOException;
import java.net.URISyntaxException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Created by apssouza on 19/12/18.
 */
public class MyApp {
    public static void main(String[] args) throws IOException, URISyntaxException {
//        String sparkMasterUrl = "local[*]";
//        String csvFile = "/Users/apssouza/Projetos/java/lambda-arch/data/spark/input/localhost.csv";

        String sparkMasterUrl = "spark://spark-master:7077";
        String csvFile = "hdfs://namenode:8020/user/lambda/localhost.csv";
        String[] jars = {"/Users/apssouza/Projetos/java/lambda-arch/target/lambda-arch-1.0-SNAPSHOT.jar"};

        SparkSession session = SparkSession.builder()
                .appName("Lambda-project")
                .master(sparkMasterUrl)
                .config("spark.jars", "/Users/apssouza/Projetos/java/lambda-arch/target/lambda-arch-1.0-SNAPSHOT.jar")
                .getOrCreate();

        DataFrameReader read = session.read();
        Dataset<Row> dataFrame = read
                .format("csv")
                .option("header", "true")
                .load(csvFile);

        JavaRDD<Row> rddwithoutMap = dataFrame.javaRDD();
        JavaRDD<Row> rddwithMap = dataFrame.javaRDD()
                .map(new Function<Row, Row>() {
                    @Override
                    public Row call(Row row) throws Exception {
                        return row;
                    }
                });

        long count = rddwithoutMap.count();
        long countBeforeMap = rddwithMap.count();

    }
}
