package com.apssouza.lambda;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class App {

    private static ObjectMapper objectMapper = new ObjectMapper();


    public static void main(String[] args) throws IOException, URISyntaxException {
        String hdfsUrl = "./data/spark/";

//        String sparkMasterUrl = "local[*]";
//        String csvFile = "/Users/apssouza/Projetos/java/lambda-arch/data/spark/input/localhost.csv";

        String sparkMasterUrl = "spark://spark-master:7077";
        String csvFile = "hdfs://namenode:8020/user/lambda/localhost.csv";
        String[] jars = {"./target/lambda-arch-1.0-SNAPSHOT.jar"};

        JavaSparkContext sparkContext = getSparkContext(sparkMasterUrl, jars);
        SQLContext sqlContext = new SQLContext(sparkContext);
        HeatMapMesurement app = new HeatMapMesurement();
        Dataset<Row> dataFrame = getDataSet(sqlContext, csvFile);
        List<Map<String, Object>> gridBoxes = app.getHeatmap(10, dataFrame);
        writeJson(gridBoxes, hdfsUrl + "output/gridbox-batch.json");

        sparkContext.close();
        sparkContext.stop();
    }


    public static Dataset<Row> getDataSet(SQLContext sqlContext, String csvFile) {
        return sqlContext.read()
                .format("csv")
                .option("header", "true")
                .load(csvFile);
    }


    private static JavaSparkContext getSparkContext(String sparkMasterUrl, String[] jars) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("BDE-SensorDemo")
                .setMaster(sparkMasterUrl)
                .setJars(jars);
        return new JavaSparkContext(sparkConf);
    }


    private static void writeJson(List<Map<String, Object>> gridBoxes, String filepath) throws IOException {
        Map<String, Object> data = new HashMap<>();
        data.put("data", gridBoxes);
        System.out.println(data.toString());
        BufferedWriter writer = new BufferedWriter(new FileWriter(filepath, true));
        objectMapper.writeValue(writer, data);
    }

}

