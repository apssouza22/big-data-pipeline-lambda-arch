package com.apssouza.lambda.products;

import org.apache.commons.collections.map.HashedMap;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by apssouza on 06/02/19.
 */
public class BatchJob {

    public static void main(String[] args) {

        String hdfsUrl = "./data/spark/";

//        String sparkMasterUrl = "local[*]";
//        String csvFile = "/Users/apssouza/Projetos/java/lambda-arch/data/spark/input/localhost.csv";

        String sparkMasterUrl = "spark://spark-master:7077";
        String csvFile = "hdfs://namenode:8020/user/lambda/localhost.csv";
        String[] jars = {"./target/lambda-arch-1.0-SNAPSHOT.jar"};

        JavaSparkContext sparkContext = getSparkContext(sparkMasterUrl, jars);
        SQLContext sqlContext = new SQLContext(sparkContext);
        Dataset<Row> dataSet = getDataSet(sqlContext, csvFile);
        Dataset<Row> inputDF = dataSet.where("unix_timestamp() - timestamp_hour / 1000 <= 60 * 60 * 6");

        inputDF.registerTempTable("activity");

        handleVisitorsByProduct(sqlContext);
        handleActivitiesByProduct(sqlContext);
    }

    private static void handleActivitiesByProduct(SQLContext sqlContext) {
        Dataset<Row> activityByProduct = sqlContext.sql(
                "SELECT product, timestamp_hour, " +
                        "sum( case when action = 'purchase' then 1 else 0 end)as purchase_count, " +
                        "sum( case when action = 'add_to_cart' then 1 else 0 end)as add_to_cart_count, " +
                        "sum( case when action = 'page_view' then 1 else 0 end)as page_view_count " +
                        "from activity group by product, timestamp_hour "
        ).cache();

        Map<String, String> options2 = new HashMap() {{
            put("keyspace", "lambda");
            put("table", "batch_activity_by_product");
        }};

        activityByProduct
                .write()
                .format("org.apache.spark.sql.cassandra")
                .options(options2).save();
    }

    private static void handleVisitorsByProduct(SQLContext sqlContext) {
        Dataset<Row> visitorsByProduct = sqlContext.sql(
                "SELECT product, timestamp_hour, COUNT(DISTINCT visitor) as unique_visitors " +
                        "| FROM activity GROUP BY product, timestamp_hour"
        );

        Map<String, String> options = new HashMap() {{
            put("keyspace", "lambda");
            put("table", "batch_visitors_by_product");
        }};
        visitorsByProduct
                .write()
                .format("org.apache.spark.sql.cassandra")
                .options(options).save();
    }


    private static JavaSparkContext getSparkContext(String sparkMasterUrl, String[] jars) {
        SparkConf sparkConf = new SparkConf()
                .setAppName("BDE-SensorDemo")
                .setMaster(sparkMasterUrl)
                .setJars(jars);
        return new JavaSparkContext(sparkConf);
    }


    public static Dataset<Row> getDataSet(SQLContext sqlContext, String csvFile) {
        return sqlContext.read()
                .format("csv")
                .option("header", "true")
                .load(csvFile);
    }
}
