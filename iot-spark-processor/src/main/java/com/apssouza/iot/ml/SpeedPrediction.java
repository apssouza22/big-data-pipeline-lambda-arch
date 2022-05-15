package com.apssouza.iot.ml;

import com.apssouza.iot.common.PropertyFileReader;

import org.apache.spark.SparkConf;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class SpeedPrediction {

    public static void main(String[] args) throws Exception {
        String fileProp = "iot-spark.properties";
        Properties prop = PropertyFileReader.readPropertyFile(fileProp);

        var file = prop.getProperty("com.iot.app.hdfs") + "iot-data-parque";
        var model = prop.getProperty("com.iot.app.hdfs") + "model-prediction";
        String[] jars = {prop.getProperty("com.iot.app.jar")};
        prop.setProperty("com.iot.app.spark.app.name", "Iot ML");

        var conf = getSparkConf(prop);
        conf.setJars(jars);
        var sparkSession = SparkSession.builder().config(conf).getOrCreate();
        ModelPipeline
                .getInstance(sparkSession)
                .loadData(file)
                .transformData()
                .splitData(0.8, 0.2)
                .createModel()
                .trainModel()
                .evaluateModel()
                .saveModel(model)
                ;

        LinearRegressionModel sameModel = LinearRegressionModel.load(model);
        var newData = Vectors.dense(new double[]{33.0,7.0});
        double prediction = sameModel.predict(newData);
        System.out.println("Model Prediction on New Data = " + prediction);

    }
    public static SparkConf getSparkConf(Properties prop) {
        var sparkConf = new SparkConf()
                .setAppName("ml-prediction")
                .setMaster(prop.getProperty("com.iot.app.spark.master"));
        if ("local".equals(prop.getProperty("com.iot.app.env"))) {
            sparkConf.set("spark.driver.bindAddress", "127.0.0.1");
        }
        return sparkConf;
    }

}
