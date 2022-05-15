package com.apssouza.iot.ml;

import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.regression.LinearRegression;
import org.apache.spark.ml.regression.LinearRegressionModel;
import org.apache.spark.ml.regression.LinearRegressionTrainingSummary;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.types.DataTypes.IntegerType;

import java.io.IOException;

public class ModelPipeline {

    private  SparkSession sparkSession;
    private Dataset<Row> dataFrame;
    private Dataset<Row> trainingData;
    private Dataset<Row> testData;
    private LinearRegression lr;
    private LinearRegressionModel lrModel;
    private Dataset<Row> dataset;

    public ModelPipeline( SparkSession sparkSession) {
        this.sparkSession = sparkSession;
    }

    public static ModelPipeline getInstance( SparkSession sparkSession) {
        return new ModelPipeline(sparkSession);
    }

    public ModelPipeline loadData( String file) {
        this.dataFrame = sparkSession.read().parquet(file);
        return this;
    }

    public ModelPipeline transformData() {
        dataFrame = dataFrame.withColumnRenamed("speed", "label")
                .withColumn("grid", concat(col("latitude"), lit('|'), col("longitude")))
                .withColumn("lat",dataFrame.col("latitude").cast(IntegerType))
                .withColumn("long",dataFrame.col("longitude").cast(IntegerType));

        //        Dataset<Row> df_sample2 = dataFrame2.withColumn("label", when(col("label").isNotNull().
        //                and(col("label").equalTo(lit("Yes"))), lit(1)).otherwise(lit(0)));

        // After VectorAssembler you have to have a training dataset with label and features columns.
        // https://spark.apache.org/docs/latest/ml-features#vectorassembler
        VectorAssembler VA = new VectorAssembler()
                .setInputCols(new String[]{"lat","long", "dayOfWeek"})
                .setOutputCol("features");
        this.dataset = VA.transform(dataFrame);
        dataset.show();
        dataset.printSchema();
        return this;
    }

    public ModelPipeline splitData(double training, double test) {
        var splits = dataset.randomSplit(new double[] { training, test }, 11L);
        this.trainingData = splits[0];
        this.testData = splits[1];
        return this;
    }

    public ModelPipeline createModel() {
        this.lr = new LinearRegression()
                .setMaxIter(10)
                .setRegParam(0.3)
                .setElasticNetParam(0.8);
        return this;
    }

    public ModelPipeline trainModel() {
        this.lrModel = lr.fit(trainingData);

        // Print the coefficients and intercept for linear regression.
        System.out.println("Coefficients: "
                + lrModel.coefficients() + " Intercept: " + lrModel.intercept());

        // Summarize the model over the training set and print out some metrics.
        LinearRegressionTrainingSummary trainingSummary = lrModel.summary();
        System.out.println("numIterations: " + trainingSummary.totalIterations());
        System.out.println("objectiveHistory: " + Vectors.dense(trainingSummary.objectiveHistory()));
        System.out.println("Training RMSE: " + trainingSummary.rootMeanSquaredError());
        System.out.println("Training r2: " + trainingSummary.r2());
        trainingSummary.residuals().show();
        return this;
    }

    public ModelPipeline evaluateModel() {
        var evaluationSummary = lrModel.evaluate(testData);
        System.out.println("Test RMSE: " + evaluationSummary.rootMeanSquaredError());
        System.out.println("Test R2: " + evaluationSummary.r2());

        Dataset<Row> predictions = evaluationSummary.predictions();
        predictions.select(
                predictions.col("features"),
                predictions.col("label"),
                predictions.col("prediction")
        ).show();

        return this;
    }

    public ModelPipeline saveModel(String model) throws IOException {
        lrModel.save(model);
        return this;
    }
}
