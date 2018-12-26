package com.apssouza.lambda;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import scala.Tuple2;
import sensors.model.Coordinate;
import sensors.model.Measurement;
import sensors.utils.TimestampComparator;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class SparkJob {

    private SQLContext sqlContext;

    public SparkJob(SQLContext sqlContext) {
        this.sqlContext = sqlContext;
    }

    public Dataset<Row> getDataSet(String csvFile) {
        return sqlContext.read()
                .format("csv")
                .option("header", "true")
                .load(csvFile);
    }


    public List<Map<String, Object>> getHeatmap(int maxDetail, String csvFile) throws IOException {
        Dataset<Row> dataFrame = getDataSet(csvFile);
        JavaRDD<Measurement> measurements = csvToMeasurements(dataFrame);
        JavaRDD<Measurement> measurementsWithRoundedCoordinates = roundCoordinates(measurements);

        LocalDateTime minTimestamp = measurements.min(new TimestampComparator()).getTimestamp();
        LocalDateTime maxTimestamp = measurements.max(new TimestampComparator()).getTimestamp();
        long duration = minTimestamp.until(maxTimestamp, ChronoUnit.MILLIS);
        List<Map<String, Object>> gridBoxes = new ArrayList<>();
        for (int detail = 1; detail <= maxDetail; detail *= 2) {
            long timeStep = duration / detail;
            for (int i = 0; i < detail; i++) {
                LocalDateTime start = minTimestamp.plus(timeStep * i, ChronoUnit.MILLIS);
                LocalDateTime end = minTimestamp.plus(timeStep * (i + 1), ChronoUnit.MILLIS);
                JavaRDD<Measurement> measurementsFilteredByTime = filterByTime(measurementsWithRoundedCoordinates, start, end);
                JavaPairRDD<Coordinate, Integer> counts = countPerGridBox(measurementsFilteredByTime);
                gridBoxes.addAll(getCountInArea(counts));
//                String fileName = outputPath + "/" + (i + 1) + ".json";
            }
        }
        return gridBoxes;
    }


    private List<Map<String, Object>> getCountInArea(JavaPairRDD<Coordinate, Integer> tuples) throws IOException {
        return tuples.map(tuple -> {
                    Coordinate coordinate = tuple._1();
                    Map<String, Object> gridBox = new HashMap<>();
                    gridBox.put("latitude", coordinate.getLatitude());
                    gridBox.put("longitude", coordinate.getLongitude());
                    gridBox.put("count", tuple._2());
                    return gridBox;
                }
        ).collect();
    }


    /**
     * Converts each row from the dataset  to a Measurement
     *
     * @param dataset | Spark SQL context
     * @return A set containing all data from the CSV file as Measurements
     */
    private JavaRDD<Measurement> csvToMeasurements(Dataset<Row> dataset) {
        JavaRDD<Measurement> map = dataset.javaRDD()
                .map(row -> {
                    LocalDateTime time = LocalDateTime.parse(row.getString(row.fieldIndex("timestamp")), DateTimeFormatter.ISO_DATE_TIME);
                    Double latitude = Double.parseDouble(row.getString(row.fieldIndex("latitude")));
                    Double longitude = Double.parseDouble(row.getString(row.fieldIndex("longitude")));
                    Coordinate coordinate = new Coordinate(latitude, longitude);
                    return new Measurement(coordinate, time);
                });
        return map;
    }


    /**
     * Maps the measurements by rounding the coordinate.
     * The world is defined by a grid of boxes, each box has a size of 0.0005 by 0.0005.
     * Every mapping will be rounded to the center of the box it is part of.
     * Boundary cases will be rounded up, so a coordinate on (-0.00025,0) will be rounded to (0,0),
     * while the coordinate (0.00025,0) will be rounded to (0.0005,0).
     *
     * @param measurements | The dataset of measurements
     * @return A set of measurements with rounded coordinates
     */
    private JavaRDD<Measurement> roundCoordinates(JavaRDD<Measurement> measurements) {
        return measurements.map(measurement -> {
                    double roundedLatitude = (double) (5 * Math.round((measurement.getCoordinate().getLatitude() * 10000) / 5)) / 10000;
                    double roundedLongitude = (double) (5 * Math.round((measurement.getCoordinate().getLongitude() * 10000) / 5)) / 10000;
                    Coordinate roundedCoordinate = new Coordinate(roundedLatitude, roundedLongitude);
                    measurement.setRoundedCoordinate(roundedCoordinate);
                    return measurement;
                }
        );
    }

    /**
     * Filter the measurements in a given time period
     *
     * @param measurements | The dataset of measurements
     * @param start        | Start of the time period
     * @param end          | End of the time period
     * @return A set of measurements in the given time period
     */
    private JavaRDD<Measurement> filterByTime(JavaRDD<Measurement> measurements, LocalDateTime start, LocalDateTime end) {
        return measurements.filter(measurement -> (
                        measurement.getTimestamp().isEqual(start) || measurement.getTimestamp().isAfter(start)
                ) && measurement.getTimestamp().isBefore(end)
        );
    }

    /**
     * Reduces the dataset by counting the number of measurements for a specific grid box (rounded coordinate)
     *
     * @param measurements | The dataset of measurements
     * @return A set of tuples linking rounded coordinates to their number of occurrences
     */
    private JavaPairRDD<Coordinate, Integer> countPerGridBox(JavaRDD<Measurement> measurements) {
        return measurements.mapToPair(
                measurement -> new Tuple2<>(measurement.getRoundedCoordinate(), 1)
        ).reduceByKey((a, b) -> a + b);
    }

}
