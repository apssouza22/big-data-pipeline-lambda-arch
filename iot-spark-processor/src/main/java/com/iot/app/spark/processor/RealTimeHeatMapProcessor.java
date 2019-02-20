package com.iot.app.spark.processor;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.iot.app.spark.dto.Coordinate;
import com.iot.app.spark.entity.HeatMapData;
import com.iot.app.spark.dto.Measurement;
import com.iot.app.spark.dto.IoTData;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static com.datastax.spark.connector.japi.CassandraStreamingJavaUtil.javaFunctions;

/**
 * Process the real-time heat map calculation
 *
 * @author apssouza22
 */
public class RealTimeHeatMapProcessor {

    public void processHeatMap(JavaDStream<IoTData> filteredIotDataStream) throws IOException {
        JavaDStream<Measurement> measurements = iotDataToMeasurements(filteredIotDataStream);
        JavaDStream<Measurement> measurementsWithRoundedCoordinates = roundCoordinates(measurements);
        JavaPairDStream<Coordinate, Integer> counts = countPerGridBox(measurementsWithRoundedCoordinates);
        JavaDStream<HeatMapData> heatMapStream = getHeatMap(counts);
        save(heatMapStream);
    }

    private void save(JavaDStream<HeatMapData> heatMapStream) {
        // Map Cassandra table column
        Map<String, String> columnNameMappings = new HashMap<>();
        columnNameMappings.put("latitude", "latitude");
        columnNameMappings.put("longitude", "longitude");
        columnNameMappings.put("totalCount", "totalcount");
        columnNameMappings.put("timeStamp", "timestamp");

        // call CassandraStreamingJavaUtil function to save in DB
        javaFunctions(heatMapStream).writerBuilder(
                "traffickeyspace",
                "heat_map",
                CassandraJavaUtil.mapToRow(HeatMapData.class, columnNameMappings)
        ).saveToCassandra();
    }


    private JavaDStream<HeatMapData> getHeatMap(JavaPairDStream<Coordinate, Integer> tuples) throws IOException {
        return tuples.map(tuple -> {
            Coordinate coordinate = tuple._1();
            return new HeatMapData(coordinate.getLatitude(), coordinate.getLongitude(), tuple._2(), new Date());
        });
    }


    /**
     * Converts each row from the dataset  to a Measurement
     *
     * @param streaming | Spark SQL context
     * @return A set containing all data from the CSV file as Measurements
     */
    private JavaDStream<Measurement> iotDataToMeasurements(JavaDStream<IoTData> streaming) {
        JavaDStream<Measurement> map = streaming.map(row -> {
            Coordinate coordinate = new Coordinate(
                    Double.valueOf(row.getLatitude()),
                    Double.valueOf(row.getLongitude())
            );
            return new Measurement(coordinate, row.getTimestamp());
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
    private JavaDStream<Measurement> roundCoordinates(JavaDStream<Measurement> measurements) {
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
     * Reduces the dataset by counting the number of measurements for a specific grid box (rounded coordinate)
     *
     * @param measurements | The dataset of measurements
     * @return A set of tuples linking rounded coordinates to their number of occurrences
     */
    private JavaPairDStream<Coordinate, Integer> countPerGridBox(JavaDStream<Measurement> measurements) {
        // reduce by key and window (30 sec window and 10 sec slide).
        return measurements.mapToPair(
                measurement -> new Tuple2<>(
                        measurement.getRoundedCoordinate(),
                        1
                )
        ).reduceByKeyAndWindow((a, b) -> a + b,
                Durations.seconds(30),
                Durations.seconds(10)
        );
    }

}
