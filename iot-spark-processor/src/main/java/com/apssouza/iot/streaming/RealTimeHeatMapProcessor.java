package com.apssouza.iot.streaming;

import com.apssouza.iot.common.dto.Coordinate;
import com.apssouza.iot.common.dto.Measurement;
import com.apssouza.iot.common.entity.HeatMapData;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.apssouza.iot.common.dto.IoTData;

import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;

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


    public static void processHeatMap(JavaDStream<IoTData> streaming) throws IOException {
        JavaDStream<HeatMapData> heatMapStream = streaming
                .map(RealTimeHeatMapProcessor::mapToMeasurement)
                .map(RealTimeHeatMapProcessor::roundCoordinates)
                .mapToPair(measurement -> new Tuple2<>(measurement.getRoundedCoordinate(), 1))
                .reduceByKeyAndWindow((a, b) -> a + b, Durations.seconds(120), Durations.seconds(10))
                .map(RealTimeHeatMapProcessor::mapHeatMap);

        save(heatMapStream);
    }

    /**
     * Converts each row from the dataset  to a Measurement
     *
     * @return A set containing all data from the CSV file as Measurements
     */
    private static Measurement mapToMeasurement(IoTData row) {
        Coordinate coordinate = new Coordinate(
                Double.parseDouble(row.getLatitude()),
                Double.parseDouble(row.getLongitude())
        );
        return new Measurement(coordinate, row.getTimestamp());
    }

    /**
     * Maps the measurements by rounding the coordinate. The world is defined by a grid of boxes, each box has a size of
     * 0.0005 by 0.0005. Every mapping will be rounded to the center of the box it is part of. Boundary cases will be
     * rounded up, so a coordinate on (-0.00025,0) will be rounded to (0,0), while the coordinate (0.00025,0) will be
     * rounded to (0.0005,0).
     *
     * @param measurement
     * @return A set of measurements with rounded coordinates
     */
    private static Measurement roundCoordinates(Measurement measurement) {
        double roundedLatitude = 5 * Math.round(measurement.getCoordinate().getLatitude() * 10000 / 5) / 10000;
        double roundedLongitude = 5 * Math.round(measurement.getCoordinate().getLongitude() * 10000 / 5) / 10000;

        Coordinate roundedCoordinate = new Coordinate(roundedLatitude, roundedLongitude);
        measurement.setRoundedCoordinate(roundedCoordinate);
        return measurement;
    }

    private static HeatMapData mapHeatMap(Tuple2<Coordinate, Integer> tuple) {
        Coordinate coordinate = tuple._1();
        return new HeatMapData(coordinate.getLatitude(), coordinate.getLongitude(), tuple._2(), new Date());
    }


    private static void save(JavaDStream<HeatMapData> heatMapStream) {
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

}
