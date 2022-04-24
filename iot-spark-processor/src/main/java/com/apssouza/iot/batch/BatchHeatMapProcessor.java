package com.apssouza.iot.batch;

import com.apssouza.iot.common.dto.Measurement;
import com.apssouza.iot.common.entity.HeatMapData;
import com.apssouza.iot.common.TimestampComparator;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.apssouza.iot.common.dto.Coordinate;
import com.apssouza.iot.common.dto.IoTData;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;

import java.io.IOException;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;


/**
 * Process the batch heat map calculation
 *
 * @author apssouza22
 */
public class BatchHeatMapProcessor {

    public static void processHeatMap(JavaRDD<IoTData> dataFrame) throws IOException {
        JavaRDD<Measurement> measurements = transformToMeasurements(dataFrame);
        JavaRDD<Measurement> roundedCoordinates = roundCoordinates(measurements);
        Date minTimestamp = measurements.min(new TimestampComparator()).getTimestamp();
        Date maxTimestamp = measurements.max(new TimestampComparator()).getTimestamp();
        long diffInMillies = Math.abs(minTimestamp.getTime() - maxTimestamp.getTime());
        long diffInDays = TimeUnit.DAYS.convert(diffInMillies, TimeUnit.MILLISECONDS);

        Calendar c = Calendar.getInstance();
        c.setTime(minTimestamp);
        c.set(Calendar.HOUR_OF_DAY, 0);
        c.set(Calendar.MINUTE, 0);
        Date start = c.getTime();

        for (int i = 0; i < diffInDays; i++) {
            c.setTime(start);
            c.add(Calendar.DATE, 1);
            Date end = c.getTime();
            processInterval(roundedCoordinates, start, end);
            start = end;
        }
    }

    private static void processInterval(
            JavaRDD<Measurement> roundedCoordinates,
            Date start,
            Date end
    ) {
        JavaRDD<Measurement> measurementsFilteredByTime = filterByTime(roundedCoordinates, start, end);
        JavaPairRDD<Coordinate, Integer> counts = countPerGridBox(measurementsFilteredByTime);
        JavaRDD<HeatMapData> countInArea = getCountInArea(counts, start);
        save(countInArea);
    }

    private static void save(JavaRDD<HeatMapData> mapDataJavaRDD) {
        Map<String, String> columnNameMappings = new HashMap<>();
        columnNameMappings.put("latitude", "latitude");
        columnNameMappings.put("longitude", "longitude");
        columnNameMappings.put("totalCount", "totalcount");
        columnNameMappings.put("timeStamp", "timestamp");

        CassandraJavaUtil.javaFunctions(mapDataJavaRDD).writerBuilder(
                "traffickeyspace",
                "heat_map_batch",
                CassandraJavaUtil.mapToRow(HeatMapData.class, columnNameMappings)
        ).saveToCassandra();
    }


    private static  JavaRDD<HeatMapData> getCountInArea(JavaPairRDD<Coordinate, Integer> tuples, Date day) {
        return tuples.map(tuple -> {
            Coordinate coordinate = tuple._1();
            Integer count = tuple._2();
            return new HeatMapData(coordinate.getLatitude(), coordinate.getLongitude(), count, day);
        });
    }


    /**
     * Converts each row from the iotData  to a Measurement
     *
     * @param iotData | Spark SQL context
     * @return A set containing all data from the CSV file as Measurements
     */
    private static JavaRDD<Measurement> transformToMeasurements(JavaRDD<IoTData> iotData) {
        return iotData.map(row -> {
            Coordinate coordinate = new Coordinate(
                    Double.valueOf(row.getLatitude()),
                    Double.valueOf(row.getLongitude())
            );
            return new Measurement(coordinate, row.getTimestamp());
        });
    }


    /**
     * Maps the measurements by rounding the coordinate. The world is defined by a grid of boxes, each box has a size of 0.0005 by 0.0005. Every mapping will be rounded to the center of the box it is
     * part of. Boundary cases will be rounded up, so a coordinate on (-0.00025,0) will be rounded to (0,0), while the coordinate (0.00025,0) will be rounded to (0.0005,0).
     *
     * @param measurements | The dataset of measurements
     * @return A set of measurements with rounded coordinates
     */
    private static JavaRDD<Measurement> roundCoordinates(JavaRDD<Measurement> measurements) {
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
    private static  JavaRDD<Measurement> filterByTime(JavaRDD<Measurement> measurements, Date start, Date end) {
        return measurements.filter(
                measurement -> measurement.getTimestamp().after(start)
                && measurement.getTimestamp().before(end)
        );
    }

    /**
     * Reduces the dataset by counting the number of measurements for a specific grid box (rounded coordinate)
     *
     * @param measurements | The dataset of measurements
     * @return A set of tuples linking rounded coordinates to their number of occurrences
     */
    private static  JavaPairRDD<Coordinate, Integer> countPerGridBox(JavaRDD<Measurement> measurements) {
        return measurements.mapToPair(
                measurement -> new Tuple2<>(measurement.getRoundedCoordinate(), 1)
        ).reduceByKey((a, b) -> a + b);
    }

}
