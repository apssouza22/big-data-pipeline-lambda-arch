package com.iot.app.spark.processor;

import com.datastax.spark.connector.japi.CassandraJavaUtil;
import com.iot.app.spark.entity.POITrafficData;
import com.iot.app.spark.entity.TotalTrafficData;
import com.iot.app.spark.entity.WindowTrafficData;
import com.iot.app.spark.util.GeoDistanceCalculator;
import com.iot.app.spark.util.IotDataTimestampComparator;
import com.iot.app.spark.dto.AggregateKey;
import com.iot.app.spark.dto.IoTData;
import com.iot.app.spark.dto.POIData;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import scala.Tuple3;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Class to process IoT data stream and to produce traffic data details.
 *
 * @author abaghel
 */
public class BatchTrafficDataProcessor {
    private static final Logger logger = Logger.getLogger(BatchTrafficDataProcessor.class);

    /**
     * Method to get total traffic counts of different type of vehicles for each route.
     *
     * @param filteredIotDataStream IoT data stream
     */
    public void processTotalTrafficData(JavaRDD<IoTData> filteredIotDataStream) {
        // We need to get count of vehicle group by routeId and vehicleType
        JavaPairRDD<AggregateKey, Long> countDStreamPair = filteredIotDataStream
                .mapToPair(iot -> new Tuple2<>(
                        new AggregateKey(iot.getRouteId(), iot.getVehicleType()),
                        1L
                ))
                .reduceByKey((a, b) -> a + b);
        JavaRDD<TotalTrafficData> trafficDStream = countDStreamPair.map(totalTrafficDataFunc);
        persistTotalTraffic(trafficDStream);
    }

    private void persistTotalTraffic(JavaRDD<TotalTrafficData> trafficDStream) {
        // Map Cassandra table column
        Map<String, String> columnNameMappings = new HashMap<String, String>();
        columnNameMappings.put("routeId", "routeid");
        columnNameMappings.put("vehicleType", "vehicletype");
        columnNameMappings.put("totalCount", "totalcount");
        columnNameMappings.put("timeStamp", "timestamp");
        columnNameMappings.put("recordDate", "recorddate");

        CassandraJavaUtil.javaFunctions(trafficDStream).writerBuilder(
                "traffickeyspace",
                "total_traffic_batch",
                CassandraJavaUtil.mapToRow(TotalTrafficData.class, columnNameMappings)
        ).saveToCassandra();
    }


    /**
     * Method to get window traffic counts of different type of vehicles for each route.
     * Window duration = 30 seconds and Slide interval = 10 seconds
     *
     * @param filteredIotDataStream IoT data stream
     */
    public void processWindowTrafficData(JavaRDD<IoTData> filteredIotDataStream) {
        Date minTimestamp = filteredIotDataStream.min(new IotDataTimestampComparator()).getTimestamp();
        Date maxTimestamp = filteredIotDataStream.max(new IotDataTimestampComparator()).getTimestamp();
        long diffInMillies = Math.abs(minTimestamp.getTime() - maxTimestamp.getTime());
        long diff = TimeUnit.DAYS.convert(diffInMillies, TimeUnit.MILLISECONDS);
        Calendar c = Calendar.getInstance();
        c.setTime(minTimestamp);
        c.set(Calendar.HOUR_OF_DAY, 0);
        c.set(Calendar.MINUTE, 0);
        Date start = c.getTime();
        for (int i = 0; i < diff; i++) {
            c.setTime(start);
            c.add(Calendar.DATE, 1);
            Date end = c.getTime();
            processInterval(filteredIotDataStream, start, end);
            start = end;
        }
    }

    private void processInterval(JavaRDD<IoTData> data, Date start, Date end) {
        JavaRDD<IoTData> filteredData = filterByTime(data, start, end);
        JavaRDD<WindowTrafficData> trafficDStream = getWindowTrafficData(filteredData);
        persistWindowTraffic(trafficDStream);
    }

    private void persistWindowTraffic(JavaRDD<WindowTrafficData> trafficDStream) {
        // Map Cassandra table column
        Map<String, String> columnNameMappings = new HashMap<>();
        columnNameMappings.put("routeId", "routeid");
        columnNameMappings.put("vehicleType", "vehicletype");
        columnNameMappings.put("totalCount", "totalcount");
        columnNameMappings.put("timeStamp", "timestamp");
        columnNameMappings.put("recordDate", "recorddate");

        // call CassandraStreamingJavaUtil function to save in DB
        CassandraJavaUtil.javaFunctions(trafficDStream).writerBuilder(
                "traffickeyspace",
                "window_traffic_batch",
                CassandraJavaUtil.mapToRow(WindowTrafficData.class, columnNameMappings)
        ).saveToCassandra();
    }

    private JavaRDD<WindowTrafficData> getWindowTrafficData(JavaRDD<IoTData> filteredData) {
        JavaPairRDD<AggregateKey, Long> javaPairRDD = filteredData.mapToPair(iot -> new Tuple2<>(
                new AggregateKey(iot.getRouteId(), iot.getVehicleType()),
                1L
        ));

        // Transform to dstream of TrafficData
        return javaPairRDD.map(windowTrafficDataFunc);
    }

    /**
     * Filter the data in a given time period
     *
     * @param data  | The dataset of data
     * @param start | Start of the time period
     * @param end   | End of the time period
     * @return A set of data in the given time period
     */
    private JavaRDD<IoTData> filterByTime(JavaRDD<IoTData> data, Date start, Date end) {
        return data.filter(measurement -> (
                        measurement.getTimestamp().equals(start) || measurement.getTimestamp().after(start)
                ) && measurement.getTimestamp().before(end)
        );
    }

    /**
     * Method to get the vehicles which are in radius of POI and their distance from POI.
     *
     * @param nonFilteredIotDataStream original IoT data stream
     * @param broadcastPOIValues       variable containing POI coordinates, route and vehicle types to monitor.
     */
    public void processPOIData(
            JavaRDD<IoTData> nonFilteredIotDataStream,
            Broadcast<Tuple3<POIData, String, String>> broadcastPOIValues
    ) {
        // Filter by routeId,vehicleType and in POI range
        JavaRDD<IoTData> iotDataStreamFiltered = filterVehicleInPOIRange(nonFilteredIotDataStream, broadcastPOIValues);

        // pair with poi
        JavaPairRDD<IoTData, POIData> poiDStreamPair = iotDataStreamFiltered.mapToPair(
                iot -> new Tuple2<>(iot, broadcastPOIValues.value()._1())
        );

        // Transform to dstream of POITrafficData
        JavaRDD<POITrafficData> trafficDStream = poiDStreamPair.map(poiTrafficDataFunc);
        persistPOI(trafficDStream);
    }

    private void persistPOI(JavaRDD<POITrafficData> trafficDStream) {
        // Map Cassandra table column
        Map<String, String> columnNameMappings = new HashMap<String, String>();
        columnNameMappings.put("vehicleId", "vehicleid");
        columnNameMappings.put("distance", "distance");
        columnNameMappings.put("vehicleType", "vehicletype");
        columnNameMappings.put("timeStamp", "timestamp");

        // call CassandraStreamingJavaUtil function to save in DB
        CassandraJavaUtil.javaFunctions(trafficDStream)
                .writerBuilder(
                        "traffickeyspace",
                        "poi_traffic_batch",
                        CassandraJavaUtil.mapToRow(POITrafficData.class, columnNameMappings)
                )
                .withConstantTTL(120)//keeping data for 2 minutes
                .saveToCassandra();
    }

    private JavaRDD<IoTData> filterVehicleInPOIRange(
            JavaRDD<IoTData> nonFilteredIotDataStream,
            Broadcast<Tuple3<POIData, String, String>> broadcastPOIValues
    ) {
        return nonFilteredIotDataStream
                .filter(iot -> (
                        iot.getRouteId().equals(broadcastPOIValues.value()._2())
                                && iot.getVehicleType().contains(broadcastPOIValues.value()._3())
                                && GeoDistanceCalculator.isInPOIRadius(Double.valueOf(iot.getLatitude()),
                                Double.valueOf(iot.getLongitude()), broadcastPOIValues.value()._1().getLatitude(),
                                broadcastPOIValues.value()._1().getLongitude(),
                                broadcastPOIValues.value()._1().getRadius())
                ));
    }

    //Function to create TotalTrafficData object from IoT data
    private static final Function<Tuple2<AggregateKey, Long>, TotalTrafficData> totalTrafficDataFunc = tuple -> {
        logger.debug("Total Count : " + "key " + tuple._1().getRouteId() + "-" + tuple._1().getVehicleType() + " value " + tuple._2());
        TotalTrafficData trafficData = new TotalTrafficData();
        trafficData.setRouteId(tuple._1().getRouteId());
        trafficData.setVehicleType(tuple._1().getVehicleType());
        trafficData.setTotalCount(tuple._2());
        trafficData.setTimeStamp(new Date());
        trafficData.setRecordDate(new SimpleDateFormat("yyyy-MM-dd").format(new Date()));
        return trafficData;
    };

    //Function to create WindowTrafficData object from IoT data
    private static final Function<Tuple2<AggregateKey, Long>, WindowTrafficData> windowTrafficDataFunc = (tuple -> {
        logger.debug("Window Count : " + "key " + tuple._1().getRouteId() + "-" + tuple._1().getVehicleType() + " value " + tuple._2());
        WindowTrafficData trafficData = new WindowTrafficData();
        trafficData.setRouteId(tuple._1().getRouteId());
        trafficData.setVehicleType(tuple._1().getVehicleType());
        trafficData.setTotalCount(tuple._2());
        trafficData.setTimeStamp(new Date());
        trafficData.setRecordDate(new SimpleDateFormat("yyyy-MM-dd").format(new Date()));
        return trafficData;
    });

    //Function to create POITrafficData object from IoT data
    private static final Function<Tuple2<IoTData, POIData>, POITrafficData> poiTrafficDataFunc = (tuple -> {
        POITrafficData poiTraffic = new POITrafficData();
        poiTraffic.setVehicleId(tuple._1.getVehicleId());
        poiTraffic.setVehicleType(tuple._1.getVehicleType());
        poiTraffic.setTimeStamp(new Date());
        double distance = GeoDistanceCalculator.getDistance(
                Double.valueOf(tuple._1.getLatitude()).doubleValue(),
                Double.valueOf(tuple._1.getLongitude()).doubleValue(),
                tuple._2.getLatitude(), tuple._2.getLongitude()
        );
        logger.debug("Distance for " + tuple._1.getLatitude() + "," + tuple._1.getLongitude() + "," + tuple._2.getLatitude() + "," + tuple._2.getLongitude() + " = " + distance);
        poiTraffic.setDistance(distance);
        return poiTraffic;
    });

}
