package com.apssouza.iot.processor;

import com.apssouza.iot.dto.IoTData;
import com.apssouza.iot.dto.POIData;
import com.apssouza.iot.entity.POITrafficData;
import com.apssouza.iot.util.GeoDistanceCalculator;
import com.datastax.spark.connector.japi.CassandraJavaUtil;
import static com.datastax.spark.connector.japi.CassandraStreamingJavaUtil.javaFunctions;

import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.joda.time.Duration;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import scala.Tuple2;
import scala.Tuple3;

public class PointOfInterestProcessor {

    private static final Logger logger = Logger.getLogger(RealtimeTrafficDataProcessor.class);

    /**
     * Method to get the vehicles which are in radius of POI and their distance from POI.
     *
     * @param nonFilteredIotDataStream original IoT data stream
     * @param broadcastPOIValues       variable containing POI coordinates, route and vehicle types to monitor.
     */
    public static void processPOIData(
            JavaDStream<IoTData> nonFilteredIotDataStream,
            Broadcast<Tuple3<POIData, String, String>> broadcastPOIValues
    ) {

        // Filter by routeId,vehicleType and in POI range
        JavaDStream<IoTData> iotDataStreamFiltered = nonFilteredIotDataStream
                .filter(iot -> (
                        iot.getRouteId().equals(broadcastPOIValues.value()._2())
                                && iot.getVehicleType().contains(broadcastPOIValues.value()._3())
                                &&
                                GeoDistanceCalculator.isInPOIRadius(
                                        Double.valueOf(iot.getLatitude()),
                                        Double.valueOf(iot.getLongitude()),
                                        broadcastPOIValues.value()._1().getLatitude(),
                                        broadcastPOIValues.value()._1().getLongitude(),
                                        broadcastPOIValues.value()._1().getRadius()
                                )
                ));

        // pair with poi
        JavaPairDStream<IoTData, POIData> poiDStreamPair = iotDataStreamFiltered.mapToPair(
                iot -> new Tuple2<>(iot, broadcastPOIValues.value()._1())
        );

        // Transform to dstream of POITrafficData
        JavaDStream<POITrafficData> trafficDStream = poiDStreamPair.map(poiTrafficDataFunc);

        // Map Cassandra table column
        Map<String, String> columnNameMappings = new HashMap<String, String>();
        columnNameMappings.put("vehicleId", "vehicleid");
        columnNameMappings.put("distance", "distance");
        columnNameMappings.put("vehicleType", "vehicletype");
        columnNameMappings.put("timeStamp", "timestamp");

        // call CassandraStreamingJavaUtil function to save in DB
        javaFunctions(trafficDStream)
                .writerBuilder(
                        "traffickeyspace",
                        "poi_traffic",
                        CassandraJavaUtil.mapToRow(POITrafficData.class, columnNameMappings)
                )
                .withConstantTTL(Duration.standardSeconds(2))//keeping data for 2 minutes
                .saveToCassandra();
    }


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
        logger.debug("Distance for " + tuple._1.getLatitude() + "," + tuple._1.getLongitude() + "," +
                tuple._2.getLatitude() + "," + tuple._2.getLongitude() + " = " + distance);
        poiTraffic.setDistance(distance);
        return poiTraffic;
    });

}
